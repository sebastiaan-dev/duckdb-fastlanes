#include "read_fast_lanes.hpp"

#include <duckdb/function/compression/compression.hpp>
#include <duckdb/main/extension_util.hpp>
#include <duckdb/storage/segment/uncompressed.hpp>
#include <fls/connection.hpp>
#include <fls/encoder/materializer.hpp>
#include <fls/expression/alp_expression.hpp>
#include <fls/expression/frequency_operator.hpp>
#include <fls/expression/slpatch_operator.hpp>
#include <fls/expression/cross_rle_operator.hpp>
// #include <fls/expression/fsst_expression.hpp>
// #include <fls/expression/fsst12_expression.hpp>
// #include <fls/expression/fsst_expression.hpp>
#include <fls/expression/fsst_expression.hpp>
#include <fls/expression/physical_expression.hpp>
#include <fls/primitive/copy/fls_copy.hpp>
#include <fls/utl/cpu/arch.hpp>
#include <fls_gen/untranspose/untranspose.hpp>
#include <sys/param.h>

namespace duckdb {

struct str_inlined {
	uint32_t length;
	char inlined[12];
};
struct str_pointer {
	uint32_t length;
	char prefix[4];
	char *ptr;
};

union test {
	struct {
		uint32_t length;
		char prefix[4];
		char *ptr;
	} pointer;
	struct {
		uint32_t length;
		char inlined[12];
	} inlined;
};

//-------------------------------------------------------------------
// Translations
//-------------------------------------------------------------------
static LogicalType TranslateType(const fastlanes::DataType type) {
	switch (type) {
	case fastlanes::DataType::DOUBLE:
		return LogicalType::DOUBLE;
	case fastlanes::DataType::FLOAT:
		return LogicalType::FLOAT;
	case fastlanes::DataType::INT8:
		return LogicalType::TINYINT;
	case fastlanes::DataType::INT16:
		return LogicalType::SMALLINT;
	case fastlanes::DataType::INT32:
		return LogicalType::INTEGER;
	case fastlanes::DataType::INT64:
		return LogicalType::BIGINT;
	case fastlanes::DataType::UINT8:
	case fastlanes::DataType::UINT16:
	case fastlanes::DataType::UINT32:
	case fastlanes::DataType::UINT64:
		// Unsigned types map to DuckDB's unsigned bigint
		return LogicalType::UBIGINT;
	case fastlanes::DataType::STR:
	case fastlanes::DataType::FLS_STR:
		return LogicalType::VARCHAR;
	case fastlanes::DataType::BOOLEAN:
		return LogicalType::BOOLEAN;
	case fastlanes::DataType::DATE:
		return LogicalType::DATE;
	case fastlanes::DataType::BYTE_ARRAY:
		return LogicalType::BLOB;
	case fastlanes::DataType::LIST:
		// TODO
		return LogicalType::LIST(LogicalType::SQLNULL);
	case fastlanes::DataType::STRUCT:
		// TODO
		return LogicalType::STRUCT({});
	case fastlanes::DataType::MAP:
		// TODO
		return LogicalType::MAP(LogicalType::SQLNULL, LogicalType::SQLNULL);
	case fastlanes::DataType::FALLBACK:
		return LogicalType::VARCHAR;
	case fastlanes::DataType::INVALID:
	default:
		throw InternalException("TranslateType: column type is not supported");
	}
}

//-------------------------------------------------------------------
// Materialize
//-------------------------------------------------------------------
fastlanes::n_t find_rle_segment(const fastlanes::len_t *rle_lengths, fastlanes::n_t size, fastlanes::n_t range_index) {
	fastlanes::n_t target_start = range_index * 1024;
	fastlanes::n_t current_pos = 0;

	for (fastlanes::n_t i = 0; i < size; ++i) {
		if (current_pos + rle_lengths[i] > target_start) {
			return i;
		}
		current_pos += rle_lengths[i];
	}

	// If out of bounds, return last valid index or a sentinel value (-1)
	return size - 1;
}

template <typename PT>
void decode_rle_range(const fastlanes::len_t *rle_lengths, const PT *rle_values, fastlanes::n_t size,
                      fastlanes::n_t range_index, PT *decoded_arr) {
	fastlanes::n_t start_rle_index = find_rle_segment(rle_lengths, size, range_index);

	fastlanes::n_t needed = 1024;
	fastlanes::n_t current_index = start_rle_index;
	fastlanes::n_t current_pos = 0;

	for (fastlanes::n_t i = 0; i < start_rle_index; ++i)
		current_pos += rle_lengths[i];

	fastlanes::n_t offset = range_index * 1024 - current_pos;
	fastlanes::n_t decoded_pos = 0; // Track the correct position in decoded_arr

	while (needed > 0 && current_index < size) {
		fastlanes::n_t available = rle_lengths[current_index] - offset;
		fastlanes::n_t to_copy = std::min(available, needed);

		for (fastlanes::n_t i = 0; i < to_copy; ++i) {
			decoded_arr[decoded_pos++] = rle_values[current_index];
		}

		needed -= to_copy;
		offset = 0;
		++current_index;
	}
}

void decode_rle_range(const fastlanes::len_t *rle_lengths, const uint8_t *rle_value_bytes,
                      const fastlanes::ofs_t *rle_value_offsets, fastlanes::n_t size, fastlanes::n_t range_index,
                      Vector &target_col, string_t *target) {

	fastlanes::n_t start_rle_index = find_rle_segment(rle_lengths, size, range_index);

	fastlanes::n_t needed = 1024;
	fastlanes::n_t current_index = start_rle_index;
	fastlanes::n_t current_pos = 0;

	for (fastlanes::n_t i = 0; i < start_rle_index; ++i)
		current_pos += rle_lengths[i];

	fastlanes::n_t offset = range_index * 1024 - current_pos;

	size_t entries = 0;
	while (needed > 0 && current_index < size) {
		fastlanes::ofs_t prev_offset = 0;
		if (current_index == 0) {
			prev_offset = 0;
		} else {
			prev_offset = rle_value_offsets[current_index - 1];
		}
		fastlanes::ofs_t cur_offset = rle_value_offsets[current_index];
		auto length = cur_offset - prev_offset;
		fastlanes::n_t available = rle_lengths[current_index] - offset;
		fastlanes::n_t to_copy = std::min(available, needed);

		for (fastlanes::n_t i = 0; i < to_copy; ++i) {
			// target[idx] = StringVector::AddString(target_col, reinterpret_cast<const char *>(opr->Data() + offset),
			// length);
			target[entries] = StringVector::AddString(
			    target_col, reinterpret_cast<const char *>(rle_value_bytes + prev_offset), length);
			entries++;
		}

		needed -= to_copy;
		offset = 0;
		++current_index;
	}
}

/**
 * The material_visitor struct implements a visitor which selects a decoding
 * function dependent on the incoming type of the operator. The decoding function decodes directly
 * into external memory of the initiating code to save on a copy operation.
 *
 * Internally we try to use the following copy function, which provides a SIMD optimized implementation for
 * data types of different bit widths:
 *
 * fastlanes::copy(const PT* __restrict in_p, PT* __restrict out_p)
 *
 * TODO: Are we allocating enough memory to copy into the DataChunk from DuckDB?
 */
struct material_visitor {
	explicit material_visitor(const fastlanes::n_t vec_idx, Vector &target_col)
	    : vec_idx(vec_idx), target_col(target_col) {};

	/**
	 * Unpack uncompressed values (no decoding required).
	 */
	template <typename PT>
	void operator()(const fastlanes::sp<fastlanes::dec_uncompressed_opr<PT>> &opr) const {
		std::cout << "dec_uncompressed_opr" << '\n';
		fastlanes::copy<PT>(opr->Data(), FlatVector::GetData<PT>(target_col));
	}
	/**
	 * Decode FOR vector with bit-packed values.
	 */
	template <typename PT>
	void operator()(const fastlanes::sp<fastlanes::dec_unffor_opr<PT>> &opr) const {
		std::cout << "dec_unffor_opr" << '\n';
		fastlanes::copy<PT>(opr->Data(), FlatVector::GetData<PT>(target_col));
	}
	/**
	 * Decode ALP compressed doubles.
	 */
	template <typename PT>
	void operator()(const fastlanes::sp<fastlanes::dec_alp_opr<PT>> &opr) const {
		std::cout << "dec_alp_opr" << '\n';
		fastlanes::copy<PT>(opr->decoded_arr, FlatVector::GetData<PT>(target_col));
	}
	/**
	 * Decode ALP compressed doubles, encoded with ALP_rd.
	 */
	template <typename PT>
	void operator()(const fastlanes::sp<fastlanes::dec_alp_rd_opr<PT>> &opr) const {
		std::cout << "dec_alp_rd_opr" << '\n';
		fastlanes::copy<PT>(opr->glue_arr, FlatVector::GetData<PT>(target_col));
	}
	/**
	 * Decode constant value.
	 */
	template <typename PT>
	void operator()(const fastlanes::sp<fastlanes::dec_constant_opr<PT>> &opr) const {
		std::cout << "dec_constant_opr" << '\n';
		target_col.SetVectorType(VectorType::CONSTANT_VECTOR);
		ConstantVector::SetNull(target_col, false);

		const auto target = ConstantVector::GetData<PT>(target_col);
		fastlanes::copy<PT>(&opr->value, &target[0]);
	}
	/**
	 * Decode constant value with string type.
	 */
	void operator()(const fastlanes::sp<fastlanes::dec_constant_str_opr> &opr) const {
		std::cout << "dec_constant_str_opr" << '\n';
		target_col.SetVectorType(VectorType::CONSTANT_VECTOR);
		ConstantVector::SetNull(target_col, false);

		const auto constant_value_size = static_cast<fastlanes::len_t>(opr->bytes.size());
		const auto target = ConstantVector::GetData<string_t>(target_col);
		target[0] =
		    StringVector::AddString(target_col, reinterpret_cast<char *>(opr->bytes.data()), constant_value_size);
	}

	//////////// TODO //////////////
	template <typename PT>
	void operator()(const fastlanes::sp<fastlanes::PhysicalExpr> &expr) const {
		std::cout << "PhysicalExpr" << '\n';
	}
	//////////// TODO //////////////
	void operator()(const fastlanes::sp<fastlanes::dec_struct_opr> &struct_expr) const {
		std::cout << "dec_struct_opr" << '\n';
	}
	/**
	 * Unpack uncompressed strings.
	 */
	void operator()(const fastlanes::sp<fastlanes::dec_fls_str_uncompressed_opr> &opr) const {
		std::cout << "dec_fls_str_uncompressed_opr" << '\n';
		const auto target = FlatVector::GetData<string_t>(target_col);
		uint64_t offset = 0;

		for (size_t idx {0}; idx < fastlanes::CFG::VEC_SZ; ++idx) {
			const auto length = opr->Length()[idx];

			target[idx] =
			    StringVector::AddString(target_col, reinterpret_cast<const char *>(opr->Data() + offset), length);
			offset += length;
		}
	}
	/**
	 * Decode strings which are compressed using FSST.
	 *
	 * Allows up to 256 entries.
	 *
	 */
	void operator()(const fastlanes::sp<fastlanes::dec_fsst_opr> &opr) const {
		std::cout << "dec_fsst_opr" << '\n';

		const auto target_ptr = FlatVector::GetData<str_pointer>(target_col);

		unique_ptr<vector<vector<uint8_t>>> buffer(new vector<vector<uint8_t>>);
		buffer->resize(fastlanes::CFG::VEC_SZ);

		for (size_t idx {0}; idx < 1024; ++idx) {
			auto& vec_ref = (*buffer)[idx];

			std::string init_str = "1234";
			vector<u_int8_t> string_buffer(init_str.begin(), init_str.end());
			vec_ref = string_buffer;


			target_ptr[idx].length = 5;
			memcpy(target_ptr[idx].prefix, vec_ref.data(), string_t::PREFIX_LENGTH);
			target_ptr[idx].ptr = reinterpret_cast<char *>(vec_ref.data());
		};

		// unique_ptr<vector<vector<uint8_t>>> buffer(new vector<vector<uint8_t>>);
		// buffer->resize(fastlanes::CFG::VEC_SZ);

		/// START NO FSST
		// const auto target = FlatVector::GetData<string_t>(target_col);
		// /// START DECODE
		// auto *in_byte_arr = reinterpret_cast<uint8_t *>(opr->fsst_bytes_segment_view.data);
		//
		// // Loop over all string entries in the vector.
		// for (auto i {0}; i < fastlanes::CFG::VEC_SZ; ++i) {
		// 	generated::untranspose::fallback::scalar::untranspose_i(opr->offset_arr, opr->untrasposed_offset);
		//
		// 	fastlanes::len_t encoded_size {0};
		// 	fastlanes::ofs_t offset {0};
		//
		// 	if (i == 0) {
		// 		encoded_size = opr->untrasposed_offset[0];
		// 	} else {
		// 		offset = opr->untrasposed_offset[i - 1];
		// 		const auto offset_next = opr->untrasposed_offset[i];
		// 		encoded_size = offset_next - offset;
		// 	}
		//
		// 	vector<u_int8_t> string_buffer(fastlanes::CFG::String::max_bytes_per_string);
		// 	// Decode the compressed string into the opr->tmp_string.data() buffer, return the bytesize
		// 	const auto decoded_size = static_cast<fastlanes::ofs_t>(
		// 	    fsst_decompress(&opr->fsst_decoder, encoded_size, in_byte_arr,
		// 	                    fastlanes::CFG::String::max_bytes_per_string, string_buffer.data()));
		// 	buffer->push_back(string_buffer);
		//
		// 	// Ensure that the decoded size fits in the buffer.
		// 	FLS_ASSERT_L(decoded_size, opr->tmp_string.capacity())
		//
		// 	// auto current_string = buffer->get(i);
		// 	// auto test = opr->tmp_string.data();
		// 	target[i] = StringVector::AddString(target_col, reinterpret_cast<const char *>(buffer->get(i).data()),
		// decoded_size);
		// }
		/// END DECODE
		/// END NO FSST

		/// START FSST

		// target_col.SetVectorType(VectorType::FSST_VECTOR);
		// std::cout << "SET FSST TYPE" << '\n';
		//
		// auto &fsst_string_buffer = target_col.GetAuxiliary().get()->Cast<VectorFSSTStringBuffer>();
		// buffer_ptr<void> decoder = make_buffer<fsst_decoder_t>();
		// std::cout << "STARTING COPY" << '\n';
		// memcpy(decoder.get(), &opr->fsst_decoder, sizeof(fsst_decoder_t));
		// std::cout << "ENDING COPY" << '\n';
		// fsst_string_buffer.AddDecoder(decoder, fastlanes::CFG::String::max_bytes_per_string);
		// std::cout << "Added Decoder" << '\n';

		// for (int i = 0; i < 255; ++i) {
		// 	// Get the length of the symbol.
		// 	unsigned char symbol_len = opr->fsst_decoder.len[i];
		// 	// Print the symbol index and its length.
		// 	printf("Symbol %d (length %u): ", i, symbol_len);
		//
		// 	std::string result;
		// 	result.reserve(symbol_len);
		// 	// For each byte in the symbol, extract and print it in hexadecimal.
		// 	for (int j = 0; j < symbol_len; ++j) {
		// 		unsigned char byte = (opr->fsst_decoder.symbol[i] >> (8 * j)) & 0xFF;
		// 		result.push_back(byte);
		// 	}
		//
		// 	std::cout << "Symbol " << i << ": " << result << std::endl;
		// }

		// fsst_string_buffer.AddBlob();
		/// END FSST

		// uint64_t offset = 0;
		//
		// for (size_t i = 0; i < length_vec.size(); ++i) {
		// 	const auto start_string = reinterpret_cast<const char *>(&byte_arr_vec[offset]);
		//
		// 	target[i] = StringVector::AddString(target_col, start_string, length_vec[i]);
		// 	offset += length_vec[i];
		// }

		// target_col.SetVectorType(VectorType::FSST_VECTOR);
		// auto &fsst_string_buffer = target_col.GetAuxiliary().get()->Cast<VectorFSSTStringBuffer>();
		// buffer_ptr<fsst_decoder_t> decoder = make_buffer<fsst_decoder_t>();
		// //
		// memcpy(decoder->len, opr->fsst_decoder.len, sizeof(opr->fsst_decoder.len));
		// memcpy(decoder->symbol, opr->fsst_decoder.symbol, sizeof(opr->fsst_decoder.symbol));
		// decoder->version = opr->fsst_decoder.version;
		// decoder->zero_terminated = opr->fsst_decoder.zero_terminated;
		// //
		// fsst_string_buffer.AddDecoder(reinterpret_cast<buffer_ptr<void> &>(decoder),
		// StringUncompressed::DEFAULT_STRING_BLOCK_LIMIT);
	}
	/**
	 * Decode strings which are compressed using FSST (12-bit encoded).
	 *
	 * Allows up to 4096 entries.
	 *
	 * // TODO: Do we want to return a FlatVector or FSSTVector?
	 */
	void operator()(const fastlanes::sp<fastlanes::dec_fsst12_opr> &opr) const {
		std::cout << "dec_fsst12_opr" << '\n';

		// target_col.SetVectorType(VectorType::FSST_VECTOR);
		// auto &fsst_string_buffer = target_col.GetAuxiliary().get()->Cast<VectorFSSTStringBuffer>();

		// duckdb_fsst_decoder_t decoder = reinterpret_cast<duckdb_fsst_decoder_t>(opr->fsst12_decoder);

		// make_buffer<duckdb_fsst_encoder_t>();
		//
		//  auto test = make_buffer<duckdb_fsst_encoder_t>();
		//
		//  buffer_ptr<duckdb_fsst_decoder_t> decoder = make_buffer<duckdb_fsst_decoder_t>();
		//  fsst_string_buffer.AddDecoder(reinterpret_cast<buffer_ptr<void> &>(decoder),
		//  StringUncompressed::DEFAULT_STRING_BLOCK_LIMIT); fsst_string_buffer.SetCount(1024);

		// vector<uint8_t> byte_arr;
		// vector<uint32_t> length_arr;
		// opr->Decode(byte_arr, length_arr);
		//
		// const auto target = FlatVector::GetData<string_t>(target_col);
		// uint64_t offset = 0;
		//
		// for (size_t i = 0; i < length_arr.size(); ++i) {
		// 	const auto start_string = reinterpret_cast<const char *>(&byte_arr[offset]);
		//
		// target[i] = StringVector::AddString(target_col, start_string, length_arr[i]);
		// 	offset += length_arr[i];
		// }
	}
	// DICT
	//////////// TODO //////////////
	template <typename KEY_PT, typename INDEX_PT>
	void operator()(const fastlanes::sp<fastlanes::dec_dict_opr<KEY_PT, INDEX_PT>> &dict_expr) const {
		std::cout << "dec_dict_opr<KEY_PT, INDEX_PT>" << '\n';
	}
	//////////// TODO //////////////
	template <typename INDEX_PT>
	void operator()(const fastlanes::sp<fastlanes::dec_dict_opr<fastlanes::fls_string_t, INDEX_PT>> &opr) const {
		std::cout << "dec_dict_opr<fastlanes::fls_string_t, INDEX_PT>" << '\n';
	}
	//////////// TODO //////////////
	template <typename INDEX_PT>
	void operator()(const fastlanes::sp<fastlanes::dec_fsst_dict_opr<INDEX_PT>> &opr) const {
		std::cout << "dec_fsst_dict_opr<INDEX_PT>" << '\n';
	}
	//////////// TODO //////////////
	template <typename INDEX_PT>
	void operator()(const fastlanes::sp<fastlanes::dec_fsst12_dict_opr<INDEX_PT>> &opr) const {
		std::cout << "dec_fsst12_dict_opr<INDEX_PT>" << '\n';
	}
	//////////// TODO //////////////
	template <typename KEY_PT, typename INDEX_PT>
	void operator()(const fastlanes::sp<fastlanes::dec_rle_map_opr<KEY_PT, INDEX_PT>> &opr) const {
		std::cout << "dec_rle_map_opr<KEY_PT, INDEX_PT>" << '\n';
	}
	//////////// TODO //////////////
	template <typename INDEX_PT>
	void operator()(const fastlanes::sp<fastlanes::dec_rle_map_opr<fastlanes::fls_string_t, INDEX_PT>> &opr) const {
		std::cout << "dec_rle_map_opr<fastlanes::fls_string_t, INDEX_PT>" << '\n';
	}

	template <typename PT>
	void operator()(const fastlanes::sp<fastlanes::dec_null_opr<PT>> &opr) const {
		std::cout << "dec_null_opr<PT>" << '\n';
	}
	template <typename PT>
	void operator()(const fastlanes::sp<fastlanes::dec_transpose_opr<PT>> &opr) const {
		std::cout << "dec_transpose_opr<PT>" << '\n';
	}
	template <typename PT>
	void operator()(const fastlanes::sp<fastlanes::dec_slpatch_opr<PT>> &opr) const {
		std::cout << "dec_slpatch_opr" << '\n';

		const auto target = FlatVector::GetData<PT>(target_col);
		fastlanes::copy<PT>(opr->data, target);
	}
	template <typename PT>
	void operator()(const fastlanes::sp<fastlanes::dec_frequency_opr<PT>> &opr) const {
		std::cout << "dec_frequency_opr" << '\n';

		const auto target = FlatVector::GetData<PT>(target_col);
		fastlanes::copy<PT>(opr->data, target);
	}
	void operator()(const fastlanes::sp<fastlanes::dec_frequency_str_opr> &opr) const {
		std::cout << "dec_frequency_str_opr" << '\n';

		const auto target = FlatVector::GetData<string_t>(target_col);
		size_t entries = 0;

		auto *exception_positions = reinterpret_cast<fastlanes::vec_idx_t *>(opr->exception_positions_seg.data);
		auto *exception_values_bytes = reinterpret_cast<uint8_t *>(opr->exception_values_bytes_seg.data);
		auto *exception_values_offset = reinterpret_cast<fastlanes::ofs_t *>(opr->exception_values_offset_seg.data);
		auto *n_exceptions_p = reinterpret_cast<fastlanes::vec_idx_t *>(opr->n_exceptions_seg.data);
		auto n_exceptions = n_exceptions_p[0];

		fastlanes::vec_idx_t exception_idx {0};
		fastlanes::vec_idx_t exception_position {0};
		for (fastlanes::n_t idx {0}; idx < fastlanes::CFG::VEC_SZ; ++idx) {
			exception_position = exception_positions[exception_idx];

			if (exception_position == idx && exception_idx < n_exceptions) {
				fastlanes::ofs_t cur_ofs;
				fastlanes::ofs_t next_ofs = exception_values_offset[exception_idx];
				if (exception_idx == 0) {
					cur_ofs = 0;
				} else {
					cur_ofs = exception_values_offset[exception_idx - 1];
				}

				target[entries] = StringVector::AddString(
				    target_col, reinterpret_cast<const char *>(exception_values_bytes + cur_ofs), next_ofs - cur_ofs);
				entries++;

				exception_idx++;
			} else {
				target[entries] = StringVector::AddString(
				    target_col, reinterpret_cast<const char *>(opr->frequent_val.p), opr->frequent_val.length);
				entries++;
			}
		}
	}
	template <typename PT>
	void operator()(const fastlanes::sp<fastlanes::dec_cross_rle_opr<PT>> &opr) const {
		std::cout << "dec_cross_rle_opr<PT>" << '\n';

		const auto target = FlatVector::GetData<PT>(target_col);

		const auto *length = reinterpret_cast<const fastlanes::len_t *>(opr->lengths_segment.data);
		const auto *values = reinterpret_cast<const PT *>(opr->values_segment.data);

		const auto size = opr->lengths_segment.data_span.size() / sizeof(fastlanes::len_t);

		decode_rle_range(length, values, size, vec_idx, target);
	}
	void operator()(const fastlanes::sp<fastlanes::dec_cross_rle_opr<fastlanes::fls_string_t>> &opr) const {
		std::cout << "dec_cross_rle_opr<fastlanes::fls_string_t>" << '\n';

		const auto target = FlatVector::GetData<string_t>(target_col);

		const auto *length = reinterpret_cast<const fastlanes::len_t *>(opr->lengths_segment.data);
		const auto *values_bytes = reinterpret_cast<const uint8_t *>(opr->values_bytes_seg.data);
		const auto *offsets = reinterpret_cast<const fastlanes::ofs_t *>(opr->values_offset_seg.data);

		const auto size = opr->lengths_segment.data_span.size() / sizeof(fastlanes::len_t);

		decode_rle_range(length, values_bytes, offsets, size, vec_idx, target_col, target);
	}

	void operator()(const auto &opr) const {
		std::cout << "not supported" << '\n';
		throw std::runtime_error("Operation not supported");
	}

	fastlanes::n_t vec_idx;
	Vector &target_col;
};

//-------------------------------------------------------------------
// Local
//-------------------------------------------------------------------
static unique_ptr<LocalTableFunctionState> LocalInitFn(ExecutionContext &context, TableFunctionInitInput &input,
                                                       GlobalTableFunctionState *global_state) {
	auto local_state = make_uniq<FastLanesReadLocalState>();
	auto &bind_data = input.bind_data->Cast<FastLanesReadBindData>();

	local_state->conn = fastlanes::Connection {};
	local_state->reader = std::make_unique<fastlanes::Reader>(bind_data.directory, local_state->conn);
	local_state->row_group = std::make_unique<fastlanes::Rowgroup>(local_state->reader->footer());
	local_state->materializer = std::make_unique<fastlanes::Materializer>(*local_state->row_group);

	return local_state;
};

//-------------------------------------------------------------------
// Global
//-------------------------------------------------------------------
static unique_ptr<GlobalTableFunctionState> GlobalInitFn(ClientContext &context, TableFunctionInitInput &input) {
	auto global_state = make_uniq<FastLanesReadGlobalState>();
	auto &bind_data = input.bind_data->Cast<FastLanesReadBindData>();

	// Verify that a Fastlanes vector fits in the output Vector of DuckDB
	// TODO: DuckDB 2048
	D_ASSERT(fastlanes::CFG::VEC_SZ <= 1024);
	// Vector size should be a power of 2 to allow shift based multiplication.
	D_ASSERT(powerof2(fastlanes::CFG::VEC_SZ));

	global_state->vec_sz = fastlanes::CFG::VEC_SZ;
	global_state->vec_sz_exp = std::log2(global_state->vec_sz);
	global_state->n_vector = bind_data.n_vector;
	global_state->cur_vector = 0;

	return global_state;
};

//-------------------------------------------------------------------
// Bind
//-------------------------------------------------------------------
static unique_ptr<FunctionData> BindFn(ClientContext &context, TableFunctionBindInput &input,
                                       vector<LogicalType> &return_types, vector<string> &names) {
	auto read_bind_data = make_uniq<FastLanesReadBindData>();
	read_bind_data->directory = input.inputs[0].ToString();

	// Set up a connection to the file
	fastlanes::Connection conn;
	const fastlanes::Reader &fls_reader = conn.reset().read_fls(read_bind_data->directory);

	const auto footer = fls_reader.footer();
	const auto &column_descriptors = footer.GetColumnDescriptors();
	// Fill the data types and return types.
	for (idx_t i = 0; i < column_descriptors.size(); ++i) {
		return_types.push_back(TranslateType(column_descriptors[i].data_type));
		names.push_back(column_descriptors[i].name);
	}

	read_bind_data->n_vector = footer.GetNVectors();

	return read_bind_data;
};

//-------------------------------------------------------------------
// Table
//-------------------------------------------------------------------
static void LoadFlsVector(const FastLanesReadGlobalState &global_state, TableFunctionInput &data, DataChunk &output,
                          int16_t offset) {
	const auto &local_state = data.local_state->Cast<FastLanesReadLocalState>();

	const auto &expressions = local_state.reader->get_chunk(global_state.cur_vector);
	// ColumnCount is defined during the bind of the table function.
	for (idx_t col_idx = 0; col_idx < output.ColumnCount(); col_idx++) {
		auto &target_col = output.data[col_idx];
		const auto expr = expressions[col_idx];

		expr->PointTo(global_state.cur_vector);
		visit(material_visitor {global_state.cur_vector, target_col}, expr->operators[expr->operators.size() - 1]);
	}
}

static void TableFn(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &global_state = data.global_state->Cast<FastLanesReadGlobalState>();

	if (global_state.cur_vector < global_state.n_vector - 1) {
		// TODO: Set cardinality to 2048 here
		output.SetCardinality(1024);
		LoadFlsVector(global_state, data, output, 0);
		// ++global_state.cur_vector;
		// LoadFlsVector(global_state, data, output, 1024);
	} else if (global_state.cur_vector < global_state.n_vector) {
		output.SetCardinality(1024);
		LoadFlsVector(global_state, data, output, 0);
	} else {
		// Stop the stream of data, table function is no longer called.
		output.SetCardinality(0);
		return;
	}

	// Go to the next vector in the row group
	++global_state.cur_vector;
}

//-------------------------------------------------------------------
// Register
//-------------------------------------------------------------------
void ReadFastLanes::Register(DatabaseInstance &db) {
	auto table_function = TableFunction("read_fls", {LogicalType::VARCHAR}, TableFn, BindFn, GlobalInitFn, LocalInitFn);
	// TODO: support
	// table_function.projection_pushdown = true;
	ExtensionUtil::RegisterFunction(db, table_function);
}
} // namespace duckdb