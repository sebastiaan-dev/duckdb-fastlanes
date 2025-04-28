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
#include <fls/expression/fsst12_expression.hpp>
// #include <fls/expression/fsst_expression.hpp>
#include "duckdb/common/multi_file/multi_file_function.hpp"
#include "fls/common/magic_enum.hpp"

#include <thread>
#include <iostream>
#include <syncstream>
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
		// std::cout << "dec_uncompressed_opr" << '\n';
		fastlanes::copy<PT>(opr->Data(), FlatVector::GetData<PT>(target_col));
	}
	/**
	 * Decode FOR vector with bit-packed values.
	 */
	template <typename PT>
	void operator()(const fastlanes::sp<fastlanes::dec_unffor_opr<PT>> &opr) const {
		// std::cout << "dec_unffor_opr" << '\n';
		fastlanes::copy<PT>(opr->Data(), FlatVector::GetData<PT>(target_col));
	}
	/**
	 * Decode ALP compressed doubles.
	 */
	template <typename PT>
	void operator()(const fastlanes::sp<fastlanes::dec_alp_opr<PT>> &opr) const {
		// std::cout << "dec_alp_opr" << '\n';
		// TODO: This should not have a decoded_arr as intermediary.
		fastlanes::copy<PT>(opr->decoded_arr, FlatVector::GetData<PT>(target_col));
	}
	/**
	 * Decode ALP compressed doubles, encoded with ALP_rd.
	 */
	template <typename PT>
	void operator()(const fastlanes::sp<fastlanes::dec_alp_rd_opr<PT>> &opr) const {
		// std::cout << "dec_alp_rd_opr" << '\n';
		// TODO: This should not have a glue_arr as intermediary.
		fastlanes::copy<PT>(opr->glue_arr, FlatVector::GetData<PT>(target_col));
	}
	/**
	 * Decode constant value.
	 */
	template <typename PT>
	void operator()(const fastlanes::sp<fastlanes::dec_constant_opr<PT>> &opr) const {
		// std::cout << "dec_constant_opr" << '\n';
		target_col.SetVectorType(VectorType::CONSTANT_VECTOR);
		ConstantVector::SetNull(target_col, false);

		const auto target = ConstantVector::GetData<PT>(target_col);
		fastlanes::copy<PT>(&opr->value, &target[0]);
	}
	/**
	 * Decode constant value with string type.
	 */
	void operator()(const fastlanes::sp<fastlanes::dec_constant_str_opr> &opr) const {
		// std::cout << "dec_constant_str_opr" << '\n';
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
		// std::cout << "dec_fls_str_uncompressed_opr" << '\n';
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
		const auto target_ptr = FlatVector::GetData<string_t>(target_col);
		auto buffer = make_buffer<VectorStringBuffer>();
		auto *in_byte_arr = reinterpret_cast<uint8_t *>(opr->fsst_bytes_segment_view.data);

		for (auto i {0}; i < fastlanes::CFG::VEC_SZ; ++i) {
			generated::untranspose::fallback::scalar::untranspose_i(opr->offset_arr, opr->untrasposed_offset);

			fastlanes::len_t encoded_size {0};
			fastlanes::ofs_t offset {0};

			if (i == 0) {
				encoded_size = opr->untrasposed_offset[0];
			} else {
				offset = opr->untrasposed_offset[i - 1];
				const auto offset_next = opr->untrasposed_offset[i];
				encoded_size = offset_next - offset;
			}

			const auto decoded_size = static_cast<fastlanes::ofs_t>(
			    fsst_decompress(&opr->fsst_decoder, encoded_size, in_byte_arr,
			                    fastlanes::CFG::String::max_bytes_per_string, opr->tmp_string.data()));
			in_byte_arr += encoded_size;

			string_t tmp = buffer->EmptyString(decoded_size);
			auto data_ptr = tmp.GetDataWriteable();
			memcpy(data_ptr, opr->tmp_string.data(), decoded_size);

			target_ptr[i] = tmp;
		}

		target_col.SetAuxiliary(buffer);
	}
	/**
	 * Decode strings which are compressed using FSST (12-bit encoded).
	 *
	 * Allows up to 4096 entries.
	 *
	 * // TODO: Do we want to return a FlatVector or FSSTVector?
	 */
	void operator()(const fastlanes::sp<fastlanes::dec_fsst12_opr> &opr) const {
		// std::cout << "dec_fsst12_opr" << '\n';

		const auto start = std::chrono::high_resolution_clock::now();

		const auto target_ptr = FlatVector::GetData<string_t>(target_col);
		auto buffer = make_buffer<VectorStringBuffer>();
		auto *in_byte_arr = reinterpret_cast<uint8_t *>(opr->fsst12_bytes_segment_view.data);

		// for (auto i {0}; i < fastlanes::CFG::VEC_SZ; ++i) {
		// 	auto* test = reinterpret_cast<fastlanes::len_t *>(opr->fsst12_length_segment_view.data);
		// 	std::cout << test[i] << '\n';
		// }

		std::cout << "decoded" << '\n';

		for (auto i {0}; i < fastlanes::CFG::VEC_SZ; ++i) {
			std::cout << "iterate " << i << '\n';
			generated::untranspose::fallback::scalar::untranspose_i(opr->offset_arr, opr->untrasposed_offset);
			std::cout << "iterate " << i << '\n';
			fastlanes::len_t encoded_size {0};
			fastlanes::ofs_t offset {0};
			std::cout << "iterate " << i << '\n';
			if (i == 0) {
				encoded_size = opr->untrasposed_offset[0];
			} else {
				offset = opr->untrasposed_offset[i - 1];
				const auto offset_next = opr->untrasposed_offset[i];
				encoded_size = offset_next - offset;
			}
			std::cout << "iterate " << i << '\n';
			const auto decoded_size = static_cast<fastlanes::ofs_t>(
			    fsst12_decompress(&opr->fsst12_decoder, encoded_size, in_byte_arr,
			                      fastlanes::CFG::String::max_bytes_per_string, opr->tmp_string.data()));
			std::cout << "decoded " << i << '\n';
			in_byte_arr += encoded_size;
			std::cout << "iterate " << i << '\n';
			string_t tmp = buffer->EmptyString(decoded_size);
			auto data_ptr = tmp.GetDataWriteable();
			memcpy(data_ptr, opr->tmp_string.data(), decoded_size);

			target_ptr[i] = tmp;
		}

		target_col.SetAuxiliary(buffer);

		const auto end = std::chrono::high_resolution_clock::now();
		const std::chrono::duration<double> elapsed = (end - start);
		std::cout << elapsed << '\n';

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
		// std::cout << "dec_frequency_str_opr" << '\n';

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
		throw std::runtime_error("Operation not supported");
	}

	fastlanes::n_t vec_idx;
	Vector &target_col;
};

//-------------------------------------------------------------------
// Local
//-------------------------------------------------------------------
// static unique_ptr<LocalTableFunctionState> LocalInitFn(ExecutionContext &context, TableFunctionInitInput &input,
//                                                        GlobalTableFunctionState *global_state) {
// 	auto local_state = make_uniq<FastLanesReadLocalState>();
// 	auto &bind_data = input.bind_data->Cast<FastLanesReadBindData>();
//
// 	local_state->conn = fastlanes::Connection {};
// 	local_state->reader = std::make_unique<fastlanes::Reader>(bind_data.directory, local_state->conn);
// 	local_state->row_group = std::make_unique<fastlanes::Rowgroup>(local_state->reader->footer());
// 	local_state->materializer = std::make_unique<fastlanes::Materializer>(*local_state->row_group);
//
// 	return local_state;
// };

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
// static unique_ptr<FunctionData> BindFn(ClientContext &context, TableFunctionBindInput &input,
//                                        vector<LogicalType> &return_types, vector<string> &names) {
// 	auto read_bind_data = make_uniq<FastLanesReadBindData>();
// 	read_bind_data->directory = input.inputs[0].ToString();
//
// 	// Set up a connection to the file
// 	fastlanes::Connection conn;
// 	const fastlanes::Reader &fls_reader = conn.reset().read_fls(read_bind_data->directory);
//
// 	const auto footer = fls_reader.footer();
// 	const auto &column_descriptors = footer.GetColumnDescriptors();
// 	// Fill the data types and return types.
// 	for (idx_t i = 0; i < column_descriptors.size(); ++i) {
// 		return_types.push_back(TranslateType(column_descriptors[i].data_type));
// 		names.push_back(column_descriptors[i].name);
// 	}
//
// 	read_bind_data->n_vector = footer.GetNVectors();
//
// 	return read_bind_data;
// };

//-------------------------------------------------------------------
// Table
//-------------------------------------------------------------------
// static void LoadFlsVector(FastLanesReadGlobalState &global_state, TableFunctionInput &data, DataChunk &output,
//                           int16_t offset) {
// 	const auto &local_state = data.local_state->Cast<FastLanesReadLocalState>();
//
// 	const auto &expressions = local_state.reader->get_chunk(global_state.cur_vector);
// 	// ColumnCount is defined during the bind of the table function.
// 	for (idx_t col_idx = 0; col_idx < output.ColumnCount(); col_idx++) {
// 		auto &target_col = output.data[col_idx];
// 		const auto expr = expressions[col_idx];
//
// 		expr->PointTo(global_state.cur_vector);
// 		visit(material_visitor {global_state.cur_vector, target_col, global_state},
// 		      expr->operators[expr->operators.size() - 1]);
// 	}
// }

// static void TableFn(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
// 	auto &global_state = data.global_state->Cast<FastLanesReadGlobalState>();
//
// 	if (global_state.cur_vector < global_state.n_vector - 1) {
// 		// TODO: Set cardinality to 2048 here
// 		output.SetCardinality(1024);
// 		LoadFlsVector(global_state, data, output, 0);
// 		// ++global_state.cur_vector;
// 		// LoadFlsVector(global_state, data, output, 1024);
// 	} else if (global_state.cur_vector < global_state.n_vector) {
// 		output.SetCardinality(1024);
// 		LoadFlsVector(global_state, data, output, 0);
// 	} else {
// 		// Stop the stream of data, table function is no longer called.
// 		output.SetCardinality(0);
// 		return;
// 	}
//
// 	// Go to the next vector in the row group
// 	++global_state.cur_vector;
// }

FastLanesReader::FastLanesReader(OpenFileInfo file_p) : BaseFileReader(std::move(file_p)) {
	D_ASSERT(StringUtil::EndsWith(file.path, ".fls"));

	// The incoming path should be a full path to a file "/**/*.fls", verify if in this directory there exists
	// a footer.json file, if there is none, this implies that the footer is baked in the file.
	// TODO: Do we want the user to be able to supply a separate location for footer data?
	auto path = file.path.substr(0, file.path.find_last_of("/") + 1);
	if (std::filesystem::exists(path + fastlanes::FOOTER_FILE_NAME)) {
		reader = make_uniq<fastlanes::Reader>(path, conn);
	} else {
		// TODO: Implement
		throw std::runtime_error("Baked-in footer not supported.");
	}

	// Configure the schema based on the data provided by the footer
	for (auto &column_descriptor : reader->footer().GetColumnDescriptors()) {
		auto type = TranslateType(column_descriptor.data_type);
		auto name = column_descriptor.name;

		MultiFileColumnDefinition result(name, type);

		columns.push_back(result);
	}
}

FastLanesReader::~FastLanesReader() {
}

idx_t FastLanesReader::GetNVectors() const {
	return reader->footer().GetNVectors();
}

idx_t FastLanesReader::GetNRows() const {
	return reader->footer().GetNVectors() * fastlanes::CFG::VEC_SZ;
}

std::vector<std::shared_ptr<fastlanes::PhysicalExpr>> FastLanesReader::GetChunk(idx_t vec_idx) const {
	return reader->get_chunk(vec_idx);
}

/**
 * Define all the required functions from the MultiFileFunction template class.
 *
 */
struct FastLanesMultiFileInfo {
	/* ---- MultiFileBind ---- */
	static unique_ptr<BaseFileReaderOptions> InitializeOptions(ClientContext &context,
	                                                           optional_ptr<TableFunctionInfo> info);
	static bool ParseOption(ClientContext &context, const string &key, const Value &val, MultiFileOptions &file_options,
	                        BaseFileReaderOptions &options);
	/* ---- MultiFileBindInternal ---- */
	/*!
	 * Save user-provided options (currently none) and allocate FastLanesReadBindData (TableFunctionData) object.
	 */
	static unique_ptr<TableFunctionData> InitializeBindData(MultiFileBindData &multi_file_data,
	                                                        unique_ptr<BaseFileReaderOptions> options);
	static void BindReader(ClientContext &context, vector<LogicalType> &return_types, vector<string> &names,
	                       MultiFileBindData &bind_data);
	static shared_ptr<BaseUnionData> GetUnionData(shared_ptr<BaseFileReader> scan_p, idx_t file_idx);

	static void FinalizeBindData(const MultiFileBindData &multi_file_data);
	/* ---- MultiFileGetBindInfo ---- */
	static void GetBindInfo(const TableFunctionData &bind_data, BindInfo &info);

	/* ---- MultiInitGlobal ---- */
	static unique_ptr<GlobalTableFunctionState>
	InitializeGlobalState(ClientContext &context, MultiFileBindData &bind_data, MultiFileGlobalState &global_state);
	static optional_idx MaxThreads(const MultiFileBindData &bind_data, const MultiFileGlobalState &global_state,
	                               FileExpandResult expand_result);
	/* ---- MultiInitLocal ---- */
	static unique_ptr<LocalTableFunctionState> InitializeLocalState(ExecutionContext &, GlobalTableFunctionState &);

	/* ---- MultiFileScan ---- */
	static void Scan(ClientContext &context, BaseFileReader &reader, GlobalTableFunctionState &global_state,
	                 LocalTableFunctionState &local_state, DataChunk &chunk);

	/* --- TryOpenNextFile ---- */
	static shared_ptr<BaseFileReader> CreateReader(ClientContext &context, GlobalTableFunctionState &gstate,
	                                               BaseUnionData &union_data, const MultiFileBindData &bind_data);
	static shared_ptr<BaseFileReader> CreateReader(ClientContext &context, GlobalTableFunctionState &gstate,
	                                               const OpenFileInfo &file, idx_t file_idx,
	                                               const MultiFileBindData &bind_data);
	static shared_ptr<BaseFileReader> CreateReader(ClientContext &context, const OpenFileInfo &file,
	                                               const BaseFileReaderOptions &options,
	                                               const MultiFileOptions &file_options);
	static void FinalizeReader(ClientContext &context, BaseFileReader &reader, GlobalTableFunctionState &);
	/* ---- TryInitializeNextBatch ---- */
	static void FinishReading(ClientContext &context, GlobalTableFunctionState &global_state,
	                          LocalTableFunctionState &local_state);
	static bool TryInitializeScan(ClientContext &context, shared_ptr<BaseFileReader> &reader,
	                              GlobalTableFunctionState &gstate, LocalTableFunctionState &lstate);
	static void FinishFile(ClientContext &context, GlobalTableFunctionState &global_state, BaseFileReader &reader);

	/* ---- MultiFileCardinality ---- */
	/*!
	 * Estimate the cardinality of the to-be-read files, the estimate is based on the first file.
	 */
	static unique_ptr<NodeStatistics> GetCardinality(const MultiFileBindData &bind_data_p, idx_t file_count);
	/* ---- MultiFileProgress ---- */
	static double GetProgressInFile(ClientContext &context, const BaseFileReader &reader);
	/* ---- MultiFileGetVirtualColumns ---- */
	static void GetVirtualColumns(ClientContext &context, MultiFileBindData &bind_data, virtual_column_map_t &result);
};

unique_ptr<BaseFileReaderOptions> FastLanesMultiFileInfo::InitializeOptions(ClientContext &context,
                                                                            optional_ptr<TableFunctionInfo> info) {
	return make_uniq<BaseFileReaderOptions>();
}

bool FastLanesMultiFileInfo::ParseOption(ClientContext &context, const string &key, const Value &val,
                                         MultiFileOptions &file_options, BaseFileReaderOptions &options) {
	return false;
}

void FastLanesMultiFileInfo::GetBindInfo(const TableFunctionData &bind_data, BindInfo &info) {
}

unique_ptr<TableFunctionData> FastLanesMultiFileInfo::InitializeBindData(MultiFileBindData &multi_file_data,
                                                                         unique_ptr<BaseFileReaderOptions> options) {
	return make_uniq<FastLanesReadBindData>();
}

void FastLanesMultiFileInfo::BindReader(ClientContext &context, vector<LogicalType> &return_types,
                                        vector<string> &names, MultiFileBindData &bind_data) {
	// TODO: Allow a user to supply a schema.
	BaseFileReaderOptions options;
	bind_data.reader_bind = bind_data.multi_file_reader->BindReader<FastLanesMultiFileInfo>(
	    context, return_types, names, *bind_data.file_list, bind_data, options, bind_data.file_options);
}

shared_ptr<BaseUnionData> FastLanesMultiFileInfo::GetUnionData(shared_ptr<BaseFileReader> scan_p, idx_t file_idx) {
	std::cout << "Union Data" << '\n';

	// TODO: Implement.
	return nullptr;
}

void FastLanesMultiFileInfo::FinalizeBindData(const MultiFileBindData &multi_file_data) {
	auto &bind_data = multi_file_data.bind_data->Cast<FastLanesReadBindData>();
	// If we are not using union-by-name, there must be an initial reader from which we learn the schema.
	if (!multi_file_data.file_options.union_by_name) {
		D_ASSERT(multi_file_data.initial_reader);

		const auto &initial_reader = multi_file_data.initial_reader->Cast<FastLanesReader>();
		bind_data.initial_file_cardinality = initial_reader.GetNRows();
		bind_data.initial_file_n_vectors = initial_reader.GetNVectors();

		return;
	}

	throw std::runtime_error("Union by name is not supported");
}

optional_idx FastLanesMultiFileInfo::MaxThreads(const MultiFileBindData &bind_data_p,
                                                const MultiFileGlobalState &global_state,
                                                const FileExpandResult expand_result) {
	// If we have multiple files, we launch the maximum number of threads, this prevents situations where the first
	// file is small or empty, leading to a single thread running the query.
	if (expand_result == FileExpandResult::MULTIPLE_FILES) {
		return optional_idx();
	}
	// TODO: We are using vectors inside a file here, make a setting where we can choose the granularity.
	// rowgroup > vectors
	const auto &bind_data = bind_data_p.bind_data->Cast<FastLanesReadBindData>();
	// bind_data.initial_file_n_vectors,
	return MaxValue(static_cast<idx_t>(0), static_cast<idx_t>(1));
}

shared_ptr<BaseFileReader> FastLanesMultiFileInfo::CreateReader(ClientContext &context,
                                                                GlobalTableFunctionState &gstate,
                                                                BaseUnionData &union_data,
                                                                const MultiFileBindData &bind_data_p) {
	return make_shared_ptr<FastLanesReader>(union_data.file);
}

shared_ptr<BaseFileReader> FastLanesMultiFileInfo::CreateReader(ClientContext &context,
                                                                GlobalTableFunctionState &gstate,
                                                                const OpenFileInfo &file, idx_t file_idx,
                                                                const MultiFileBindData &bind_data) {
	return make_shared_ptr<FastLanesReader>(file);
}
shared_ptr<BaseFileReader> FastLanesMultiFileInfo::CreateReader(ClientContext &context, const OpenFileInfo &file,
                                                                const BaseFileReaderOptions &options,
                                                                const MultiFileOptions &file_options) {
	return make_shared_ptr<FastLanesReader>(file);
};

void FastLanesMultiFileInfo::FinalizeReader(ClientContext &context, BaseFileReader &reader,
                                            GlobalTableFunctionState &) {
}

unique_ptr<GlobalTableFunctionState>
FastLanesMultiFileInfo::InitializeGlobalState(ClientContext &context, MultiFileBindData &bind_data_p,
                                              MultiFileGlobalState &global_state_p) {
	return make_uniq<FastLanesReadGlobalState>();
}

unique_ptr<LocalTableFunctionState> FastLanesMultiFileInfo::InitializeLocalState(ExecutionContext &context,
                                                                                 GlobalTableFunctionState &gstate) {
	return make_uniq<FastLanesReadLocalState>();
}

bool FastLanesMultiFileInfo::TryInitializeScan(ClientContext &context, shared_ptr<BaseFileReader> &reader_p,
                                               GlobalTableFunctionState &gstate_p, LocalTableFunctionState &lstate_p) {
	auto &gstate = gstate_p.Cast<FastLanesReadGlobalState>();
	auto &lstate = lstate_p.Cast<FastLanesReadLocalState>();
	auto &reader = reader_p->Cast<FastLanesReader>();

	// Check if there are noo more vectors left to scan.
	if (gstate.cur_row_group) {
		return false;
	}
	// Prepare the local state of the current thread by informing its processing responsibilities.
	lstate.cur_vector = 0;
	lstate.to_vector = 64;
	// Consume the vector in the global state.
	// TODO: Make vector batch size an option.
	//gstate.cur_vector++;
	gstate.cur_row_group++;

	return true;
}

void FastLanesMultiFileInfo::Scan(ClientContext &context, BaseFileReader &reader_p,
                                  GlobalTableFunctionState &global_state_p, LocalTableFunctionState &local_state_p,
                                  DataChunk &chunk) {
	// TODO: Use DuckDB 2048 size, use min operator for possibly misaligned final vector.
	// Also do not save this locally.
	size_t batch_size = fastlanes::CFG::VEC_SZ;
	const auto &global_state = global_state_p.Cast<FastLanesReadGlobalState>();
	auto &local_state = local_state_p.Cast<FastLanesReadLocalState>();
	const auto &reader = reader_p.Cast<FastLanesReader>();

	if (local_state.cur_vector > local_state.to_vector - 1) {
		chunk.SetCardinality(0);
		return;
	}

	const auto &expressions = reader.GetChunk(local_state.cur_vector);
	// ColumnCount is defined during the bind of the table function.
	for (idx_t col_idx = 0; col_idx < chunk.ColumnCount(); col_idx++) {
		auto &target_col = chunk.data[col_idx];
		const auto expr = expressions[col_idx];

		expr->PointTo(local_state.cur_vector);
		std::visit(material_visitor {local_state.cur_vector, target_col}, expr->operators[expr->operators.size() - 1]);
	}

	chunk.SetCardinality(batch_size);
	local_state.cur_vector++;
}

void FastLanesMultiFileInfo::FinishReading(ClientContext &context, GlobalTableFunctionState &global_state,
                                           LocalTableFunctionState &local_state) {
}

void FastLanesMultiFileInfo::FinishFile(ClientContext &context, GlobalTableFunctionState &global_state_p,
                                        BaseFileReader &reader) {
	auto &g_state = global_state_p.Cast<FastLanesReadGlobalState>();

	// Reset progression trackers of the current file.
	g_state.cur_vector = 0;
	g_state.cur_row_group = 0;
}

unique_ptr<NodeStatistics> FastLanesMultiFileInfo::GetCardinality(const MultiFileBindData &bind_data_p,
                                                                  idx_t file_count) {
	auto &bind_data = bind_data_p.bind_data->Cast<FastLanesReadBindData>();
	// Fallback when the first file does not contain any data.
	return make_uniq<NodeStatistics>(MaxValue(bind_data.initial_file_cardinality, static_cast<idx_t>(42)) * file_count);
}

double FastLanesMultiFileInfo::GetProgressInFile(ClientContext &context, const BaseFileReader &reader) {
	return 0;
}

void FastLanesMultiFileInfo::GetVirtualColumns(ClientContext &context, MultiFileBindData &bind_data,
                                               virtual_column_map_t &result) {
}

//-------------------------------------------------------------------
// Register
//-------------------------------------------------------------------
void ReadFastLanes::Register(DatabaseInstance &db) {
	MultiFileFunction<FastLanesMultiFileInfo> table_function("read_fls");
	// TODO: support
	// table_function.filter_pushdown = true;
	// table_function.filter_prune = true;
	// table_function.projection_pushdown = true;

	ExtensionUtil::RegisterFunction(db, MultiFileReader::CreateFunctionSet(static_cast<TableFunction>(table_function)));
}
} // namespace duckdb