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

FastLanesReader::FastLanesReader(OpenFileInfo file_p) : BaseFileReader(std::move(file_p)) {
	D_ASSERT(StringUtil::EndsWith(file.path, ".fls"));

	// The incoming path should be a full path to a file "/**/*.fls", verify if in this directory there exists
	// a footer.json file, if there is none, this implies that the footer is baked in the file.
	// TODO: Do we want the user to be able to supply a separate location for footer data?
	dir_path = file.path.substr(0, file.path.find_last_of('/') + 1);
	if (std::filesystem::exists(dir_path / fastlanes::TABLE_DESCRIPTOR_FILE_NAME)) {
		table_reader = make_uniq<fastlanes::TableReader>(dir_path, conn);
	} else {
		// TODO: Implement
		throw std::runtime_error("Baked-in footer not supported.");
	}

	fastlanes::RowgroupDescriptor rowgroup_descriptor = table_reader->get_file_metadata().m_rowgroup_descriptors[0];
	auto column_descriptors = rowgroup_descriptor.GetColumnDescriptors();

	// Configure the schema based on the data provided by the footer
	for (auto &column_descriptor : column_descriptors) {
		auto type = TranslateType(column_descriptor.data_type);
		auto name = column_descriptor.name;

		MultiFileColumnDefinition result(name, type);

		columns.push_back(result);
	}
}

FastLanesReader::~FastLanesReader() {
}

fastlanes::TableDescriptor &FastLanesReader::GetFileMetadata() const {
	return table_reader->get_file_metadata();
}


idx_t FastLanesReader::GetNRowGroups() const {
	return table_reader->get_n_rowgroups();
}

idx_t FastLanesReader::GetNVectors(idx_t row_group_idx) const {
	const fastlanes::vector<fastlanes::RowgroupDescriptor>& descriptors =
	    table_reader->get_file_metadata().m_rowgroup_descriptors;
	D_ASSERT(row_group_idx < descriptors.size());

	return descriptors[row_group_idx].GetNVectors();
}


idx_t FastLanesReader::GetNRows() const {
	const fastlanes::TableDescriptor& table_descriptor = table_reader->get_file_metadata();

	idx_t total_n_vectors = 0;
	for (auto& row_group_descriptor : table_descriptor.m_rowgroup_descriptors) {
		total_n_vectors += row_group_descriptor.GetNVectors();
	}

	return total_n_vectors * fastlanes::CFG::VEC_SZ;
}

fastlanes::up<fastlanes::RowgroupReader> FastLanesReader::CreateRowGroupReader(const idx_t rowgroup_idx) {
	return table_reader->get_rowgroup_reader(rowgroup_idx);
}

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
		bind_data.initial_file_n_rowgroups = initial_reader.GetNRowGroups();

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
	return MaxValue(bind_data.initial_file_n_rowgroups, static_cast<idx_t>(1));
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
	if (gstate.cur_rowgroup >= reader.GetNRowGroups()) {
		return false;
	}
	// Prepare the local state of the current thread by informing its processing responsibilities.
	lstate.cur_vector = 0;
	lstate.cur_rowgroup = gstate.cur_rowgroup;

	lstate.row_group_reader = reader.CreateRowGroupReader(lstate.cur_rowgroup);

	// Consume the vector in the global state.
	// TODO: Make vector batch size an option.
	gstate.cur_rowgroup++;

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

	if (local_state.cur_vector >= reader.GetNVectors(local_state.cur_rowgroup)) {
		return;
	}

	const auto &expressions = local_state.row_group_reader->get_chunk(local_state.cur_vector);
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
	g_state.cur_rowgroup = 0;
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

// TODO: The map currently is not initialised without projection.
unique_ptr<BaseStatistics> FastLanesMultiFileInfo::GetStatistics(ClientContext &context, BaseFileReader &reader_p, const string &name) {
	const auto &reader = reader_p.Cast<FastLanesReader>();
	unique_ptr<BaseStatistics> stats;

	const fastlanes::TableDescriptor& table_descriptor = reader.GetFileMetadata();
	for (idx_t row_group_idx = 0; row_group_idx < table_descriptor.m_rowgroup_descriptors.size(); row_group_idx++) {
		auto &row_group_descriptor = table_descriptor.m_rowgroup_descriptors[row_group_idx];
		auto &column_descriptors = row_group_descriptor.GetColumnDescriptors();
		auto column_idx = row_group_descriptor.LookUp(name);
		auto &column_descriptor = column_descriptors[column_idx];


		if (column_descriptor.n_null > 0) {
			stats->SetHasNull();
		} else {
			stats->SetHasNoNull();
		}
	}

	return stats;
}


//-------------------------------------------------------------------
// Register
//-------------------------------------------------------------------
void ReadFastLanes::Register(DatabaseInstance &db) {
	MultiFileFunction<FastLanesMultiFileInfo> table_function("read_fls");
	// table_function.statistics = MultiFileFunction<FastLanesMultiFileInfo>::MultiFileScanStats;

	// TODO: support
	// table_function.filter_pushdown = true;
	// table_function.filter_prune = true;
	// table_function.projection_pushdown = true;

	ExtensionUtil::RegisterFunction(db, MultiFileReader::CreateFunctionSet(static_cast<TableFunction>(table_function)));
}
} // namespace duckdb