#pragma once

#include "fls/connection.hpp"
#include "fls/expression/cross_rle_operator.hpp"
#include "fls/expression/frequency_operator.hpp"
#include "fls_gen/untranspose/untranspose.hpp"
#include <fls/encoder/materializer.hpp>
#include "fls/expression/fsst_expression.hpp"
#include <fls/expression/slpatch_operator.hpp>
#include <fls/expression/fsst12_expression.hpp>
#include <fls/primitive/copy/fls_copy.hpp>
#include <duckdb/storage/segment/uncompressed.hpp>
#include "fls/expression/dict_expression.hpp"
#include "fls/expression/rle_expression.hpp"
#include "zstd/common/debug.h"

#ifndef NDEBUG
#include <iostream>
#  define DPRINT(x) \
do { std::cerr << x << std::endl; } while (0)
#else
#  define DPRINT(x) \
do { } while (0)
#endif

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
// Materialize
//-------------------------------------------------------------------
inline fastlanes::n_t t_find_rle_segment(const fastlanes::len_t *rle_lengths, fastlanes::n_t size,
                                  fastlanes::n_t range_index) {
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
void t_decode_rle_range(const fastlanes::len_t *rle_lengths, const PT *rle_values, fastlanes::n_t size,
                        fastlanes::n_t range_index, PT *decoded_arr) {
	fastlanes::n_t start_rle_index = t_find_rle_segment(rle_lengths, size, range_index);

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

inline void t_decode_rle_range(const fastlanes::len_t *rle_lengths, const uint8_t *rle_value_bytes,
                        const fastlanes::ofs_t *rle_value_offsets, fastlanes::n_t size, fastlanes::n_t range_index,
                        Vector &target_col, string_t *target) {

	fastlanes::n_t start_rle_index = t_find_rle_segment(rle_lengths, size, range_index);

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
 */
struct material_visitor {
	explicit material_visitor(const fastlanes::n_t vec_idx, const idx_t batch_idx, Vector &target_col)
	    : vec_idx(vec_idx), batch_idx(batch_idx), target_col(target_col) {};

	/**
	 * Unpack uncompressed values (no decoding required).
	 */
	template <typename PT>
	void operator()(const fastlanes::sp<fastlanes::dec_uncompressed_opr<PT>> &opr) const {
		DPRINT("uncompressed_opr");
		fastlanes::copy<PT>(opr->Data(), FlatVector::GetData<PT>(target_col));
	}
	/**
	 * Decode FOR vector with bit-packed values.
	 */
	template <typename PT>
	void operator()(const fastlanes::sp<fastlanes::dec_unffor_opr<PT>> &opr) const {
		DPRINT("unffor_opr");
		fastlanes::copy<PT>(opr->Data(), FlatVector::GetData<PT>(target_col));
	}
	/**
	 * Decode ALP compressed doubles.
	 */
	template <typename PT>
	void operator()(const fastlanes::sp<fastlanes::dec_alp_opr<PT>> &opr) const {
		DPRINT("alp_opr");
		fastlanes::copy<PT>(opr->decoded_arr, FlatVector::GetData<PT>(target_col));
	}
	/**
	 * Decode ALP compressed doubles, encoded with ALP_rd.
	 */
	template <typename PT>
	void operator()(const fastlanes::sp<fastlanes::dec_alp_rd_opr<PT>> &opr) const {
		DPRINT("alp_rd_opr");
		fastlanes::copy<PT>(opr->glue_arr, FlatVector::GetData<PT>(target_col));
	}
	/**
	 * Decode constant value.
	 */
	template <typename PT>
	void operator()(const fastlanes::sp<fastlanes::dec_constant_opr<PT>> &opr) const {
		DPRINT("constant_opr");
		target_col.SetVectorType(VectorType::CONSTANT_VECTOR);
		ConstantVector::SetNull(target_col, false);

		const auto target = ConstantVector::GetData<PT>(target_col);
		fastlanes::copy<PT>(&opr->value, &target[0]);
	}
	/**
	 * Decode constant value with string type.
	 */
	void operator()(const fastlanes::sp<fastlanes::dec_constant_str_opr> &opr) const {
		DPRINT("constant_str_opr");
		target_col.SetVectorType(VectorType::CONSTANT_VECTOR);
		ConstantVector::SetNull(target_col, false);

		const auto constant_value_size = static_cast<fastlanes::len_t>(opr->bytes.size());
		const auto target = ConstantVector::GetData<string_t>(target_col);
		target[0] =
		    StringVector::AddString(target_col, reinterpret_cast<char *>(opr->bytes.data()), constant_value_size);
	}

	template <typename PT>
	void operator()(const fastlanes::sp<fastlanes::PhysicalExpr> &expr) const {
		DPRINT("PhysicalExpr");
	}
	void operator()(const fastlanes::sp<fastlanes::dec_struct_opr> &struct_expr) const {
		DPRINT("struct_opr");
	}
	/**
	 * Unpack uncompressed strings.
	 */
	void operator()(const fastlanes::sp<fastlanes::dec_fls_str_uncompressed_opr> &opr) const {
		DPRINT("fls_str_uncompressed_opr");
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
	 */
	void operator()(const fastlanes::sp<fastlanes::dec_fsst_opr> &opr) const {
		DPRINT("fsst_opr");
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
	 */
	void operator()(const fastlanes::sp<fastlanes::dec_fsst12_opr> &opr) const {
		DPRINT("fsst12_opr");
		const auto target_ptr = FlatVector::GetData<string_t>(target_col);
		auto buffer = make_buffer<VectorStringBuffer>();
		auto *in_byte_arr = reinterpret_cast<uint8_t *>(opr->fsst12_bytes_segment_view.data);

		for (auto i {0}; i < fastlanes::CFG::VEC_SZ; ++i) {
			// std::cout << "iterate " << i << '\n';
			generated::untranspose::fallback::scalar::untranspose_i(opr->offset_arr, opr->untrasposed_offset);
			// std::cout << "iterate " << i << '\n';
			fastlanes::len_t encoded_size {0};
			fastlanes::ofs_t offset {0};
			// std::cout << "iterate " << i << '\n';
			if (i == 0) {
				encoded_size = opr->untrasposed_offset[0];
			} else {
				offset = opr->untrasposed_offset[i - 1];
				const auto offset_next = opr->untrasposed_offset[i];
				encoded_size = offset_next - offset;
			}
			// std::cout << "iterate " << i << '\n';
			const auto decoded_size = static_cast<fastlanes::ofs_t>(
			    fsst12_decompress(&opr->fsst12_decoder, encoded_size, in_byte_arr,
			                      fastlanes::CFG::String::max_bytes_per_string, opr->tmp_string.data()));
			// std::cout << "decoded " << i << '\n';
			in_byte_arr += encoded_size;
			// std::cout << "iterate " << i << '\n';
			string_t tmp = buffer->EmptyString(decoded_size);
			auto data_ptr = tmp.GetDataWriteable();
			memcpy(data_ptr, opr->tmp_string.data(), decoded_size);

			target_ptr[i] = tmp;
		}

		target_col.SetAuxiliary(buffer);
	}
	// DICT
	template <typename KEY_PT, typename INDEX_PT>
	void operator()(const fastlanes::sp<fastlanes::dec_dict_opr<KEY_PT, INDEX_PT>> &dict_expr) const {
		DPRINT("dict_opr<KEY_PT, INDEX_PT>");
		const auto target_ptr = FlatVector::GetData<KEY_PT>(target_col);

		const auto* key_p   = dict_expr->Keys();
		const auto* index_p = dict_expr->Index();

		const fastlanes::n_t offset = batch_idx * fastlanes::CFG::VEC_SZ;
		for (fastlanes::n_t idx {0}; idx < fastlanes::CFG::VEC_SZ; ++idx) {
			target_ptr[idx + offset] = key_p[index_p[idx]];
		}
	}
	template <typename INDEX_PT>
	void operator()(const fastlanes::sp<fastlanes::dec_dict_opr<fastlanes::fls_string_t, INDEX_PT>> &opr) const {
		DPRINT("dict_opr<fastlanes::fls_string_t, INDEX_PT>");
	}
	template <typename INDEX_PT>
	void operator()(const fastlanes::sp<fastlanes::dec_fsst_dict_opr<INDEX_PT>> &opr) const {
		DPRINT("fsst_dict_opr<INDEX_PT>");
	}
	template <typename INDEX_PT>
	void operator()(const fastlanes::sp<fastlanes::dec_fsst12_dict_opr<INDEX_PT>> &opr) const {
		DPRINT("fsst12_dict_opr<INDEX_PT>");
	}
	template <typename KEY_PT, typename INDEX_PT>
	void operator()(const fastlanes::sp<fastlanes::dec_rle_map_opr<KEY_PT, INDEX_PT>> &opr) const {
		DPRINT("rle_map_opr<KEY_PT, INDEX_PT>");
		const auto target_ptr = FlatVector::GetData<KEY_PT>(target_col);
		static_assert(!std::is_same_v<KEY_PT, fastlanes::fls_string_t>, "Generic Decode logic cannot handle fls_string_t!");
		auto *rle_vals = reinterpret_cast<KEY_PT *>(opr->rle_vals_segment_view.data);

		// Skip the untranspose step.
		const fastlanes::n_t offset = batch_idx * fastlanes::CFG::VEC_SZ;
		for (auto val_idx {0}; val_idx < fastlanes::CFG::VEC_SZ; val_idx++) {
			target_ptr[offset + val_idx] = rle_vals[opr->idxs[val_idx]];
		}
	}
	template <typename INDEX_PT>
	void operator()(const fastlanes::sp<fastlanes::dec_rle_map_opr<fastlanes::fls_string_t, INDEX_PT>> &opr) const {
		DPRINT("rle_map_opr<fls_string_t, INDEX_PT>");
	}

	template <typename PT>
	void operator()(const fastlanes::sp<fastlanes::dec_null_opr<PT>> &opr) const {
		DPRINT("null_opr<PT>");
	}
	template <typename PT>
	void operator()(const fastlanes::sp<fastlanes::dec_transpose_opr<PT>> &opr) const {
		DPRINT("transpose_opr<PT>");
	}
	template <typename PT>
	void operator()(const fastlanes::sp<fastlanes::dec_slpatch_opr<PT>> &opr) const {
		DPRINT("slpatch_opr<PT>");

		const auto target = FlatVector::GetData<PT>(target_col);
		fastlanes::copy<PT>(opr->data, target);
	}
	template <typename PT>
	void operator()(const fastlanes::sp<fastlanes::dec_frequency_opr<PT>> &opr) const {
		DPRINT("frequency_opr<PT>");

		const auto target = FlatVector::GetData<PT>(target_col);
		fastlanes::copy<PT>(opr->data, target);
	}
	void operator()(const fastlanes::sp<fastlanes::dec_frequency_str_opr> &opr) const {
		DPRINT("frequency_str_opr");

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
		DPRINT("cross_rle_opr<PT>");

		const auto target = FlatVector::GetData<PT>(target_col);

		const auto *length = reinterpret_cast<const fastlanes::len_t *>(opr->lengths_segment.data);
		const auto *values = reinterpret_cast<const PT *>(opr->values_segment.data);

		const auto size = opr->lengths_segment.data_span.size() / sizeof(fastlanes::len_t);

		t_decode_rle_range(length, values, size, vec_idx, target);
	}
	void operator()(const fastlanes::sp<fastlanes::dec_cross_rle_opr<fastlanes::fls_string_t>> &opr) const {
		DPRINT("cross_rle_opr<fastlanes::fls_string_t>");

		const auto target = FlatVector::GetData<string_t>(target_col);

		const auto *length = reinterpret_cast<const fastlanes::len_t *>(opr->lengths_segment.data);
		const auto *values_bytes = reinterpret_cast<const uint8_t *>(opr->values_bytes_seg.data);
		const auto *offsets = reinterpret_cast<const fastlanes::ofs_t *>(opr->values_offset_seg.data);

		const auto size = opr->lengths_segment.data_span.size() / sizeof(fastlanes::len_t);

		t_decode_rle_range(length, values_bytes, offsets, size, vec_idx, target_col, target);
	}

	void operator()(const auto &opr) const {
		throw std::runtime_error("Operation not supported");
	}

	fastlanes::n_t vec_idx;
	idx_t batch_idx;
	Vector &target_col;
};
} // namespace duckdb
