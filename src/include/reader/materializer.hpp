#pragma once

#include "fls_decode.hpp"
#include "fls/expression/cross_rle_operator.hpp"
#include "fls/expression/frequency_operator.hpp"
#include "fls_gen/untranspose/untranspose.hpp"
#include <fls/encoder/materializer.hpp>
#include "fls/expression/fsst_expression.hpp"
#include <fls/expression/slpatch_operator.hpp>
#include <fls/expression/fsst12_expression.hpp>
#include "fls/expression/fsst_dict_operator.hpp"
#include "fls/expression/fsst12_dict_operator.hpp"
#include <fls/primitive/copy/fls_copy.hpp>
#include "fls/expression/dict_expression.hpp"
#include "fls/expression/rle_expression.hpp"
#include "zstd/common/debug.h"
#include "fls/expression/transpose_operator.hpp"
#include <iostream>

#ifndef NDEBUG
#include <iostream>
#define DPRINT(x)                                                                                                      \
	do {                                                                                                               \
		std::cerr << x << '\n';                                                                                        \
	} while (0)
#else
#define DPRINT(x)                                                                                                      \
	do {                                                                                                               \
	} while (0)
#endif

namespace duckdb {

template <typename T>
T load_unaligned(const void *ptr) {
	T value;
	std::memcpy(&value, ptr, sizeof(T));
	return value;
}

//-------------------------------------------------------------------
// Materialize
//-------------------------------------------------------------------
inline fastlanes::n_t t_find_rle_segment(const std::byte *rle_lengths, fastlanes::n_t size,
                                         fastlanes::n_t range_index) {
	fastlanes::n_t target_start = range_index * 1024;
	fastlanes::n_t current_pos = 0;

	for (fastlanes::n_t i = 0; i < size; ++i) {
		const auto length = load_unaligned<fastlanes::len_t>(rle_lengths + i * sizeof(fastlanes::len_t));

		if (current_pos + length > target_start) {
			return i;
		}
		current_pos += length;
	}

	// If out of bounds, return last valid index or a sentinel value (-1)
	return size - 1;
}

template <typename PT>
void t_decode_rle_range(const std::byte *rle_lengths, const std::byte *rle_values, fastlanes::n_t size,
                        fastlanes::n_t range_index, PT *decoded_arr) {
	fastlanes::n_t start_rle_index = t_find_rle_segment(rle_lengths, size, range_index);

	fastlanes::n_t needed = 1024;
	fastlanes::n_t current_index = start_rle_index;
	fastlanes::n_t current_pos = 0;

	for (fastlanes::n_t i = 0; i < start_rle_index; ++i)
		current_pos += load_unaligned<fastlanes::len_t>(rle_lengths + i * sizeof(fastlanes::len_t));

	fastlanes::n_t offset = range_index * 1024 - current_pos;
	fastlanes::n_t decoded_pos = 0; // Track the correct position in decoded_arr

	while (needed > 0 && current_index < size) {

		fastlanes::n_t available =
		    load_unaligned<fastlanes::len_t>(rle_lengths + current_index * sizeof(fastlanes::len_t)) - offset;
		fastlanes::n_t to_copy = std::min(available, needed);

		for (fastlanes::n_t i = 0; i < to_copy; ++i) {
			decoded_arr[decoded_pos++] =
			    load_unaligned<PT>(rle_values + current_index * sizeof(PT)); // rle_values[current_index];
		}

		needed -= to_copy;
		offset = 0;
		++current_index;
	}
}

inline void t_decode_rle_range(const std::byte *rle_lengths, const uint8_t *rle_value_bytes,
                               const std::byte *rle_value_offsets, fastlanes::n_t size, fastlanes::n_t range_index,
                               Vector &target_col, string_t *target) {

	fastlanes::n_t start_rle_index = t_find_rle_segment(rle_lengths, size, range_index);

	fastlanes::n_t needed = 1024;
	fastlanes::n_t current_index = start_rle_index;
	fastlanes::n_t current_pos = 0;

	for (fastlanes::n_t i = 0; i < start_rle_index; ++i)
		current_pos += load_unaligned<fastlanes::len_t>(rle_lengths + i * sizeof(fastlanes::len_t));

	fastlanes::n_t offset = range_index * 1024 - current_pos;

	size_t entries = 0;
	while (needed > 0 && current_index < size) {
		fastlanes::ofs_t prev_offset = 0;
		if (current_index == 0) {
			prev_offset = 0;
		} else {
			prev_offset = load_unaligned<fastlanes::ofs_t>(
			    rle_value_offsets +
			    (current_index - 1) * sizeof(fastlanes::ofs_t)); // rle_value_offsets[current_index - 1];
		}
		fastlanes::ofs_t cur_offset = load_unaligned<fastlanes::ofs_t>(
		    rle_value_offsets + current_index * sizeof(fastlanes::ofs_t)); // rle_value_offsets[current_index];
		auto length = cur_offset - prev_offset;
		fastlanes::n_t available =
		    load_unaligned<fastlanes::len_t>(rle_lengths + current_index * sizeof(fastlanes::len_t)) - offset;
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

template <typename PT>
PT *GetWritePtr(Vector &v) {
	return FlatVector::GetData<PT>(v);
}

struct ColumnCtx {
public:
	explicit ColumnCtx() {};

	unique_ptr<Vector> dict_vec;
	unique_ptr<Vector> fsst_aux_vec;
	buffer_ptr<void> fsst_decoder;
	std::vector<fastlanes::sp<fastlanes::dec_fsst_opr>> fsst_oprs;
};

struct KeepAlive : VectorBuffer {
	fastlanes::sp<fastlanes::dec_fsst_opr> hold;
	explicit KeepAlive(fastlanes::sp<fastlanes::dec_fsst_opr> p)
	    : VectorBuffer(VectorBufferType::MANAGED_BUFFER), hold(std::move(p)) {
	}
};

// TODO: maybe make it a template with a parameter FIRST_RUN which initialises a special vector type as kind::<type>
// using constexpr.
// TODO: maybe also make a template for the batch?
struct MaterializeVisitor {
	Vector &target_col;
	size_t offset;
	ColumnCtx &ctx;

	/**
	 * Unpack uncompressed values (no decoding required).
	 */
	template <typename PT>
	void operator()(const fastlanes::sp<fastlanes::dec_uncompressed_opr<PT>> &opr) const {
		fastlanes::copy<PT>(opr->Data(), GetDataPtr<PT>(target_col));
	}
	/**
	 * Decode FOR vector with bit-packed values.
	 */
	template <typename PT>
	void operator()(const fastlanes::sp<fastlanes::dec_unffor_opr<PT>> &opr) const {
		fastlanes::copy<PT>(opr->Data(), GetDataPtr<PT>(target_col));
	}
	/**
	 * Decode ALP compressed doubles.
	 */
	template <typename PT>
	void operator()(const fastlanes::sp<fastlanes::dec_alp_opr<PT>> &opr) const {
		fastlanes::copy<PT>(opr->decoded_arr, GetDataPtr<PT>(target_col));
	}
	/**
	 * Decode ALP compressed doubles, encoded with ALP_rd.
	 */
	template <typename PT>
	void operator()(const fastlanes::sp<fastlanes::dec_alp_rd_opr<PT>> &opr) const {
		fastlanes::copy<PT>(opr->glue_arr, GetDataPtr<PT>(target_col));
	}
	/**
	 * Decode constant value.
	 */
	template <typename PT>
	void operator()(const fastlanes::sp<fastlanes::dec_constant_opr<PT>> &opr) const {
		target_col.SetVectorType(VectorType::CONSTANT_VECTOR);
		ConstantVector::SetNull(target_col, false);

		fastlanes::copy<PT>(&opr->value, &GetDataPtr<PT>(target_col)[0]);
	}
	/**
	 * Decode constant value with string type.
	 */
	void operator()(const fastlanes::sp<fastlanes::dec_constant_str_opr> &opr) const {
		target_col.SetVectorType(VectorType::CONSTANT_VECTOR);
		ConstantVector::SetNull(target_col, false);

		const auto constant_value_size = static_cast<fastlanes::len_t>(opr->bytes.size());
		const auto target = GetDataPtr<string_t>(target_col);
		target[0] =
		    StringVector::AddString(target_col, reinterpret_cast<char *>(opr->bytes.data()), constant_value_size);
	}

	template <typename PT>
	void operator()(const fastlanes::sp<fastlanes::PhysicalExpr> &expr) const {
		throw std::runtime_error("Operation not supported");
	}
	void operator()(const fastlanes::sp<fastlanes::dec_struct_opr> &struct_expr) const {
		throw std::runtime_error("Operation not supported");
	}
	/**
	 * Unpack uncompressed strings.
	 */
	void operator()(const fastlanes::sp<fastlanes::dec_fls_str_uncompressed_opr> &opr) const {
		const auto target = GetDataPtr<string_t>(target_col);
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
		target_col.SetVectorType(VectorType::FSST_VECTOR);
		// auto buf = make_buffer<KeepAlive>(opr);

		if (ctx.fsst_decoder.get() == nullptr) {
			const fsst_decoder_t &dec_ref = opr->fsst_decoder;
			ctx.fsst_decoder =
			    buffer_ptr<void>(new fsst_decoder_t(dec_ref), [](void *p) { delete static_cast<fsst_decoder_t *>(p); });
			ctx.fsst_aux_vec = make_uniq<Vector>(target_col.GetType(), 2048);
			ctx.fsst_aux_vec->SetVectorType(VectorType::FSST_VECTOR);
			FSSTVector::RegisterDecoder(*ctx.fsst_aux_vec, ctx.fsst_decoder,
			                            fastlanes::CFG::String::max_bytes_per_string);
		}

		target_col.Reinterpret(*ctx.fsst_aux_vec);

		if (FSSTVector::GetCount(target_col) == 0) {
			FSSTVector::SetCount(target_col, 1024);
		} else {
			FSSTVector::SetCount(target_col, 2048);
		}

		auto out = FSSTVector::GetCompressedData<string_t>(target_col) + offset;
		const auto *bytes = reinterpret_cast<uint8_t *>(opr->fsst_bytes_segment_view.data);

		generated::untranspose::fallback::scalar::untranspose_i(opr->offset_arr, opr->untrasposed_offset);

		fastlanes::ofs_t prev_end = 0;
		for (idx_t i = 0; i < 1024; ++i) {
			const fastlanes::ofs_t end = opr->untrasposed_offset[i];
			const fastlanes::ofs_t len = end - prev_end;

			// out[i] = string_t(reinterpret_cast<const char *>(bytes + prev_end), len);

			out[i] = FSSTVector::AddCompressedString(target_col, reinterpret_cast<const char *>(bytes + prev_end), len);

			prev_end = end;
		}

		// target_col.GetAuxiliary()->Cast<VectorFSSTStringBuffer>().AddHeapReference(buf);

		// const auto target_ptr = GetDataPtr<string_t>(target_col);
		// auto *in_byte_arr = reinterpret_cast<uint8_t *>(opr->fsst_bytes_segment_view.data);
		//
		// generated::untranspose::fallback::scalar::untranspose_i(opr->offset_arr, opr->untrasposed_offset);
		//
		// fastlanes::ofs_t prev = 0;
		// for (auto i {0}; i < fastlanes::CFG::VEC_SZ; ++i) {
		// 	const fastlanes::ofs_t end = opr->untrasposed_offset[i];
		// 	const fastlanes::len_t len = end - prev;
		// 	prev = end;
		//
		// 	const auto decoded_size = static_cast<fastlanes::ofs_t>(
		// 	    test_fsst_decode(&opr->fsst_decoder, len, in_byte_arr, fastlanes::CFG::String::max_bytes_per_string,
		// 	                     opr->tmp_string.data()));
		//
		// 	in_byte_arr += len;
		// 	target_ptr[i] = StringVector::AddString(target_col, reinterpret_cast<const char *>(opr->tmp_string.data()),
		// 	                                        decoded_size);
		// }
	}
	/**
	 * Decode strings which are compressed using FSST (12-bit encoded).
	 *
	 * Allows up to 4096 entries.
	 */
	void operator()(const fastlanes::sp<fastlanes::dec_fsst12_opr> &opr) const {
		const auto target_ptr = GetDataPtr<string_t>(target_col);
		auto *in_byte_arr = reinterpret_cast<uint8_t *>(opr->fsst12_bytes_segment_view.data);

		generated::untranspose::fallback::scalar::untranspose_i(opr->offset_arr, opr->untrasposed_offset);

		fastlanes::ofs_t prev = 0;
		for (auto i {0}; i < fastlanes::CFG::VEC_SZ; ++i) {
			const fastlanes::ofs_t end = opr->untrasposed_offset[i];
			const fastlanes::len_t len = end - prev;
			prev = end;

			const auto decoded_size = static_cast<fastlanes::ofs_t>(
			    fsst12_decompress(&opr->fsst12_decoder, len, in_byte_arr, fastlanes::CFG::String::max_bytes_per_string,
			                      opr->tmp_string.data()));

			in_byte_arr += len;

			// target_ptr[i] = string_t(reinterpret_cast<const char *>(opr->tmp_string.data()), decoded_size);
			target_ptr[i] = StringVector::AddString(target_col, reinterpret_cast<const char *>(opr->tmp_string.data()),
			                                        decoded_size);
		}
	}
	template <typename KEY_PT, typename INDEX_PT>
	void operator()(const fastlanes::sp<fastlanes::dec_dict_opr<KEY_PT, INDEX_PT>> &dict_expr) const {
		// if (ctx.dict_vec.get() == nullptr) {
		// 	auto dict_size = dict_expr->key_segment_view.data_span.size();
		// 	const auto *key_p = dict_expr->key_segment_view.data_span.data();
		//
		// 	ctx.dict_vec = make_uniq<Vector>(target_col.GetType(), dict_size);
		//
		// 	auto child_data_ptr = FlatVector::GetData<KEY_PT>(*ctx.dict_vec.get());
		// 	for (fastlanes::n_t i {0}; i < dict_size; ++i) {
		// 		child_data_ptr[i] = load_unaligned<KEY_PT>(key_p + i * sizeof(KEY_PT));
		// 	}
		// }
		//
		// if (target_col.GetVectorType() == VectorType::DICTIONARY_VECTOR) {
		// 	auto sel_vec = DictionaryVector::SelVector(target_col);
		// 	const auto *index_p = dict_expr->Index();
		//
		// 	for (idx_t i = 1024; i < 2048; i++) {
		// 		sel_vec.set_index(i, index_p[i]);
		// 	}
		//
		// 	return;
		// }
		//
		// const auto *index_p = dict_expr->Index();
		//
		// SelectionVector sel_vec(2048);
		// for (idx_t i = 0; i < 1024; i++) {
		// 	sel_vec.set_index(i, index_p[i]);
		// }
		//
		// target_col.SetVectorType(VectorType::DICTIONARY_VECTOR);
		// target_col.Reference(*ctx.dict_vec.get());
		// target_col.Slice(sel_vec, 2048);

		const auto target_ptr = GetDataPtr<KEY_PT>(target_col);
		// const auto *key_p = dict_expr->key_segment_view.data_span.data();
		const auto *index_p = dict_expr->Index();
		const auto *key_p = dict_expr->Keys();

		for (fastlanes::n_t i {0}; i < fastlanes::CFG::VEC_SZ; ++i) {
			target_ptr[i] = key_p[index_p[i]];
			// target_ptr[i] = load_unaligned<KEY_PT>(key_p + index_p[i] * sizeof(KEY_PT));
		}
	}
	template <typename INDEX_PT>
	void operator()(const fastlanes::sp<fastlanes::dec_dict_opr<fastlanes::fls_string_t, INDEX_PT>> &opr) const {
		const auto target_ptr = GetDataPtr<string_t>(target_col);

		const auto *offset_bytes = opr->dict_offsets_segment.data_span.data();
		const auto *idx_arr = opr->Index();
		const auto *bytes = opr->Bytes();

		for (fastlanes::n_t i = 0; i < fastlanes::CFG::VEC_SZ; ++i) {
			const INDEX_PT k = idx_arr[i];

			const fastlanes::ofs_t cur = load_unaligned<fastlanes::ofs_t>(offset_bytes + k * sizeof(fastlanes::ofs_t));
			const fastlanes::ofs_t prev =
			    (k == 0) ? 0 : load_unaligned<fastlanes::ofs_t>(offset_bytes + (k - 1) * sizeof(fastlanes::ofs_t));

			const auto len = static_cast<idx_t>(cur - prev);
			const auto src = reinterpret_cast<const char *>(bytes + prev);

			// target_ptr[i] = string_t(src, len);
			// target_ptr[i] = StringVector::AddString(target_col, src, len);
		}
	}
	template <typename INDEX_PT>
	void operator()(const fastlanes::sp<fastlanes::dec_fsst_dict_opr<INDEX_PT>> &opr) const {
		// Create Vector
		if (ctx.dict_vec.get() == nullptr) {
			auto dict_size = opr->fsst_offset_segment_view.data_span.size() / sizeof(fastlanes::ofs_t);

			ctx.dict_vec = make_uniq<Vector>(target_col.GetType(), dict_size);

			const auto child_ptr = GetDataPtr<string_t>(*ctx.dict_vec);
			const auto offset_bytes = reinterpret_cast<const uint8_t *>(opr->fsst_offset_segment_view.data_span.data());
			auto *in_byte_arr = reinterpret_cast<uint8_t *>(opr->fsst_bytes_segment_view.data);

			fastlanes::ofs_t prev_end = 0;
			for (fastlanes::n_t i {0}; i < dict_size; ++i) {
				const fastlanes::ofs_t end =
				    load_unaligned<fastlanes::ofs_t>(offset_bytes + i * sizeof(fastlanes::ofs_t));
				const fastlanes::ofs_t enc_len = end - prev_end;

				const auto decoded_size = static_cast<fastlanes::ofs_t>(
				    test_fsst_decode(&opr->fsst_decoder, enc_len, in_byte_arr + prev_end,
				                     fastlanes::CFG::String::max_bytes_per_string, opr->tmp_string.data()));

				child_ptr[i] = StringVector::AddString(
				    *ctx.dict_vec, reinterpret_cast<const char *>(opr->tmp_string.data()), decoded_size);

				prev_end = end;
			}
		}

		// Load second part
		if (target_col.GetVectorType() == VectorType::DICTIONARY_VECTOR) {
			auto sel_vec = DictionaryVector::SelVector(target_col);

			for (idx_t i = 0; i < 1024; i++) {
				sel_vec.set_index(i + 1024, static_cast<sel_t>(opr->Index()[i]));
			}

			return;
		}

		// Load first part
		SelectionVector sel_vec(2048);
		for (idx_t i = 0; i < 1024; i++) {
			sel_vec.set_index(i, static_cast<sel_t>(opr->Index()[i]));
		}

		target_col.SetVectorType(VectorType::DICTIONARY_VECTOR);
		target_col.Reference(*ctx.dict_vec);
		target_col.Slice(sel_vec, 2048);
	}
	template <typename INDEX_PT>
	void operator()(const fastlanes::sp<fastlanes::dec_fsst12_dict_opr<INDEX_PT>> &opr) const {
		const auto target_ptr = GetDataPtr<string_t>(target_col);
		auto *in_byte_arr = reinterpret_cast<uint8_t *>(opr->fsst12_bytes_segment_view.data);

		FLS_ASSERT_NOT_NULL_POINTER(in_byte_arr)

		for (fastlanes::n_t idx {0}; idx < fastlanes::CFG::VEC_SZ; ++idx) {
			const auto index = opr->Index()[idx];

			fastlanes::ofs_t offset = 0;
			fastlanes::len_t length = 0;

			if (index == 0) {
				offset = 0;
				length = opr->Offsets()[index];
			} else {
				offset = opr->Offsets()[index - 1];
				const auto offset_next = opr->Offsets()[index];
				length = offset_next - offset;
			}
			FLS_ASSERT_LE(length, fastlanes::CFG::String::max_bytes_per_string)

			const auto decoded_size = static_cast<fastlanes::ofs_t>(
			    fsst12_decompress(&opr->fsst12_decoder, length, in_byte_arr + offset,
			                      fastlanes::CFG::String::max_bytes_per_string, opr->tmp_string.data()));

			FLS_ASSERT_L(decoded_size, opr->tmp_string.capacity())

			target_ptr[idx] = StringVector::AddString(
			    target_col, reinterpret_cast<const char *>(opr->tmp_string.data()), decoded_size);
		}
	}
	template <typename KEY_PT, typename INDEX_PT>
	void operator()(const fastlanes::sp<fastlanes::dec_rle_map_opr<KEY_PT, INDEX_PT>> &opr) const {
		static_assert(!std::is_same_v<KEY_PT, fastlanes::fls_string_t>,
		              "Generic Decode logic cannot handle fls_string_t!");

		const auto target_ptr = GetDataPtr<KEY_PT>(target_col);
		const auto *rle_vals_bytes = opr->rle_vals_segment_view.data;

		for (auto i {0}; i < fastlanes::CFG::VEC_SZ; i++) {
			const auto byte_offset = opr->idxs[i] * sizeof(KEY_PT);
			target_ptr[i] = load_unaligned<KEY_PT>(rle_vals_bytes + byte_offset);
		}
	}
	template <typename INDEX_PT>
	void operator()(const fastlanes::sp<fastlanes::dec_rle_map_opr<fastlanes::fls_string_t, INDEX_PT>> &opr) const {
		const auto target_ptr = GetDataPtr<string_t>(target_col);

		const auto *bytes = reinterpret_cast<uint8_t *>(opr->rle_vals_segment_view.data);
		const auto *offsets = reinterpret_cast<fastlanes::ofs_t *>(opr->rle_offset_segment_view.data);

		generated::untranspose::fallback::scalar::untranspose_i(opr->idxs, opr->temporary_idxs);

		for (fastlanes::n_t val_idx {0}; val_idx < fastlanes::CFG::VEC_SZ; ++val_idx) {
			const auto cur_idx = opr->temporary_idxs[val_idx];
			const auto cur_ofs = offsets[cur_idx];
			const auto next_offset = offsets[cur_idx + 1];

			target_ptr[val_idx] = StringVector::AddString(target_col, reinterpret_cast<const char *>(bytes + cur_ofs),
			                                              next_offset - cur_ofs);
		}
	}

	template <typename PT>
	void operator()(const fastlanes::sp<fastlanes::dec_null_opr<PT>> &opr) const {
		throw std::runtime_error("Operation not supported");
	}
	template <typename PT>
	void operator()(const fastlanes::sp<fastlanes::dec_transpose_opr<PT>> &opr) const {
		const auto target_ptr = GetDataPtr<PT>(target_col);
		generated::untranspose::fallback::scalar::untranspose_i(opr->transposed_data, &target_ptr[0]);
	}
	template <typename PT>
	void operator()(const fastlanes::sp<fastlanes::dec_slpatch_opr<PT>> &opr) const {
		fastlanes::copy<PT>(opr->data, GetDataPtr<PT>(target_col));
	}
	template <typename PT>
	void operator()(const fastlanes::sp<fastlanes::dec_frequency_opr<PT>> &opr) const {
		fastlanes::copy<PT>(opr->data, GetDataPtr<PT>(target_col));
	}
	void operator()(const fastlanes::sp<fastlanes::dec_frequency_str_opr> &opr) const {
		const auto target = GetDataPtr<string_t>(target_col);
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
		throw std::runtime_error("Operation not supported");
	}
	void operator()(const fastlanes::sp<fastlanes::dec_cross_rle_opr<fastlanes::fls_string_t>> &opr) const {
		throw std::runtime_error("Operation not supported");
	}

	void operator()(const auto &opr) const {
		throw std::runtime_error("Operation not supported");
	}

private:
	//! Wrap the GetData function so we never forget to add the offset when we are mutating another part of the
	//! DuckDB vector.
	template <typename T>
	T *GetDataPtr(Vector &col) const {
		return FlatVector::GetData<T>(col) + offset;
	}

	// template <typename PT>
	// void operator()(const fastlanes::sp<fastlanes::dec_uncompressed_opr<PT>> &opr) const {
	// 	fastlanes::copy<PT>(opr->Data(), GetWritePtr<PT>(v));
	// }
	//
	// template <typename PT>
	// void operator()(const fastlanes::sp<fastlanes::dec_unffor_opr<PT>> &opr) const {
	// 	fastlanes::copy<PT>(opr->Data(), GetWritePtr<PT>(v));
	// }
	//
	// template <typename PT>
	// void operator()(const fastlanes::sp<fastlanes::dec_alp_opr<PT>> &opr) const {
	// 	fastlanes::copy<PT>(opr->decoded_arr, GetWritePtr<PT>(v));
	// }
	//
	// template <typename PT>
	// void operator()(const fastlanes::sp<fastlanes::dec_alp_rd_opr<PT>> &opr) const {
	// 	fastlanes::copy<PT>(opr->glue_arr, GetWritePtr<PT>(v));
	// }
	//
	// template <typename PT>
	// void operator()(const fastlanes::sp<fastlanes::dec_constant_opr<PT>> &opr) const {
	// 	v.SetVectorType(VectorType::CONSTANT_VECTOR);
	// 	ConstantVector::SetNull(v, false);
	//
	// 	fastlanes::copy<PT>(&opr->value, &GetWritePtr<PT>(v));
	// }
	//
	// template <typename KEY_PT, typename INDEX_PT>
	// void operator()(const fastlanes::sp<fastlanes::dec_dict_opr<KEY_PT, INDEX_PT>> &dict_expr) const {
	// 	const auto *key_p = dict_expr->key_segment_view.data_span.data();
	// 	const auto *index_p = dict_expr->Index();
	//
	// 	for (fastlanes::n_t i {0}; i < fastlanes::CFG::VEC_SZ; ++i) {
	// 		GetWritePtr<KEY_PT *>(v)[i] = load_unaligned<KEY_PT>(key_p + index_p[i] * sizeof(KEY_PT));
	// 	}
	// }
};

class ColumnDecoder {
public:
	explicit ColumnDecoder()
	    : ctx() {

	      };

public:
	template <class FinalOpVariant>
	void Decode(const FinalOpVariant &final_op, Vector &v, const size_t offset) {
		MaterializeVisitor visitor {v, offset, ctx};
		std::visit(visitor, final_op);
	};

private:
	ColumnCtx ctx;
};

struct material_visitor {
public:
	explicit material_visitor(const fastlanes::n_t vec_idx, const idx_t offset, Vector &target_col)
	    : vec_idx(vec_idx), offset(offset), target_col(target_col) {};

	/**
	 * Unpack uncompressed values (no decoding required).
	 */
	template <typename PT>
	void operator()(const fastlanes::sp<fastlanes::dec_uncompressed_opr<PT>> &opr) const {
		DPRINT("uncompressed_opr");
		fastlanes::copy<PT>(opr->Data(), GetDataPtr<PT>(target_col));
	}
	/**
	 * Decode FOR vector with bit-packed values.
	 */
	template <typename PT>
	void operator()(const fastlanes::sp<fastlanes::dec_unffor_opr<PT>> &opr) const {
		DPRINT("unffor_opr");
		fastlanes::copy<PT>(opr->Data(), GetDataPtr<PT>(target_col));
	}
	/**
	 * Decode ALP compressed doubles.
	 */
	template <typename PT>
	void operator()(const fastlanes::sp<fastlanes::dec_alp_opr<PT>> &opr) const {
		DPRINT("alp_opr");
		fastlanes::copy<PT>(opr->decoded_arr, GetDataPtr<PT>(target_col));
	}
	/**
	 * Decode ALP compressed doubles, encoded with ALP_rd.
	 */
	template <typename PT>
	void operator()(const fastlanes::sp<fastlanes::dec_alp_rd_opr<PT>> &opr) const {
		DPRINT("alp_rd_opr");
		fastlanes::copy<PT>(opr->glue_arr, GetDataPtr<PT>(target_col));
	}
	/**
	 * Decode constant value.
	 */
	template <typename PT>
	void operator()(const fastlanes::sp<fastlanes::dec_constant_opr<PT>> &opr) const {
		DPRINT("constant_opr");
		target_col.SetVectorType(VectorType::CONSTANT_VECTOR);
		ConstantVector::SetNull(target_col, false);

		fastlanes::copy<PT>(&opr->value, &GetDataPtr<PT>(target_col)[0]);
	}
	/**
	 * Decode constant value with string type.
	 */
	void operator()(const fastlanes::sp<fastlanes::dec_constant_str_opr> &opr) const {
		DPRINT("constant_str_opr");
		target_col.SetVectorType(VectorType::CONSTANT_VECTOR);
		ConstantVector::SetNull(target_col, false);

		const auto constant_value_size = static_cast<fastlanes::len_t>(opr->bytes.size());
		const auto target = GetDataPtr<string_t>(target_col);
		target[0] =
		    StringVector::AddString(target_col, reinterpret_cast<char *>(opr->bytes.data()), constant_value_size);
	}

	template <typename PT>
	void operator()(const fastlanes::sp<fastlanes::PhysicalExpr> &expr) const {
		DPRINT("PhysicalExpr");

		throw std::runtime_error("Operation not supported");
	}
	void operator()(const fastlanes::sp<fastlanes::dec_struct_opr> &struct_expr) const {
		DPRINT("struct_opr");

		throw std::runtime_error("Operation not supported");
	}
	/**
	 * Unpack uncompressed strings.
	 */
	void operator()(const fastlanes::sp<fastlanes::dec_fls_str_uncompressed_opr> &opr) const {
		DPRINT("fls_str_uncompressed_opr");

		const auto target = GetDataPtr<string_t>(target_col);
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

		const auto target_ptr = GetDataPtr<string_t>(target_col);
		auto *in_byte_arr = reinterpret_cast<uint8_t *>(opr->fsst_bytes_segment_view.data);

		generated::untranspose::fallback::scalar::untranspose_i(opr->offset_arr, opr->untrasposed_offset);

		fastlanes::ofs_t prev = 0;
		for (auto i {0}; i < fastlanes::CFG::VEC_SZ; ++i) {
			const fastlanes::ofs_t end = opr->untrasposed_offset[i];
			const fastlanes::len_t len = end - prev;
			prev = end;

			const auto decoded_size = static_cast<fastlanes::ofs_t>(
			    test_fsst_decode(&opr->fsst_decoder, len, in_byte_arr, fastlanes::CFG::String::max_bytes_per_string,
			                     opr->tmp_string.data()));

			in_byte_arr += len;

			// target_ptr[i] = string_t(reinterpret_cast<const char *>(opr->tmp_string.data()), decoded_size);

			target_ptr[i] = StringVector::AddString(target_col, reinterpret_cast<const char *>(opr->tmp_string.data()),
			                                        decoded_size);
		}
	}
	/**
	 * Decode strings which are compressed using FSST (12-bit encoded).
	 *
	 * Allows up to 4096 entries.
	 */
	void operator()(const fastlanes::sp<fastlanes::dec_fsst12_opr> &opr) const {
		DPRINT("fsst12_opr");

		const auto target_ptr = GetDataPtr<string_t>(target_col);
		auto *in_byte_arr = reinterpret_cast<uint8_t *>(opr->fsst12_bytes_segment_view.data);

		generated::untranspose::fallback::scalar::untranspose_i(opr->offset_arr, opr->untrasposed_offset);

		fastlanes::ofs_t prev = 0;
		for (auto i {0}; i < fastlanes::CFG::VEC_SZ; ++i) {
			const fastlanes::ofs_t end = opr->untrasposed_offset[i];
			const fastlanes::len_t len = end - prev;
			prev = end;

			const auto decoded_size = static_cast<fastlanes::ofs_t>(
			    fsst12_decompress(&opr->fsst12_decoder, len, in_byte_arr, fastlanes::CFG::String::max_bytes_per_string,
			                      opr->tmp_string.data()));

			in_byte_arr += len;

			// target_ptr[i] = string_t(reinterpret_cast<const char *>(opr->tmp_string.data()), decoded_size);
			target_ptr[i] = StringVector::AddString(target_col, reinterpret_cast<const char *>(opr->tmp_string.data()),
			                                        decoded_size);
		}
	}
	// DICT
	template <typename KEY_PT, typename INDEX_PT>
	void operator()(const fastlanes::sp<fastlanes::dec_dict_opr<KEY_PT, INDEX_PT>> &dict_expr) const {
		DPRINT("dict_opr<KEY_PT, INDEX_PT>");

		auto dict_size = dict_expr->key_segment_view.data_span.size();
		auto dict_values = make_uniq<Vector>(target_col.GetType(), dict_size);

		// DictionaryVector::SetDictionaryId(result, dictionary_id);

		// Vector::Slice();

		// target_col.SetVectorType(VectorType::DICTIONARY_VECTOR);
		// DictionaryVector::SelVector();
		// DictionaryVector::VerifyDictionary()

		const auto target_ptr = GetDataPtr<KEY_PT>(target_col);

		const auto *key_p = dict_expr->key_segment_view.data_span.data();
		const auto *index_p = dict_expr->Index();

		for (fastlanes::n_t i {0}; i < fastlanes::CFG::VEC_SZ; ++i) {
			target_ptr[i] = load_unaligned<KEY_PT>(key_p + index_p[i] * sizeof(KEY_PT));
		}
	}
	template <typename INDEX_PT>
	void operator()(const fastlanes::sp<fastlanes::dec_dict_opr<fastlanes::fls_string_t, INDEX_PT>> &opr) const {
		DPRINT("dict_opr<fastlanes::fls_string_t, INDEX_PT>");

		const auto target_ptr = GetDataPtr<string_t>(target_col);

		const auto *offset_bytes = opr->dict_offsets_segment.data_span.data();
		const auto *idx_arr = opr->Index();
		const auto *bytes = opr->Bytes();

		for (fastlanes::n_t i = 0; i < fastlanes::CFG::VEC_SZ; ++i) {
			const INDEX_PT k = idx_arr[i];

			const fastlanes::ofs_t cur = load_unaligned<fastlanes::ofs_t>(offset_bytes + k * sizeof(fastlanes::ofs_t));
			const fastlanes::ofs_t prev =
			    (k == 0) ? 0 : load_unaligned<fastlanes::ofs_t>(offset_bytes + (k - 1) * sizeof(fastlanes::ofs_t));

			const auto len = static_cast<idx_t>(cur - prev);
			const auto src = reinterpret_cast<const char *>(bytes + prev);

			// target_ptr[i] = string_t(src, len);
			target_ptr[i] = StringVector::AddString(target_col, src, len);
		}
	}
	template <typename INDEX_PT>
	void operator()(const fastlanes::sp<fastlanes::dec_fsst_dict_opr<INDEX_PT>> &opr) const {
		DPRINT("fsst_dict_opr<INDEX_PT>");
		const auto target_ptr = GetDataPtr<string_t>(target_col);
		auto *in_byte_arr = reinterpret_cast<uint8_t *>(opr->fsst_bytes_segment_view.data);

		FLS_ASSERT_NOT_NULL_POINTER(in_byte_arr)

		for (fastlanes::n_t idx {0}; idx < fastlanes::CFG::VEC_SZ; ++idx) {
			const auto index = opr->Index()[idx];

			fastlanes::ofs_t offset = 0;
			fastlanes::len_t length = 0;

			// todo: do we need this cast?
			const auto offset_bytes = reinterpret_cast<const uint8_t *>(opr->fsst_offset_segment_view.data_span.data());

			if (index == 0) {
				offset = 0;
				length = load_unaligned<fastlanes::ofs_t>(offset_bytes + index * sizeof(fastlanes::ofs_t));
			} else {
				offset = load_unaligned<fastlanes::ofs_t>(offset_bytes + (index - 1) * sizeof(fastlanes::ofs_t));
				fastlanes::ofs_t offset_next = 0;
				offset_next = load_unaligned<fastlanes::ofs_t>(offset_bytes + index * sizeof(fastlanes::ofs_t));

				length = offset_next - offset;
			}

			const auto decoded_size = static_cast<fastlanes::ofs_t>(
			    test_fsst_decode(&opr->fsst_decoder, length, in_byte_arr + offset,
			                     fastlanes::CFG::String::max_bytes_per_string, opr->tmp_string.data()));

			target_ptr[idx] = StringVector::AddString(
			    target_col, reinterpret_cast<const char *>(opr->tmp_string.data()), decoded_size);
		}
	}
	template <typename INDEX_PT>
	void operator()(const fastlanes::sp<fastlanes::dec_fsst12_dict_opr<INDEX_PT>> &opr) const {
		DPRINT("fsst12_dict_opr<INDEX_PT>");

		const auto target_ptr = GetDataPtr<string_t>(target_col);
		auto *in_byte_arr = reinterpret_cast<uint8_t *>(opr->fsst12_bytes_segment_view.data);

		FLS_ASSERT_NOT_NULL_POINTER(in_byte_arr)

		for (fastlanes::n_t idx {0}; idx < fastlanes::CFG::VEC_SZ; ++idx) {
			const auto index = opr->Index()[idx];

			fastlanes::ofs_t offset = 0;
			fastlanes::len_t length = 0;

			if (index == 0) {
				offset = 0;
				length = opr->Offsets()[index];
			} else {
				offset = opr->Offsets()[index - 1];
				const auto offset_next = opr->Offsets()[index];
				length = offset_next - offset;
			}
			FLS_ASSERT_LE(length, fastlanes::CFG::String::max_bytes_per_string)

			const auto decoded_size = static_cast<fastlanes::ofs_t>(
			    fsst12_decompress(&opr->fsst12_decoder, length, in_byte_arr + offset,
			                      fastlanes::CFG::String::max_bytes_per_string, opr->tmp_string.data()));

			FLS_ASSERT_L(decoded_size, opr->tmp_string.capacity())

			target_ptr[idx] = StringVector::AddString(
			    target_col, reinterpret_cast<const char *>(opr->tmp_string.data()), decoded_size);
		}
	}
	template <typename KEY_PT, typename INDEX_PT>
	void operator()(const fastlanes::sp<fastlanes::dec_rle_map_opr<KEY_PT, INDEX_PT>> &opr) const {
		DPRINT("rle_map_opr<KEY_PT, INDEX_PT>");
		static_assert(!std::is_same_v<KEY_PT, fastlanes::fls_string_t>,
		              "Generic Decode logic cannot handle fls_string_t!");

		const auto target_ptr = GetDataPtr<KEY_PT>(target_col);
		const auto *rle_vals_bytes = opr->rle_vals_segment_view.data;

		for (auto i {0}; i < fastlanes::CFG::VEC_SZ; i++) {
			const auto byte_offset = opr->idxs[i] * sizeof(KEY_PT);
			target_ptr[i] = load_unaligned<KEY_PT>(rle_vals_bytes + byte_offset);
		}
	}
	template <typename INDEX_PT>
	void operator()(const fastlanes::sp<fastlanes::dec_rle_map_opr<fastlanes::fls_string_t, INDEX_PT>> &opr) const {
		DPRINT("rle_map_opr<fls_string_t, INDEX_PT>");

		const auto target_ptr = GetDataPtr<string_t>(target_col);

		const auto *bytes = reinterpret_cast<uint8_t *>(opr->rle_vals_segment_view.data);
		const auto *offsets = reinterpret_cast<fastlanes::ofs_t *>(opr->rle_offset_segment_view.data);

		generated::untranspose::fallback::scalar::untranspose_i(opr->idxs, opr->temporary_idxs);

		for (fastlanes::n_t val_idx {0}; val_idx < fastlanes::CFG::VEC_SZ; ++val_idx) {
			const auto cur_idx = opr->temporary_idxs[val_idx];
			const auto cur_ofs = offsets[cur_idx];
			const auto next_offset = offsets[cur_idx + 1];

			target_ptr[val_idx] = StringVector::AddString(target_col, reinterpret_cast<const char *>(bytes + cur_ofs),
			                                              next_offset - cur_ofs);
		}
	}

	template <typename PT>
	void operator()(const fastlanes::sp<fastlanes::dec_null_opr<PT>> &opr) const {
		DPRINT("null_opr<PT>");

		throw std::runtime_error("Operation not supported");
	}
	template <typename PT>
	void operator()(const fastlanes::sp<fastlanes::dec_transpose_opr<PT>> &opr) const {
		DPRINT("transpose_opr<PT>");

		const auto target_ptr = GetDataPtr<PT>(target_col);
		generated::untranspose::fallback::scalar::untranspose_i(opr->transposed_data, &target_ptr[0]);
	}
	template <typename PT>
	void operator()(const fastlanes::sp<fastlanes::dec_slpatch_opr<PT>> &opr) const {
		DPRINT("slpatch_opr<PT>");

		fastlanes::copy<PT>(opr->data, GetDataPtr<PT>(target_col));
	}
	template <typename PT>
	void operator()(const fastlanes::sp<fastlanes::dec_frequency_opr<PT>> &opr) const {
		DPRINT("frequency_opr<PT>");

		fastlanes::copy<PT>(opr->data, GetDataPtr<PT>(target_col));
	}
	void operator()(const fastlanes::sp<fastlanes::dec_frequency_str_opr> &opr) const {
		DPRINT("frequency_str_opr");

		const auto target = GetDataPtr<string_t>(target_col);
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

		const auto target = GetDataPtr<PT>(target_col);

		const auto *lengths = opr->lengths_segment.data;
		const auto *values = opr->values_segment.data;

		const auto size = opr->lengths_segment.data_span.size() / sizeof(fastlanes::len_t);

		t_decode_rle_range(lengths, values, size, vec_idx, target);
	}
	void operator()(const fastlanes::sp<fastlanes::dec_cross_rle_opr<fastlanes::fls_string_t>> &opr) const {
		DPRINT("cross_rle_opr<fastlanes::fls_string_t>");

		const auto target = GetDataPtr<string_t>(target_col);

		const auto *lengths = opr->lengths_segment.data;
		const auto *offsets = opr->values_offset_seg.data;
		const auto *values_bytes = reinterpret_cast<const uint8_t *>(opr->values_bytes_seg.data);

		const auto size = opr->lengths_segment.data_span.size() / sizeof(fastlanes::len_t);

		t_decode_rle_range(lengths, values_bytes, offsets, size, vec_idx, target_col, target);
	}

	void operator()(const auto &opr) const {
		throw std::runtime_error("Operation not supported");
	}

private:
	//! Wrap the GetData function so we never forget to add the offset when we are mutating another part of the
	//! DuckDB vector.
	template <typename T>
	T *GetDataPtr(Vector &col) const {
		return FlatVector::GetData<T>(col) + offset;
	}

private:
	fastlanes::n_t vec_idx;
	idx_t offset;
	Vector &target_col;
};
} // namespace duckdb
