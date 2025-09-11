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

struct KeepAlive : VectorBuffer {
	fastlanes::sp<fastlanes::dec_fsst_opr> hold;
	explicit KeepAlive(fastlanes::sp<fastlanes::dec_fsst_opr> p)
	    : VectorBuffer(VectorBufferType::MANAGED_BUFFER), hold(std::move(p)) {
	}
};

struct ColumnCtxBase {
	virtual ~ColumnCtxBase() = default;
};

struct FSSTColumnCtx final : ColumnCtxBase {
	unique_ptr<Vector> aux_vec;
	buffer_ptr<void> decoder;
};

struct FSSTDictColumnCtx final : ColumnCtxBase {
	size_t dict_size;
	unique_ptr<Vector> dict_vec;
	buffer_ptr<void> decoder;
};

struct ColumnCtxHandle {
	std::unique_ptr<ColumnCtxBase> ctx_p;

	template <typename T, typename... Args>
	T &Emplace(Args &&...args) {
		ctx_p = std::make_unique<T>(std::forward<Args>(args)...);
		return *static_cast<T *>(ctx_p.get());
	}
	template <typename T>
	T &Expect() {
		return *static_cast<T *>(ctx_p.get());
	}
};

enum class Pass : bool { First = false, Second = true };

template <bool INIT, Pass PASS>
struct MaterializeVisitor {
	Vector &target_col;
	ColumnCtxHandle &ctx;

	template <typename PT>
	void operator()(const fastlanes::sp<fastlanes::dec_uncompressed_opr<PT>> &opr) const {
		fastlanes::copy<PT>(opr->Data(), GetDataPtr<PT>(target_col));
	}
	template <typename PT>
	void operator()(const fastlanes::sp<fastlanes::dec_unffor_opr<PT>> &opr) const {
		fastlanes::copy<PT>(opr->Data(), GetDataPtr<PT>(target_col));
	}
	template <typename PT>
	void operator()(const fastlanes::sp<fastlanes::dec_alp_opr<PT>> &opr) const {
		fastlanes::copy<PT>(opr->decoded_arr, GetDataPtr<PT>(target_col));
	}
	template <typename PT>
	void operator()(const fastlanes::sp<fastlanes::dec_alp_rd_opr<PT>> &opr) const {
		fastlanes::copy<PT>(opr->glue_arr, GetDataPtr<PT>(target_col));
	}
	template <typename PT>
	void operator()(const fastlanes::sp<fastlanes::dec_constant_opr<PT>> &opr) const {
		target_col.SetVectorType(VectorType::CONSTANT_VECTOR);
		ConstantVector::SetNull(target_col, false);

		fastlanes::copy<PT>(&opr->value, &GetDataPtr<PT>(target_col)[0]);
	}

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

	void operator()(const fastlanes::sp<fastlanes::dec_fsst_opr> &opr) const {
		if constexpr (INIT) {
			auto &c = ctx.Emplace<FSSTColumnCtx>();
			const fsst_decoder_t &dec_ref = opr->fsst_decoder;
			c.decoder =
			    buffer_ptr<void>(new fsst_decoder_t(dec_ref), [](void *p) { delete static_cast<fsst_decoder_t *>(p); });
			c.aux_vec = make_uniq<Vector>(target_col.GetType(), 2048);
			c.aux_vec->SetVectorType(VectorType::FSST_VECTOR);
			FSSTVector::RegisterDecoder(*c.aux_vec, c.decoder, fastlanes::CFG::String::max_bytes_per_string);

			return;
		}

		const auto &c = ctx.Expect<FSSTColumnCtx>();

		if constexpr (PASS == Pass::First) {
			target_col.SetVectorType(VectorType::FSST_VECTOR);
			target_col.Reinterpret(*c.aux_vec);
			FSSTVector::SetCount(target_col, 1024);
		}

		if constexpr (PASS == Pass::Second) {
			FSSTVector::SetCount(target_col, 2048);
		}

		const auto out = GetCompressedStringPtr(target_col);
		const auto *bytes = reinterpret_cast<uint8_t *>(opr->fsst_bytes_segment_view.data);

		generated::untranspose::fallback::scalar::untranspose_i(opr->offset_arr, opr->untrasposed_offset);

		fastlanes::ofs_t prev_end = 0;
		for (idx_t i = 0; i < 1024; ++i) {
			const fastlanes::ofs_t end = opr->untrasposed_offset[i];
			const fastlanes::ofs_t len = end - prev_end;

			out[i] = FSSTVector::AddCompressedString(target_col, reinterpret_cast<const char *>(bytes + prev_end), len);

			prev_end = end;
		}
	}

	void operator()(const fastlanes::sp<fastlanes::dec_fsst12_opr> &opr) const {
		if constexpr (INIT) {
			auto &c = ctx.Emplace<FSSTColumnCtx>();
			const fsst12_decoder_t &dec_ref = opr->fsst12_decoder;
			c.decoder = buffer_ptr<void>(new fsst12_decoder_t(dec_ref),
			                             [](void *p) { delete static_cast<fsst12_decoder_t *>(p); });
			c.aux_vec = make_uniq<Vector>(target_col.GetType(), 2048);
			c.aux_vec->SetVectorType(VectorType::FSST_VECTOR);
			FSSTVector::RegisterDecoder(*c.aux_vec, c.decoder, fastlanes::CFG::String::max_bytes_per_string);

			return;
		}

		const auto &c = ctx.Expect<FSSTColumnCtx>();
		if constexpr (PASS == Pass::First) {
			target_col.SetVectorType(VectorType::FSST_VECTOR);
			target_col.Reinterpret(*c.aux_vec);
			FSSTVector::SetCount(target_col, 1024);
		}
		if constexpr (PASS == Pass::Second) {
			FSSTVector::SetCount(target_col, 2048);
		}

		const auto out = GetCompressedStringPtr(target_col);
		const auto *bytes = reinterpret_cast<uint8_t *>(opr->fsst12_bytes_segment_view.data);

		generated::untranspose::fallback::scalar::untranspose_i(opr->offset_arr, opr->untrasposed_offset);

		fastlanes::ofs_t prev_end = 0;
		for (idx_t i = 0; i < 1024; ++i) {
			const fastlanes::ofs_t end = opr->untrasposed_offset[i];
			const fastlanes::ofs_t len = end - prev_end;

			out[i] = FSSTVector::AddCompressedString(target_col, reinterpret_cast<const char *>(bytes + prev_end), len);

			prev_end = end;
		}
	}
	template <typename KEY_PT, typename INDEX_PT>
	void operator()(const fastlanes::sp<fastlanes::dec_dict_opr<KEY_PT, INDEX_PT>> &dict_expr) const {
		const auto target_ptr = GetDataPtr<KEY_PT>(target_col);
		const auto *key_p = dict_expr->key_segment_view.data_span.data();
		const auto *index_p = dict_expr->Index();
		// const auto *key_p = dict_expr->Keys();

		for (fastlanes::n_t i {0}; i < fastlanes::CFG::VEC_SZ; ++i) {
			//target_ptr[i] = key_p[index_p[i]];
			target_ptr[i] = load_unaligned<KEY_PT>(key_p + index_p[i] * sizeof(KEY_PT));
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

			target_ptr[i] = StringVector::AddString(target_col, src, len);
		}
	}
	template <typename INDEX_PT>
	void operator()(const fastlanes::sp<fastlanes::dec_fsst_dict_opr<INDEX_PT>> &opr) const {
		if constexpr (INIT) {
			auto &c = ctx.Emplace<FSSTDictColumnCtx>();
			c.dict_size = opr->fsst_offset_segment_view.data_span.size() / sizeof(fastlanes::ofs_t);
			c.dict_vec = make_uniq<Vector>(target_col.GetType(), c.dict_size);

			const auto child_ptr = GetDataPtr<string_t>(*c.dict_vec);
			const auto offset_bytes = reinterpret_cast<const uint8_t *>(opr->fsst_offset_segment_view.data_span.data());
			const auto *in_byte_arr = reinterpret_cast<uint8_t *>(opr->fsst_bytes_segment_view.data);

			fastlanes::ofs_t prev_end = 0;
			for (fastlanes::n_t i {0}; i < c.dict_size; ++i) {
				const fastlanes::ofs_t end =
				    load_unaligned<fastlanes::ofs_t>(offset_bytes + i * sizeof(fastlanes::ofs_t));
				const fastlanes::ofs_t enc_len = end - prev_end;

				const auto decoded_size = static_cast<fastlanes::ofs_t>(
				    test_fsst_decode(&opr->fsst_decoder, enc_len, in_byte_arr + prev_end,
				                     fastlanes::CFG::String::max_bytes_per_string, opr->tmp_string.data()));

				child_ptr[i] = StringVector::AddString(
				    *c.dict_vec, reinterpret_cast<const char *>(opr->tmp_string.data()), decoded_size);

				prev_end = end;
			}

			return;
		}

		auto &c = ctx.Expect<FSSTDictColumnCtx>();
		if constexpr (PASS == Pass::First) {
			SelectionVector sel_vec(2048);
			for (idx_t i = 0; i < 1024; i++) {
				sel_vec.set_index(i, static_cast<sel_t>(opr->Index()[i]));
			}

			target_col.SetVectorType(VectorType::DICTIONARY_VECTOR);
			target_col.Reference(*c.dict_vec);
			target_col.Slice(sel_vec, 2048);
		}
		if constexpr (PASS == Pass::Second) {
			auto sel_vec = DictionaryVector::SelVector(target_col);

			for (idx_t i = 0; i < 1024; i++) {
				sel_vec.set_index(i + 1024, static_cast<sel_t>(opr->Index()[i]));
			}
		}
	}
	template <typename INDEX_PT>
	void operator()(const fastlanes::sp<fastlanes::dec_fsst12_dict_opr<INDEX_PT>> &opr) const {
		const auto target_ptr = GetDataPtr<string_t>(target_col);
		auto *in_byte_arr = reinterpret_cast<uint8_t *>(opr->fsst12_bytes_segment_view.data);

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

			const auto decoded_size = static_cast<fastlanes::ofs_t>(
			    fsst12_decompress(&opr->fsst12_decoder, length, in_byte_arr + offset,
			                      fastlanes::CFG::String::max_bytes_per_string, opr->tmp_string.data()));

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
	static T *GetDataPtr(Vector &col) {
		if constexpr (PASS == Pass::First) {
			return FlatVector::GetData<T>(col);
		}
		return FlatVector::GetData<T>(col) + fastlanes::CFG::VEC_SZ;
	}

	static string_t *GetCompressedStringPtr(Vector &col) {
		if constexpr (PASS == Pass::First) {
			return FSSTVector::GetCompressedData<string_t>(col);
		}
		return FSSTVector::GetCompressedData<string_t>(col) + fastlanes::CFG::VEC_SZ;
	}
};

class ColumnDecoder {
public:
	explicit ColumnDecoder()
	    : ctx() {

	      };

public:
	template <class FinalOpVariant>
	void Init(const FinalOpVariant &final_op, Vector &v) {
		// std::cout << "INIT" << "\n";
		MaterializeVisitor<true, Pass::First> visitor {v, ctx};
		std::visit(visitor, final_op);
	}

	template <Pass PASS, class FinalOpVariant>
	void Decode(const FinalOpVariant &final_op, Vector &v) {
		// std::cout << "DECODE" << "\n";
		MaterializeVisitor<false, PASS> visitor {v, ctx};
		std::visit(visitor, final_op);
	};

private:
	ColumnCtxHandle ctx;
};
} // namespace duckdb
