#pragma once

#include "reader/materializer/fls_type_resolver.hpp"
#include <duckdb/common/types/string_type.hpp>
#include <duckdb/common/types/vector.hpp>
#include <fls/common/alias.hpp>
#include <fls/expression/dict_expression.hpp>

namespace duckdb::materializer {

template <Pass PASS, typename DST, typename KEY_PT, typename INDEX_PT>
static void KeyCopy(Vector& col, fastlanes::dec_dict_opr<KEY_PT, INDEX_PT>& op) {
	auto        keys    = op.Keys();
	auto*       out     = GetDataPtr<PASS, DST>(col);
	const auto* indexes = op.Index();

	for (fastlanes::n_t i = 0; i < fastlanes::CFG::VEC_SZ; ++i) {
		out[i] = static_cast<DST>(keys[indexes[i]]);
	}
}

template <typename KEY_PT, typename INDEX_PT>
struct KernelTraits<fastlanes::dec_dict_opr<KEY_PT, INDEX_PT>> {
	static void Prepare(ColumnCtxHandle&,
	                    LogicalType&,
	                    fastlanes::dec_dict_opr<KEY_PT, INDEX_PT>&,
	                    const std::vector<FastLanesScanFilter*>*) {
	}

	template <Pass PASS>
	static void
	Decode(ColumnCtxHandle&, Vector& col, idx_t, fastlanes::dec_dict_opr<KEY_PT, INDEX_PT>& op, fastlanes::DataType&) {
		switch (col.GetType().InternalType()) {
		case PhysicalType::INT8:
			KeyCopy<PASS, int8_t>(col, op);
			break;
		case PhysicalType::INT16:
			KeyCopy<PASS, int16_t>(col, op);
			break;
		case PhysicalType::INT32:
			KeyCopy<PASS, int32_t>(col, op);
			break;
		case PhysicalType::INT64:
			KeyCopy<PASS, int64_t>(col, op);
			break;
		default:
			throw std::runtime_error("Unsupported DECIMAL physical type");
		}
	}
};

template <typename INDEX_PT>
struct KernelTraits<fastlanes::dec_dict_opr<fastlanes::fls_string_t, INDEX_PT>> {
	static void Prepare(ColumnCtxHandle&                                            ctx,
	                    LogicalType&                                                type,
	                    fastlanes::dec_dict_opr<fastlanes::fls_string_t, INDEX_PT>& op,
	                    const std::vector<FastLanesScanFilter*>*) {
		auto& c = ctx.Emplace<FSSTDictColumnCtx>();

		c.dict_size = op.dict_offsets_segment.data_span.size() / sizeof(fastlanes::ofs_t);
		c.dict_vec  = make_uniq<Vector>(type, c.dict_size);
		c.sel_vec   = make_uniq<SelectionVector>(2048);

		const auto  child_ptr = GetDataPtr<Pass::First, string_t>(*c.dict_vec);
		const auto* offsets   = op.dict_offsets_segment.data_span.data();
		const auto* bytes     = op.Bytes();

		fastlanes::ofs_t prev_end = 0;
		for (fastlanes::n_t i {0}; i < c.dict_size; ++i) {
			const auto cur  = detail::LoadValue<fastlanes::ofs_t>(offsets + i * sizeof(fastlanes::ofs_t));
			const auto prev = cur - prev_end;

			child_ptr[i] =
			    StringVector::AddString(*c.dict_vec, reinterpret_cast<const char*>(bytes + prev), cur - prev);
			prev_end = cur;
		}
	}

	template <Pass PASS>
	static void Decode(ColumnCtxHandle& ctx,
	                   Vector&          col,
	                   idx_t,
	                   fastlanes::dec_dict_opr<fastlanes::fls_string_t, INDEX_PT>& op,
	                   fastlanes::DataType&) {
		const auto& c     = ctx.Expect<FSSTDictColumnCtx>();
		auto        index = op.Index();
		auto&       sel   = *c.sel_vec;

		if constexpr (PASS == Pass::First) {
			for (idx_t i = 0; i < 1024; ++i) {
				sel.set_index(i, static_cast<sel_t>(index[i]));
			}
			col.Dictionary(*c.dict_vec, c.dict_size, sel, 1024);
			DictionaryVector::SetDictionaryId(col, to_string(CastPointerToValue(c.dict_vec->GetBuffer().get())));
		} else {
			for (idx_t i = 0; i < 1024; ++i) {
				sel.set_index(i + 1024, static_cast<sel_t>(index[i]));
			}
			col.Dictionary(*c.dict_vec, c.dict_size, sel, 2048);
		}
	}
};

// template <typename INDEX_PT>
// struct KernelTraits<fastlanes::dec_dict_opr<fastlanes::fls_string_t, INDEX_PT>> {
// 	static void Prepare(ColumnCtxHandle&,
// 	                    LogicalType&,
// 	                    fastlanes::dec_dict_opr<fastlanes::fls_string_t, INDEX_PT>&,
// 	                    const std::vector<FastLanesScanFilter*>*) {
//
//
// 	}
//
// 	template <Pass PASS>
// 	static void Decode(ColumnCtxHandle&,
// 	                   Vector& col,
// 	                   idx_t,
// 	                   fastlanes::dec_dict_opr<fastlanes::fls_string_t, INDEX_PT>& op,
// 	                   fastlanes::DataType&) {
// 		col.SetVectorType(VectorType::FLAT_VECTOR);
// 		auto        target_ptr = GetDataPtr<PASS, string_t>(col);
// 		const auto* offsets    = op.dict_offsets_segment.data_span.data();
// 		const auto* idx_arr    = op.Index();
// 		const auto* bytes      = op.Bytes();
//
//
// 		for (fastlanes::n_t i = 0; i < fastlanes::CFG::VEC_SZ; ++i) {
// 			const INDEX_PT k   = idx_arr[i];
// 			const auto     cur = detail::LoadValue<fastlanes::ofs_t>(offsets + k * sizeof(fastlanes::ofs_t));
// 			const auto     prev =
//                 (k == 0) ? 0 : detail::LoadValue<fastlanes::ofs_t>(offsets + (k - 1) * sizeof(fastlanes::ofs_t));
// 			target_ptr[i] = StringVector::AddString(col, reinterpret_cast<const char*>(bytes + prev), cur - prev);
// 		}
// 	}
// };

} // namespace duckdb::materializer
