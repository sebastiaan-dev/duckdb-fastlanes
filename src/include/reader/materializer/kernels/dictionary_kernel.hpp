#pragma once

#include "reader/materializer/fls_type_resolver.hpp"
#include <duckdb/common/types/string_type.hpp>
#include <duckdb/common/types/vector.hpp>
#include <fls/common/alias.hpp>
#include <fls/expression/dict_expression.hpp>

namespace duckdb::materializer {

template <typename KEY_PT, typename INDEX_PT>
struct KernelTraits<fastlanes::dec_dict_opr<KEY_PT, INDEX_PT>> {
	static void Prepare(ColumnCtxHandle&,
	                    LogicalType&,
	                    fastlanes::dec_dict_opr<KEY_PT, INDEX_PT>&,
	                    const std::vector<FastLanesScanFilter*>*) {
	}

	template <Pass PASS>
	static void Decode(ColumnCtxHandle&, Vector& col, idx_t, fastlanes::dec_dict_opr<KEY_PT, INDEX_PT>& op) {
		const auto  target_ptr = GetDataPtr<PASS, KEY_PT>(col);
		const auto* key_p      = op.key_segment_view.data_span.data();
		const auto* index_p    = op.Index();
		for (fastlanes::n_t i = 0; i < fastlanes::CFG::VEC_SZ; ++i) {
			target_ptr[i] = detail::LoadValue<KEY_PT>(key_p + index_p[i] * sizeof(KEY_PT));
		}
	}
};

template <typename INDEX_PT>
struct KernelTraits<fastlanes::dec_dict_opr<fastlanes::fls_string_t, INDEX_PT>> {
	static void Prepare(ColumnCtxHandle&,
	                    LogicalType&,
	                    fastlanes::dec_dict_opr<fastlanes::fls_string_t, INDEX_PT>&,
	                    const std::vector<FastLanesScanFilter*>*) {
	}

	template <Pass PASS>
	static void
	Decode(ColumnCtxHandle&, Vector& col, idx_t, fastlanes::dec_dict_opr<fastlanes::fls_string_t, INDEX_PT>& op) {
		col.SetVectorType(VectorType::FLAT_VECTOR);
		auto        target_ptr = GetDataPtr<PASS, string_t>(col);
		const auto* offsets    = op.dict_offsets_segment.data_span.data();
		const auto* idx_arr    = op.Index();
		const auto* bytes      = op.Bytes();
		for (fastlanes::n_t i = 0; i < fastlanes::CFG::VEC_SZ; ++i) {
			const INDEX_PT k   = idx_arr[i];
			const auto     cur = detail::LoadValue<fastlanes::ofs_t>(offsets + k * sizeof(fastlanes::ofs_t));
			const auto     prev =
                (k == 0) ? 0 : detail::LoadValue<fastlanes::ofs_t>(offsets + (k - 1) * sizeof(fastlanes::ofs_t));
			target_ptr[i] = StringVector::AddString(col, reinterpret_cast<const char*>(bytes + prev), cur - prev);
		}
	}
};

} // namespace duckdb::materializer
