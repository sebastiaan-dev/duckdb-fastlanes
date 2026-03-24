#pragma once

#include "fls/expression/rle_expression.hpp"
#include "reader/materializer/fls_type_resolver.hpp"

namespace duckdb::materializer {
template <typename KEY_PT, typename INDEX_PT>
struct KernelTraits<fastlanes::dec_rle_map_opr<KEY_PT, INDEX_PT>> {
	static void Prepare(ColumnCtxHandle&,
	                    LogicalType&,
	                    fastlanes::dec_rle_map_opr<KEY_PT, INDEX_PT>&,
	                    const std::vector<FastLanesScanFilter*>*) {
	}

	template <Pass PASS>
	static void Decode(
	    ColumnCtxHandle&, Vector& col, idx_t, fastlanes::dec_rle_map_opr<KEY_PT, INDEX_PT>& op, fastlanes::DataType&) {
		//  FIXME: Decode
		auto* rle_vals = reinterpret_cast<KEY_PT*>(op.rle_vals_segment_view.data);
		for (auto idx {0}; idx < fastlanes::CFG::VEC_SZ; idx++) {
			detail::NumericHelper<PASS>::AssignValue(rle_vals[op.idxs[idx]], col, idx);
		}
	}
};

template <typename INDEX_PT>
struct KernelTraits<fastlanes::dec_rle_map_opr<fastlanes::fls_string_t, INDEX_PT>> {
	static void Prepare(ColumnCtxHandle&,
	                    LogicalType&,
	                    fastlanes::dec_rle_map_opr<fastlanes::fls_string_t, INDEX_PT>&,
	                    const std::vector<FastLanesScanFilter*>*) {
	}

	template <Pass PASS>
	static void Decode(ColumnCtxHandle&,
	                   Vector& col,
	                   idx_t,
	                   fastlanes::dec_rle_map_opr<fastlanes::fls_string_t, INDEX_PT>& op,
	                   fastlanes::DataType&) {
		const auto target_ptr = GetDataPtr<PASS, string_t>(col);

		const auto* bytes   = reinterpret_cast<uint8_t*>(op.rle_vals_segment_view.data);
		const auto* offsets = reinterpret_cast<fastlanes::ofs_t*>(op.rle_offset_segment_view.data);

#if defined(FLS_NO_TRANSPOSE) && FLS_NO_TRANSPOSE
		// fastlanes::copy(op.idxs, op.temporary_idxs);
		generated::untranspose::fallback::scalar::untranspose_i(op.idxs, op.temporary_idxs);
#else
		generated::untranspose::fallback::scalar::untranspose_i(op.idxs, op.temporary_idxs);
#endif

		for (fastlanes::n_t val_idx {0}; val_idx < fastlanes::CFG::VEC_SZ; ++val_idx) {
			const auto cur_idx     = op.temporary_idxs[val_idx];
			const auto cur_ofs     = offsets[cur_idx];
			const auto next_offset = offsets[cur_idx + 1];

			target_ptr[val_idx] =
			    StringVector::AddString(col, reinterpret_cast<const char*>(bytes + cur_ofs), next_offset - cur_ofs);
		}
	}
};

} // namespace duckdb::materializer
