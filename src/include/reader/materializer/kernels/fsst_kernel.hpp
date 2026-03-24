#pragma once

#include "fls_gen/untranspose/untranspose.hpp"
#include "reader/materializer/context.hpp"
#include <duckdb/common/types/string_type.hpp>
#include <duckdb/common/types/vector.hpp>
#include <fls/expression/fsst_expression.hpp>

namespace duckdb::materializer {

template <>
struct KernelTraits<fastlanes::dec_fsst_opr> {
	static void Prepare(ColumnCtxHandle&         ctx,
	                    LogicalType&             type,
	                    fastlanes::dec_fsst_opr& opr,
	                    const std::vector<FastLanesScanFilter*>*) {
	}

	template <Pass PASS>
	static void
	Decode(ColumnCtxHandle& ctx, Vector& target_col, idx_t, fastlanes::dec_fsst_opr& opr, fastlanes::DataType&) {
		auto& str_buffer  = StringVector::GetStringBuffer(target_col);
		auto target_ptr = GetDataPtr<PASS, string_t>(target_col);

		generated::untranspose::fallback::scalar::untranspose_i(opr.offset_arr, opr.untrasposed_offset);

		const auto* __restrict offs = opr.untrasposed_offset; // length VEC_SZ
		fastlanes::len_t sizes[fastlanes::CFG::VEC_SZ];
		{
			fastlanes::ofs_t prev = 0;
			for (idx_t i = 0; i < fastlanes::CFG::VEC_SZ; ++i) {
				const auto cur = offs[i];
				sizes[i]       = static_cast<fastlanes::len_t>(cur - prev);
				prev           = cur;
			}
		}

		const auto base = reinterpret_cast<uint8_t*>(opr.fsst_bytes_segment_view.data);
		uint8_t* __restrict in_ptrs[fastlanes::CFG::VEC_SZ];
		{
			fastlanes::ofs_t acc = 0;
			for (idx_t i = 0; i < fastlanes::CFG::VEC_SZ; ++i) {
				in_ptrs[i] = base + acc;
				acc += sizes[i];
			}
		}

		for (idx_t i = 0; i < fastlanes::CFG::VEC_SZ; ++i) {
			const fastlanes::len_t enc = sizes[i];
			const idx_t max_out = static_cast<idx_t>(enc) * 8;
			const data_ptr_t buf = str_buffer.AllocateShrinkableBuffer(max_out);

			const auto decoded =
			    static_cast<fastlanes::ofs_t>(fsst_decompress(&opr.fsst_decoder, enc, in_ptrs[i], max_out, buf));

			target_ptr[i] = str_buffer.FinalizeShrinkableBuffer(buf, max_out, decoded);
		}
	}
};

} // namespace duckdb::materializer
