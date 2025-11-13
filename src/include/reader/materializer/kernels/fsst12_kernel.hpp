#pragma once

#include "reader/materializer/context.hpp"
#include <fls/expression/fsst12_expression.hpp>

namespace duckdb::materializer {

template <>
struct KernelTraits<fastlanes::dec_fsst12_opr> {
	static void
	Prepare(ColumnCtxHandle&, LogicalType&, fastlanes::dec_fsst12_opr&, const std::vector<FastLanesScanFilter*>*) {
	}

	template <Pass PASS>
	static void Decode(ColumnCtxHandle&, Vector& col, idx_t, fastlanes::dec_fsst12_opr& opr, fastlanes::DataType&) {
		auto&      str_buffer = StringVector::GetStringBuffer(col);
		const auto target_ptr = GetDataPtr<PASS, string_t>(col);
		const auto base       = reinterpret_cast<uint8_t*>(opr.fsst12_bytes_segment_view.data);

#if defined(FLS_NO_TRANSPOSE) && FLS_NO_TRANSPOSE
		// fastlanes::copy(opr.offset_arr, opr.untrasposed_offset);
		generated::untranspose::fallback::scalar::untranspose_i(opr.offset_arr, opr.untrasposed_offset);
#else
		generated::untranspose::fallback::scalar::untranspose_i(opr.offset_arr, opr.untrasposed_offset);
#endif

		fastlanes::len_t sizes[fastlanes::CFG::VEC_SZ];
		{
			fastlanes::ofs_t prev                   = 0;
			const fastlanes::ofs_t* __restrict offs = opr.untrasposed_offset;
			for (idx_t i = 0; i < fastlanes::CFG::VEC_SZ; ++i) {
				const fastlanes::ofs_t cur = offs[i];
				sizes[i]                   = cur - prev;
				prev                       = cur;
			}
		}

		uint8_t* __restrict in_ptrs[fastlanes::CFG::VEC_SZ];
		{
			fastlanes::ofs_t acc = 0;
			for (idx_t i = 0; i < fastlanes::CFG::VEC_SZ; ++i) {
				in_ptrs[i] = base + acc;
				acc += sizes[i];
			}
		}

		for (idx_t i = 0; i < fastlanes::CFG::VEC_SZ; ++i) {
			const fastlanes::len_t enc     = sizes[i];
			const idx_t            max_out = static_cast<idx_t>(enc) * 6;
			const data_ptr_t       buf     = str_buffer.AllocateShrinkableBuffer(max_out);

			const auto decoded_len =
			    static_cast<fastlanes::ofs_t>(fsst12_decompress(&opr.fsst12_decoder, enc, in_ptrs[i], max_out, buf));
			target_ptr[i] = str_buffer.FinalizeShrinkableBuffer(buf, max_out, decoded_len);
		}
	}
};

} // namespace duckdb::materializer
