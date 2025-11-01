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
		const auto target_ptr  = GetDataPtr<PASS, string_t>(col);
		auto*      in_byte_arr = reinterpret_cast<uint8_t*>(opr.fsst12_bytes_segment_view.data);

#if defined(FLS_NO_TRANSPOSE) && FLS_NO_TRANSPOSE
		// fastlanes::copy(opr.offset_arr, opr.untrasposed_offset);
		generated::untranspose::fallback::scalar::untranspose_i(opr.offset_arr, opr.untrasposed_offset);
#else
		generated::untranspose::fallback::scalar::untranspose_i(opr.offset_arr, opr.untrasposed_offset);
#endif

		for (auto i {0}; i < fastlanes::CFG::VEC_SZ; ++i) {
			fastlanes::len_t encoded_size {0};

			if (i == 0) {
				encoded_size = opr.untrasposed_offset[0];
			} else {
				fastlanes::ofs_t offset {0};
				offset                 = opr.untrasposed_offset[i - 1];
				const auto offset_next = opr.untrasposed_offset[i];
				encoded_size           = offset_next - offset;
			}

			const auto length =
			    static_cast<fastlanes::ofs_t>(fsst12_decompress(&opr.fsst12_decoder,
			                                                    encoded_size,
			                                                    in_byte_arr,
			                                                    fastlanes::CFG::String::max_bytes_per_string,
			                                                    opr.tmp_string.data()));

			in_byte_arr += encoded_size;

			target_ptr[i] = StringVector::AddString(col, reinterpret_cast<const char*>(opr.tmp_string.data()), length);
		}
	}
};

} // namespace duckdb::materializer
