#pragma once

#include "reader/materializer/context.hpp"
#include <fls/expression/fsst12_dict_operator.hpp>
#include <fls/expression/fsst12_expression.hpp>

namespace duckdb::materializer {

template <typename INDEX_PT>
struct KernelTraits<fastlanes::dec_fsst12_dict_opr<INDEX_PT>> {
	static void Prepare(ColumnCtxHandle&,
	                    LogicalType&,
	                    fastlanes::dec_fsst12_dict_opr<INDEX_PT>&,
	                    const std::vector<FastLanesScanFilter*>*) {
	}

	template <Pass PASS>
	static void Decode(ColumnCtxHandle&, Vector& col, idx_t, fastlanes::dec_fsst12_dict_opr<INDEX_PT>& opr) {
		const auto target_ptr  = GetDataPtr<PASS, string_t>(col);
		auto*      in_byte_arr = reinterpret_cast<uint8_t*>(opr.fsst12_bytes_segment_view.data);

		for (fastlanes::n_t idx {0}; idx < fastlanes::CFG::VEC_SZ; ++idx) {
			const auto index = opr.Index()[idx];

			fastlanes::ofs_t offset = 0;
			fastlanes::len_t length = 0;

			if (index == 0) {
				offset = 0;
				length = opr.Offsets()[index];
			} else {
				offset                 = opr.Offsets()[index - 1];
				const auto offset_next = opr.Offsets()[index];
				length                 = offset_next - offset;
			}

			const auto decoded_size =
			    static_cast<fastlanes::ofs_t>(fsst12_decompress(&opr.fsst12_decoder,
			                                                    length,
			                                                    in_byte_arr + offset,
			                                                    fastlanes::CFG::String::max_bytes_per_string,
			                                                    opr.tmp_string.data()));

			target_ptr[idx] =
			    StringVector::AddString(col, reinterpret_cast<const char*>(opr.tmp_string.data()), decoded_size);
		}
	}
};

} // namespace duckdb::materializer
