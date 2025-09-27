#pragma once

#include "fls/expression/frequency_operator.hpp"
#include "reader/materializer/fls_type_resolver.hpp"

namespace duckdb::materializer {

template <typename PT>
struct KernelTraits<fastlanes::dec_frequency_opr<PT>> {
	static void Prepare(ColumnCtxHandle&,
	                    LogicalType&,
	                    fastlanes::dec_frequency_opr<PT>&,
	                    const std::vector<FastLanesScanFilter*>*) {
	}

	template <Pass PASS>
	static void Decode(ColumnCtxHandle&, Vector& col, idx_t, fastlanes::dec_frequency_opr<PT>& op) {
		detail::NumericHelper<PASS>::template CopyVector<PT>(op.data, col);
	}
};

template <>
struct KernelTraits<fastlanes::dec_frequency_str_opr> {
	static void
	Prepare(ColumnCtxHandle&, LogicalType&, fastlanes::dec_frequency_str_opr&, const std::vector<FastLanesScanFilter*>*) {
	}

	template <Pass PASS>
	static void Decode(ColumnCtxHandle&, Vector& col, idx_t, fastlanes::dec_frequency_str_opr& op) {
		const auto target  = GetDataPtr<PASS, string_t>(col);
		size_t     entries = 0;

		const auto* exception_positions     = reinterpret_cast<fastlanes::vec_idx_t*>(op.exception_positions_seg.data);
		const auto* exception_values_bytes  = reinterpret_cast<uint8_t*>(op.exception_values_bytes_seg.data);
		const auto* exception_values_offset = reinterpret_cast<fastlanes::ofs_t*>(op.exception_values_offset_seg.data);
		const auto* n_exceptions_p          = reinterpret_cast<fastlanes::vec_idx_t*>(op.n_exceptions_seg.data);
		const auto  n_exceptions            = n_exceptions_p[0];

		fastlanes::vec_idx_t exception_idx {0};
		fastlanes::vec_idx_t exception_position {0};
		for (fastlanes::n_t idx {0}; idx < fastlanes::CFG::VEC_SZ; ++idx) {
			exception_position = exception_positions[exception_idx];

			if (exception_position == idx && exception_idx < n_exceptions) {
				fastlanes::ofs_t       cur_ofs;
				const fastlanes::ofs_t next_ofs = exception_values_offset[exception_idx];
				if (exception_idx == 0) {
					cur_ofs = 0;
				} else {
					cur_ofs = exception_values_offset[exception_idx - 1];
				}

				target[entries] = StringVector::AddString(
				    col, reinterpret_cast<const char*>(exception_values_bytes + cur_ofs), next_ofs - cur_ofs);
				entries++;

				exception_idx++;
			} else {
				target[entries] = StringVector::AddString(
				    col, reinterpret_cast<const char*>(op.frequent_val.p), op.frequent_val.length);
				entries++;
			}
		}
	}
};

} // namespace duckdb::materializer
