#pragma once

#include "reader/materializer/fls_type_resolver.hpp"
#include "fls/expression/validitymask_operator.hpp"

namespace duckdb::materializer {

// FastLanes stores BOOLEAN values as a packed bitmap (not as SQL validity).
template <>
struct KernelTraits<fastlanes::dec_validitymask_opr> {
	static void Prepare(ColumnCtxHandle&, LogicalType&, fastlanes::dec_validitymask_opr&,
	                    const std::vector<FastLanesScanFilter*>*) {
	}

	template <Pass PASS>
	static void Decode(ColumnCtxHandle&, Vector& col, idx_t, fastlanes::dec_validitymask_opr& op,
	                   fastlanes::DataType&) {
		auto* out = GetDataPtr<PASS, bool>(col);
		for (idx_t word_idx = 0; word_idx < fastlanes::ValidityMask::WORD_COUNT; ++word_idx) {
			const auto word = detail::LoadValue<uint64_t>(op.Data() + word_idx * sizeof(uint64_t));
			for (idx_t bit = 0; bit < 64; ++bit) {
				out[word_idx * 64 + bit] = ((word >> bit) & 1) != 0;
			}
		}
	}
};

} // namespace duckdb::materializer
