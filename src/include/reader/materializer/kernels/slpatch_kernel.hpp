#pragma once

#include "reader/materializer/fls_type_resolver.hpp"
#include <fls/expression/slpatch_operator.hpp>

namespace duckdb::materializer {

template <typename PT>
struct KernelTraits<fastlanes::dec_slpatch_opr<PT>> {
	static void
	Prepare(ColumnCtxHandle&, LogicalType&, fastlanes::dec_slpatch_opr<PT>&, const std::vector<FastLanesScanFilter*>*) {
	}

	template <Pass PASS>
	static void Decode(ColumnCtxHandle&, Vector& col, idx_t, fastlanes::dec_slpatch_opr<PT>& op) {
		detail::NumericHelper<PASS>::template CopyVector<PT>(op.data, col);
	}
};

} // namespace duckdb::materializer
