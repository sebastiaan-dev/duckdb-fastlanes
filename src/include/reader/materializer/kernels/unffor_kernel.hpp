#pragma once

#include "reader/materializer/fls_type_resolver.hpp"

namespace duckdb::materializer {

template <typename PT>
struct KernelTraits<fastlanes::dec_unffor_opr<PT>> {
	static void
	Prepare(ColumnCtxHandle&, LogicalType&, fastlanes::dec_unffor_opr<PT>&, const std::vector<FastLanesScanFilter*>*) {
	}

	template <Pass PASS>
	static void Decode(ColumnCtxHandle&, Vector& col, idx_t, fastlanes::dec_unffor_opr<PT>& op) {
		detail::NumericHelper<PASS>::template CopyVector<PT>(op.Data(), col);
	}
};

} // namespace duckdb::materializer
