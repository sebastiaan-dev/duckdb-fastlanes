#pragma once

#include "reader/materializer/fls_type_resolver.hpp"
#include <fls/expression/alp_expression.hpp>

namespace duckdb::materializer {

template <typename OpT>
struct KernelTraits;

template <typename PT>
struct KernelTraits<fastlanes::dec_alp_opr<PT>> {
	static void
	Prepare(ColumnCtxHandle&, LogicalType&, fastlanes::dec_alp_opr<PT>&, const std::vector<FastLanesScanFilter*>*) {
	}

	template <Pass PASS>
	static void Decode(ColumnCtxHandle&, Vector& col, idx_t, fastlanes::dec_alp_opr<PT>& op) {
		detail::NumericHelper<PASS>::template CopyVector<PT>(op.decoded_arr, col);
	}
};

} // namespace duckdb::materializer
