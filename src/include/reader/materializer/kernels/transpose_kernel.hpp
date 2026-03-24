#pragma once

#include "fls/expression/transpose_operator.hpp"
#include "reader/materializer/fls_type_resolver.hpp"

namespace duckdb::materializer {

template <typename PT>
struct KernelTraits<fastlanes::dec_transpose_opr<PT>> {
	static void Prepare(ColumnCtxHandle&,
	                    LogicalType&,
	                    fastlanes::dec_transpose_opr<PT>&,
	                    const std::vector<FastLanesScanFilter*>*) {
	}

	template <Pass PASS>
	static void
	Decode(ColumnCtxHandle&, Vector& col, idx_t, fastlanes::dec_transpose_opr<PT>& op, fastlanes::DataType&) {
		detail::NumericHelper<PASS>::template UntransposeBlock<PT>(op.transposed_data, col);
	}
};

} // namespace duckdb::materializer
