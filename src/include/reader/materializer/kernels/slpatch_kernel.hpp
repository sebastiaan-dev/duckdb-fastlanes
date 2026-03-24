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
	static void
	Decode(ColumnCtxHandle&, Vector& col, idx_t, fastlanes::dec_slpatch_opr<PT>& op, fastlanes::DataType& src_type) {
		switch (src_type) {
		case fastlanes::DataType::INT8:
		case fastlanes::DataType::INT16:
		case fastlanes::DataType::INT32:
		case fastlanes::DataType::INT64: {
			using SPT              = std::conditional_t<std::is_unsigned_v<PT>, std::make_signed_t<PT>, PT>;
			const SPT* signed_view = reinterpret_cast<const SPT*>(op.data);
			detail::NumericHelper<PASS>::template CopyVector<SPT>(signed_view, col);
			break;
		}
		default:
			detail::NumericHelper<PASS>::template CopyVector<PT>(op.data, col);
		}
	}
};

} // namespace duckdb::materializer
