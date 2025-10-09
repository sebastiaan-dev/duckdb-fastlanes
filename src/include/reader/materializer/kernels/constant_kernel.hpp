#pragma once

#include "reader/materializer/context.hpp"
#include "reader/materializer/fls_type_resolver.hpp"
#include <duckdb/common/types/string_type.hpp>
#include <duckdb/common/types/vector.hpp>
#include <fls/expression/physical_expression.hpp>

namespace duckdb::materializer {

template <typename PT>
struct KernelTraits<fastlanes::dec_constant_opr<PT>> {
	static void Prepare(ColumnCtxHandle&,
	                    LogicalType&,
	                    const fastlanes::dec_constant_opr<PT>&,
	                    const std::vector<FastLanesScanFilter*>*) {
		// std::cout << "Prepare fastlanes::dec_constant_opr" << std::endl;
	}

	template <Pass PASS>
	static void
	Decode(ColumnCtxHandle&, Vector& col, idx_t, const fastlanes::dec_constant_opr<PT>& op, fastlanes::DataType&) {
		if constexpr (PASS == Pass::First) {
			detail::NumericHelper<Pass::First>::template AssignValue<PT>(op.value, col, 0);
			col.SetVectorType(VectorType::CONSTANT_VECTOR);
			ConstantVector::SetNull(col, false);
		}
	}
};

template <>
struct KernelTraits<fastlanes::dec_constant_str_opr> {
	static void Prepare(ColumnCtxHandle&,
	                    LogicalType&,
	                    fastlanes::dec_constant_str_opr&,
	                    const std::vector<FastLanesScanFilter*>*) {
	}

	template <Pass PASS>
	static void
	Decode(ColumnCtxHandle&, Vector& col, idx_t, fastlanes::dec_constant_str_opr& op, fastlanes::DataType&) {
		if constexpr (PASS == Pass::First) {
			col.SetVectorType(VectorType::CONSTANT_VECTOR);
			ConstantVector::SetNull(col, false);
			const auto len  = static_cast<idx_t>(op.bytes.size());
			auto*      dest = FlatVector::GetData<string_t>(col);
			dest[0]         = StringVector::AddString(col, reinterpret_cast<const char*>(op.bytes.data()), len);
		}
	}
};

} // namespace duckdb::materializer
