#pragma once

#include "reader/materializer/fls_type_resolver.hpp"

namespace duckdb::materializer {
using fastlanes::dec_fls_str_uncompressed_opr;
using fastlanes::dec_uncompressed_opr;

template <typename PT>
struct KernelTraits<dec_uncompressed_opr<PT>> {
	static void
	Prepare(ColumnCtxHandle&, LogicalType&, dec_uncompressed_opr<PT>&, const std::vector<FastLanesScanFilter*>*) {
	}

	template <Pass PASS>
	static void Decode(ColumnCtxHandle&, Vector& col, idx_t, dec_uncompressed_opr<PT>& op, fastlanes::DataType&) {
		detail::NumericHelper<PASS>::template CopyVector<PT>(op.Data(), col);
	}
};

template <>
struct KernelTraits<dec_fls_str_uncompressed_opr> {
	static void
	Prepare(ColumnCtxHandle&, LogicalType&, dec_fls_str_uncompressed_opr&, const std::vector<FastLanesScanFilter*>*) {
	}

	template <Pass PASS>
	static void Decode(ColumnCtxHandle&, Vector& col, idx_t, dec_fls_str_uncompressed_opr& op, fastlanes::DataType&) {
		const auto target = GetDataPtr<PASS, string_t>(col);
		uint64_t   offset = 0;

		for (size_t idx {0}; idx < fastlanes::CFG::VEC_SZ; ++idx) {
			const auto length = op.Length()[idx];

			target[idx] = StringVector::AddString(col, reinterpret_cast<const char*>(op.Data() + offset), length);
			offset += length;
		}
	}
};

} // namespace duckdb::materializer
