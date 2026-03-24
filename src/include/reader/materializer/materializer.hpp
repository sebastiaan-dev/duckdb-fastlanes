#pragma once

#include "reader/materializer/context.hpp"
#include "reader/materializer/fls_kernel.hpp"
#include <fls/common/alias.hpp>

namespace duckdb::materializer {

struct KernelInitVisitor {
	LogicalType&                       dest_type;
	ColumnCtxHandle&                   ctx;
	std::vector<FastLanesScanFilter*>* scan_filters;

	template <typename OpT>
	void operator()(fastlanes::sp<OpT> const& op) const {
		KernelTraits<OpT>::Prepare(ctx, dest_type, *op, scan_filters);
	}

	void operator()(const auto&) const {
		throw std::runtime_error("KernelInitVisitor: unsupported operation");
	}
};

template <Pass PASS>
struct KernelDecodeVisitor {
	fastlanes::DataType& src_type;
	Vector&              v;
	ColumnCtxHandle&     ctx;
	idx_t                vec_idx;

	template <typename OpT>
	void operator()(fastlanes::sp<OpT> const& op) const {
		KernelTraits<OpT>::template Decode<PASS>(ctx, v, vec_idx, *op, src_type);
	}

	void operator()(const auto&) const {
		throw std::runtime_error("KernelDecodeVisitor: unsupported operation");
	}
};

} // namespace duckdb::materializer
