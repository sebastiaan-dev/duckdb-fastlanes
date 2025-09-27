#pragma once

#include "reader/materializer/context.hpp"
#include "reader/materializer/fls_kernel.hpp"
#include <fls/common/alias.hpp>

namespace duckdb::materializer {

struct KernelInitVisitor {
	LogicalType&                       type;
	ColumnCtxHandle&                   ctx;
	std::vector<FastLanesScanFilter*>* scan_filters;

	template <typename OpT>
	void operator()(fastlanes::sp<OpT> const& op) const {
		KernelTraits<OpT>::Prepare(ctx, type, *op, scan_filters);
	}

	void operator()(const auto&) const {
		throw std::runtime_error("KernelInitVisitor: unsupported operation");
	}
};

template <Pass PASS>
struct KernelDecodeVisitor {
	Vector&          v;
	ColumnCtxHandle& ctx;
	idx_t            vec_idx;

	template <typename OpT>
	void operator()(fastlanes::sp<OpT> const& op) const {
		KernelTraits<OpT>::template Decode<PASS>(ctx, v, vec_idx, *op);
	}

	void operator()(const auto&) const {
		throw std::runtime_error("KernelDecodeVisitor: unsupported operation");
	}
};

} // namespace duckdb::materializer
