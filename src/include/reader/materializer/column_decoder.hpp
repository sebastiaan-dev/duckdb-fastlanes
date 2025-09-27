#pragma once

#include "fls_type_resolver.hpp"
#include "materializer.hpp"

namespace duckdb::materializer {

class ColumnDecoder {
public:
	explicit ColumnDecoder() {};

public:
	template <class FinalOpVariant>
	void Init(const FinalOpVariant& final_op, LogicalType& type, std::vector<FastLanesScanFilter*>* scan_filters) {
		KernelInitVisitor visitor {type, ctx, scan_filters};
		std::visit(visitor, final_op);
	}

	template <Pass PASS, class FinalOpVariant>
	void Decode(const FinalOpVariant& final_op, Vector& v, idx_t vec_idx) {
		KernelDecodeVisitor<PASS> visitor {v, ctx, vec_idx};
		std::visit(visitor, final_op);
	};

	void Reset() noexcept {
		ctx.Reset();
	}

private:
	ColumnCtxHandle ctx;
};
} // namespace duckdb::materializer