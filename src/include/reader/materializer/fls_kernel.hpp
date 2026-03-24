#pragma once

#include "reader/filter_executor.hpp"
#include "reader/materializer/context.hpp"
#include "reader/materializer/fls_type_resolver.hpp"
#include "reader/materializer/kernels/alp_kernel.hpp"
#include "reader/materializer/kernels/alp_rd_kernel.hpp"
#include "reader/materializer/kernels/constant_kernel.hpp"
#include "reader/materializer/kernels/cross_rle_kernel.hpp"
#include "reader/materializer/kernels/dictionary_kernel.hpp"
#include "reader/materializer/kernels/frequency_kernel.hpp"
#include "reader/materializer/kernels/fsst12_dict_kernel.hpp"
#include "reader/materializer/kernels/fsst12_kernel.hpp"
#include "reader/materializer/kernels/fsst_dict_kernel.hpp"
#include "reader/materializer/kernels/fsst_kernel.hpp"
#include "reader/materializer/kernels/rle_map_kernel.hpp"
#include "reader/materializer/kernels/slpatch_kernel.hpp"
#include "reader/materializer/kernels/transpose_kernel.hpp"
#include "reader/materializer/kernels/uncompressed_kernel.hpp"
#include "reader/materializer/kernels/unffor_kernel.hpp"

namespace duckdb::materializer {

template <typename OpT>
struct KernelTraits {
	static void Prepare(ColumnCtxHandle&, LogicalType&, OpT&, const std::vector<FastLanesScanFilter*>*) {
		throw std::runtime_error("Not implemented");
	}

	template <Pass PASS>
	static void Decode(ColumnCtxHandle&, Vector&, idx_t, OpT&, fastlanes::DataType&) {
		throw std::runtime_error("Not implemented");
	}
};

} // namespace duckdb::materializer
