#pragma once

#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/execution/adaptive_filter.hpp"
#include "duckdb/storage/table/column_segment.hpp"
#include <vector>

namespace duckdb {

struct FastLanesScanFilter;
class TableFilterSet;

class FilterExecutor {
public:
	static void Apply(DataChunk&                              chunk,
	                  const unique_ptr<AdaptiveFilter>&       adaptive_filter,
	                  const std::vector<FastLanesScanFilter>& scan_filters,
	                  const TableFilterSet*                   filters);
};

} // namespace duckdb
