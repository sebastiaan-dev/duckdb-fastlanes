#pragma once

#include "duckdb/execution/adaptive_filter.hpp"
#include "duckdb/storage/table/column_segment.hpp"
#include <vector>

namespace duckdb {
class TableFilterSet;

struct FastLanesScanFilter {
	FastLanesScanFilter(ClientContext& context, idx_t filter_idx, TableFilter& filter);
	~FastLanesScanFilter();
	FastLanesScanFilter(FastLanesScanFilter&&) = default;
	void ResetForRowGroup() noexcept;

	idx_t                        filter_idx;
	TableFilter&                 filter;
	unique_ptr<TableFilterState> filter_state;
};

class FilterExecutor {
public:
	static void Apply(DataChunk&                              chunk,
	                  const unique_ptr<AdaptiveFilter>&       adaptive_filter,
	                  const std::vector<FastLanesScanFilter>& scan_filters,
	                  const TableFilterSet*                   filters);
};

} // namespace duckdb
