#include "reader/filter_executor.hpp"
#include "duckdb/storage/table/column_segment.hpp"
#include <duckdb/planner/table_filter_state.hpp>

namespace duckdb {

FastLanesScanFilter::FastLanesScanFilter(ClientContext& context, idx_t filter_idx_p, TableFilter& filter_p)
    : filter_idx(filter_idx_p)
    , filter(filter_p)
    , filter_state(TableFilterState::Initialize(context, filter_p)) {
}

FastLanesScanFilter::~FastLanesScanFilter() = default;

void FastLanesScanFilter::ResetForRowGroup() noexcept {
	// Currently a no-op, but should be used when introducing filters that have row group local caches.
}

void FilterExecutor::Apply(DataChunk&                              chunk,
                           const unique_ptr<AdaptiveFilter>&       adaptive_filter,
                           const std::vector<FastLanesScanFilter>& scan_filters,
                           const TableFilterSet*                   filters) {
	if (!filters || filters->filters.empty()) {
		return;
	}

	const idx_t scan_count = chunk.size();
	if (scan_count == 0) {
		return;
	}

	SelectionVector sel(STANDARD_VECTOR_SIZE);
	for (idx_t i = 0; i < scan_count; ++i) {
		sel.set_index(i, static_cast<sel_t>(i));
	}
	idx_t active_count = scan_count;

	const auto filter_state = adaptive_filter->BeginFilter();
	for (idx_t filter_idx = 0; filter_idx < scan_filters.size(); filter_idx++) {
		auto& scan_filter   = scan_filters[adaptive_filter->permutation[filter_idx]];
		auto& result_vector = chunk.data[scan_filter.filter_idx];

		UnifiedVectorFormat unified;
		result_vector.ToUnifiedFormat(scan_count, unified);

		idx_t filter_count = active_count;
		ColumnSegment::FilterSelection(
		    sel, result_vector, unified, scan_filter.filter, *scan_filter.filter_state, scan_count, filter_count);

		active_count = filter_count;
		if (active_count == 0) {
			break;
		}
	}
	adaptive_filter->EndFilter(filter_state);

	if (active_count != scan_count) {
		chunk.Slice(sel, active_count);
	}
}

} // namespace duckdb
