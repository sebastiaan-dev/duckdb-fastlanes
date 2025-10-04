#pragma once

#include "duckdb/common/column_index.hpp"
#include "duckdb/common/optional_idx.hpp"
#include "reader/row_group_statistics.hpp"
#include <atomic>
#include <mutex>
#include <vector>

namespace duckdb {

class TableFilterSet;

class RowGroupFilter {
public:
	RowGroupFilter();

	void Initialize(const RowGroupStatistics&       statistics,
	                const TableFilterSet*           filters,
	                const std::vector<ColumnIndex>& column_indexes);
	void SetRowIdInfo(const std::vector<idx_t>* row_group_offsets, idx_t total_rows, optional_idx row_id_column);

	const std::vector<idx_t>& EnsureRowGroups();

private:
	void BuildRowGroupFilterList();
	bool RowGroupMaySatisfyFilters(idx_t rowgroup_idx) const;

private:
	const RowGroupStatistics*       statistics_ref;
	const TableFilterSet*           filters_ref;
	const std::vector<ColumnIndex>* column_indexes_ref;
	const std::vector<idx_t>*       row_group_offsets_ref;
	idx_t                           total_rows_ref;
	optional_idx                    row_id_column_ref;
	std::vector<idx_t>              rowgroups_to_scan;
	std::atomic<bool>               ready;
	std::mutex                      lock;
};

} // namespace duckdb
