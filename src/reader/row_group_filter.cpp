#include "reader/row_group_filter.hpp"
#include "duckdb/common/value_operations/value_operations.hpp"
#include "duckdb/planner/filter/constant_filter.hpp"
#include "duckdb/storage/table/row_group.hpp"
#include <algorithm>
#include <numeric>

namespace duckdb {

RowGroupFilter::RowGroupFilter()
    : statistics_ref(nullptr)
    , filters_ref(nullptr)
    , column_indexes_ref(nullptr)
    , row_group_offsets_ref(nullptr)
    , total_rows_ref(0)
    , row_id_column_ref()
    , ready(false) {
}

void RowGroupFilter::Initialize(const RowGroupStatistics&       statistics,
                                const TableFilterSet*           filters,
                                const std::vector<ColumnIndex>& column_indexes) {
	std::lock_guard<std::mutex> guard(lock);
	const bool                  changed =
	    statistics_ref != &statistics || filters_ref != filters || column_indexes_ref != &column_indexes;

	statistics_ref     = &statistics;
	filters_ref        = filters;
	column_indexes_ref = &column_indexes;

	if (changed) {
		ready.store(false, std::memory_order_relaxed);
		rowgroups_to_scan.clear();
	}
}

void RowGroupFilter::SetRowIdInfo(const std::vector<idx_t>* row_group_offsets,
                                  idx_t                     total_rows,
                                  optional_idx              row_id_column) {
	std::lock_guard<std::mutex> guard(lock);
	bool                        changed = row_group_offsets_ref != row_group_offsets || total_rows_ref != total_rows;
	const bool                  current_valid = row_id_column_ref.IsValid();
	const bool                  new_valid     = row_id_column.IsValid();
	if (current_valid != new_valid) {
		changed = true;
	} else if (current_valid && new_valid && row_id_column_ref.GetIndex() != row_id_column.GetIndex()) {
		changed = true;
	}
	row_group_offsets_ref = row_group_offsets;
	total_rows_ref        = total_rows;
	if (new_valid) {
		row_id_column_ref = row_id_column;
	} else {
		row_id_column_ref.SetInvalid();
	}
	if (changed) {
		ready.store(false, std::memory_order_relaxed);
		rowgroups_to_scan.clear();
	}
}

const std::vector<idx_t>& RowGroupFilter::EnsureRowGroups() {
	if (ready.load(std::memory_order_acquire)) {
		return rowgroups_to_scan;
	}

	std::lock_guard<std::mutex> guard(lock);
	if (!ready.load(std::memory_order_acquire)) {
		BuildRowGroupFilterList();
		ready.store(true, std::memory_order_release);
	}
	return rowgroups_to_scan;
}

void RowGroupFilter::BuildRowGroupFilterList() {
	rowgroups_to_scan.clear();
	if (!statistics_ref) {
		return;
	}

	const idx_t total_rowgroups = statistics_ref->RowGroupCount();
	if (!filters_ref || filters_ref->filters.empty()) {
		rowgroups_to_scan.resize(total_rowgroups);
		std::iota(rowgroups_to_scan.begin(), rowgroups_to_scan.end(), idx_t(0));
		return;
	}

	rowgroups_to_scan.reserve(total_rowgroups);
	for (idx_t rowgroup_idx = 0; rowgroup_idx < total_rowgroups; ++rowgroup_idx) {
		if (RowGroupMaySatisfyFilters(rowgroup_idx)) {
			rowgroups_to_scan.push_back(rowgroup_idx);
		}
	}
}

bool RowGroupFilter::RowGroupMaySatisfyFilters(idx_t rowgroup_idx) const {
	if (!filters_ref || filters_ref->filters.empty()) {
		return true;
	}
	if (!statistics_ref || !column_indexes_ref) {
		return true;
	}

	for (auto& entry : filters_ref->filters) {
		const idx_t local_column_id = entry.first;
		auto&       filter          = *entry.second;

		if (filter.filter_type != TableFilterType::CONSTANT_COMPARISON) {
			continue;
		}

		auto&   constant_filter = filter.Cast<ConstantFilter>();
		auto    comparison_type = constant_filter.comparison_type;
		StatKey statistic_key   = StatKey::None;
		switch (comparison_type) {
		case ExpressionType::COMPARE_GREATERTHAN:
		case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
			statistic_key = StatKey::Max;
			break;
		case ExpressionType::COMPARE_LESSTHAN:
		case ExpressionType::COMPARE_LESSTHANOREQUALTO:
			statistic_key = StatKey::Min;
			break;
		default:
			break;
		}
		if (statistic_key == StatKey::None) {
			continue;
		}
		if (local_column_id >= column_indexes_ref->size()) {
			continue;
		}

		const idx_t primary_index = (*column_indexes_ref)[local_column_id].GetPrimaryIndex();

		if (row_id_column_ref.IsValid() && primary_index == row_id_column_ref.GetIndex() && row_group_offsets_ref &&
		    rowgroup_idx < row_group_offsets_ref->size()) {
			idx_t row_group_begin = (*row_group_offsets_ref)[rowgroup_idx];
			idx_t row_group_end   = row_group_begin;
			if (rowgroup_idx + 1 < row_group_offsets_ref->size()) {
				row_group_end = (*row_group_offsets_ref)[rowgroup_idx + 1];
			} else if (total_rows_ref >= row_group_begin) {
				row_group_end = total_rows_ref;
			}
			if (row_group_end < row_group_begin) {
				row_group_end = row_group_begin;
			}
			auto prune_result = RowGroup::CheckRowIdFilter(filter, row_group_begin, row_group_end);
			if (prune_result == FilterPropagateResult::FILTER_ALWAYS_FALSE) {
				return false;
			}
			continue;
		}

		const auto* statistic_ptr = statistics_ref->GetStatistic(rowgroup_idx, primary_index, statistic_key);
		if (!statistic_ptr) {
			continue;
		}

		const auto& statistic_value = *statistic_ptr;
		const auto& constant        = constant_filter.constant;
		if (statistic_value.IsNull() || constant.IsNull()) {
			continue;
		}

		Value stat_casted;
		Value constant_casted;
		if (statistic_value.DefaultTryCastAs(constant.type(), stat_casted, nullptr)) {
			constant_casted = constant;
		} else if (constant.DefaultTryCastAs(statistic_value.type(), constant_casted, nullptr)) {
			stat_casted = statistic_value;
		} else {
			continue;
		}

		bool skip_rowgroup = false;
		switch (comparison_type) {
		case ExpressionType::COMPARE_GREATERTHAN:
			skip_rowgroup = ValueOperations::LessThanEquals(stat_casted, constant_casted);
			break;
		case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
			skip_rowgroup = ValueOperations::LessThan(stat_casted, constant_casted);
			break;
		case ExpressionType::COMPARE_LESSTHAN:
			skip_rowgroup = ValueOperations::GreaterThanEquals(stat_casted, constant_casted);
			break;
		case ExpressionType::COMPARE_LESSTHANOREQUALTO:
			skip_rowgroup = ValueOperations::GreaterThan(stat_casted, constant_casted);
			break;
		default:
			break;
		}

		if (skip_rowgroup) {
			return false;
		}
	}
	return true;
}

} // namespace duckdb
