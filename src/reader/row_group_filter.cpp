#include "reader/row_group_filter.hpp"
#include "duckdb/common/value_operations/value_operations.hpp"
#include "duckdb/planner/filter/constant_filter.hpp"
#include <algorithm>
#include <iostream>
#include <numeric>

namespace duckdb {

RowGroupFilter::RowGroupFilter()
    : statistics_ref(nullptr)
    , filters_ref(nullptr)
    , column_indexes_ref(nullptr)
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
