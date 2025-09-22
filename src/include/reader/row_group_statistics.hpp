#pragma once

#include "duckdb/common/types/value.hpp"
#include "fls/footer/column_descriptor.hpp"
#include <duckdb/common/multi_file/multi_file_data.hpp>
#include <fls/footer/table_descriptor_generated.h>
#include <string>
#include <unordered_map>
#include <vector>

namespace duckdb {
class ColumnDefinition;

class RowGroupStatistics {
public:
	using ColumnStatisticMap = std::unordered_map<std::string, Value>;

	RowGroupStatistics();

	void Initialize(const fastlanes::TableDescriptorT&       table_descriptor,
	                const vector<MultiFileColumnDefinition>& definitions);

	const Value* GetStatistic(idx_t rowgroup_idx, idx_t column_idx, const std::string& statistic_key) const;

	idx_t RowGroupCount() const {
		return rowgroup_statistics.size();
	}

private:
	Value ExtractColumnStatistic(const fastlanes::ColumnDescriptorT& column_descriptor,
	                             const LogicalType&                  logical_type,
	                             const std::string&                  statistic_key) const;

private:
	std::vector<std::vector<ColumnStatisticMap>> rowgroup_statistics;
};

} // namespace duckdb
