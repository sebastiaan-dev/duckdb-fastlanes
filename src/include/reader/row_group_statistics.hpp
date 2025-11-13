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

enum class StatKey : uint8_t { None = 0, Min = 1, Max = 2 };

struct ColumnStats {
	Value min;
	Value max;
};

class RowGroupStatistics {
public:
	using ColumnStatisticMap = std::unordered_map<std::string, Value>;

	RowGroupStatistics();

	void Initialize(const fastlanes::TableDescriptor&        table_descriptor,
	                const vector<MultiFileColumnDefinition>& definitions);

	const Value* GetStatistic(idx_t rowgroup_idx, idx_t column_idx, StatKey key) const;

	const ColumnStats* GetStats(const idx_t rowgroup_idx, const idx_t column_idx) const {
		if (rowgroup_idx >= m_stats.size()) {
			return nullptr;
		}
		const auto& cols = m_stats[rowgroup_idx];
		if (column_idx >= cols.size()) {
			return nullptr;
		}
		return &cols[column_idx];
	}

	idx_t RowGroupCount() const {
		return m_stats.size();
	}

private:
	static Value ExtractColumnStatistic(const fastlanes::ColumnDescriptor& column_descriptor,
	                                    const LogicalType&                 logical_type,
	                                    StatKey                            statistic_key);

private:
	std::vector<std::vector<ColumnStats>> m_stats;
};

} // namespace duckdb
