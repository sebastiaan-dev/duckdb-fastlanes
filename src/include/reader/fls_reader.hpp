#pragma once

#include "duckdb/common/multi_file/base_file_reader.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/planner/table_filter_state.hpp"
#include "fls/footer/column_descriptor.hpp"
#include "fls/reader/rowgroup_reader.hpp"
#include <atomic>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

namespace duckdb {
struct FastLanesScanFilter;
class AdaptiveFilter;
class ClientContext;
class ColumnDecoder;
/**
 * Provides an abstraction over the FastLanes file format which allows for implicit multithreaded support with DuckDB.
 */
class FastLanesReader final : public BaseFileReader {
public:
	explicit FastLanesReader(OpenFileInfo file_p);
	~FastLanesReader() override;

	std::string GetReaderType() const override {
		return "FastLanes";
	}

	bool                       TryInitializeScan(ClientContext&            context,
	                                             GlobalTableFunctionState& gstate,
	                                             LocalTableFunctionState&  lstate) override;
	void                       Scan(ClientContext&            context,
	                                GlobalTableFunctionState& global_state,
	                                LocalTableFunctionState&  local_state,
	                                DataChunk&                chunk) override;
	void                       PrepareReader(ClientContext& context, GlobalTableFunctionState&) override;
	void                       FinishFile(ClientContext& context, GlobalTableFunctionState& global_state_p) override;
	double                     GetProgressInFile(ClientContext& context) override;
	unique_ptr<BaseStatistics> GetStatistics(ClientContext& context, const string& name) override;
	shared_ptr<BaseUnionData>  GetUnionData(idx_t file_idx) override;

	idx_t                                    GetNRowGroups() const;
	idx_t                                    GetNTuples(idx_t row_group_idx) const;
	idx_t                                    GetNVectors(idx_t row_group_idx) const;
	idx_t                                    GetTotalTuples() const;
	fastlanes::up<fastlanes::RowgroupReader> CreateRowGroupReader(idx_t rowgroup_idx);

private:
	void         InitializeRowGroupStats();
	Value        ExtractColumnStatistic(const fastlanes::ColumnDescriptorT& column_descriptor,
	                                    const LogicalType&                  logical_type,
	                                    const std::string&                  statistic_key) const;
	const Value* GetRowGroupStatistic(idx_t rowgroup_idx, idx_t column_idx, const std::string& statistic_key) const;
	bool         RowGroupMaySatisfyFilters(idx_t rowgroup_idx);
	void         EnsureRowGroupFilterState();
	void         BuildRowGroupFilterList();
	void ApplyFilters(DataChunk& chunk, AdaptiveFilter& adaptive_filter, std::vector<FastLanesScanFilter>& filters);

private:
	atomic<idx_t> vectors_read;
	using ColumnStatisticMap = std::unordered_map<std::string, Value>;
	std::vector<std::vector<ColumnStatisticMap>> rowgroup_statistics;
	std::vector<idx_t>                           rowgroups_to_scan;
	mutable std::mutex                           rowgroup_filter_lock;
	std::atomic<bool>                            rowgroup_filters_ready {false};
	//! Path of the directory containing both the FastLanes data file and metadata file.
	std::filesystem::path              dir_path;
	fastlanes::Connection              conn;
	unique_ptr<fastlanes::TableReader> table_reader;
};
} // namespace duckdb
