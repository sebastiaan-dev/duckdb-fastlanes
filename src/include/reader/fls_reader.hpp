#pragma once

#include "duckdb/common/multi_file/base_file_reader.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/planner/table_filter_state.hpp"
#include "fls/reader/rowgroup_reader.hpp"
#include "fls_multi_file_info.hpp"
#include "reader/filter_executor.hpp"
#include "reader/row_group_filter.hpp"
#include "reader/row_group_statistics.hpp"
#include "reader/table_metadata.hpp"
#include <atomic>
#include <string>
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
	explicit FastLanesReader(OpenFileInfo file_p, FastLanesFileReaderOptions& options);
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

	void AddVirtualColumn(column_t virtual_column_id) override;

	idx_t                                    GetNRowGroups() const;
	idx_t                                    GetNTuples(idx_t row_group_idx) const;
	idx_t                                    GetNVectors(idx_t row_group_idx) const;
	size_t                                   GetTotalTuples() const;
	size_t                                   GetTotalVectors() const;
	fastlanes::up<fastlanes::RowgroupReader> CreateRowGroupReader(idx_t rowgroup_idx);

private:
	void                      Initialize();
	const std::vector<idx_t>& GetRowGroupsToScan();
	void ApplyFilters(DataChunk& chunk, AdaptiveFilter& adaptive_filter, std::vector<FastLanesScanFilter>& filters);

private:
	size_t                    total_vectors;
	size_t                    total_tuples;
	unique_ptr<TableMetadata> table_metadata;
	atomic<idx_t>             vectors_read;
	RowGroupStatistics        rowgroup_statistics;
	RowGroupFilter            rowgroup_filter_catalog;
	//! Path of the directory containing both the FastLanes data file and metadata file.
	std::filesystem::path dir_path;
};
} // namespace duckdb
