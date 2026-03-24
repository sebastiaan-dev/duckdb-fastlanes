#pragma once

#include "duckdb/common/multi_file/base_file_reader.hpp"
#include "duckdb/common/optional_idx.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/planner/table_filter_state.hpp"
#include "fls/reader/rowgroup_reader.hpp"
#include "fls_multi_file_info.hpp"
#include "reader/filter_executor.hpp"
#include "reader/row_group_filter.hpp"
#include "reader/row_group_statistics.hpp"
#include "reader/table_metadata.hpp"
#include <atomic>
#include <memory>
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
	explicit FastLanesReader(OpenFileInfo file_p, const FastLanesFileReaderOptions& options);
	~FastLanesReader() override;

	std::string GetReaderType() const override {
		return "FastLanes";
	}

	bool                       TryInitializeScan(ClientContext&            context,
	                                             GlobalTableFunctionState& gstate,
	                                             LocalTableFunctionState&  lstate) override;
	void                       Scan(ClientContext&            context,
	                                GlobalTableFunctionState& global_state_p,
	                                LocalTableFunctionState&  local_state_p,
	                                DataChunk&                chunk) override;
	void                       PrepareReader(ClientContext& context, GlobalTableFunctionState&) override;
	void                       FinishFile(ClientContext& context, GlobalTableFunctionState& global_state_p) override;
	double                     GetProgressInFile(ClientContext& context) override;
	unique_ptr<BaseStatistics> GetStatistics(ClientContext& context, const string& name) override;
	void                       GetPartitionStats(vector<PartitionStatistics>& result) const;
	shared_ptr<BaseUnionData>  GetUnionData(idx_t file_idx) override;
	const FastLanesFileReaderOptions& GetOptions() const;

	void AddVirtualColumn(column_t virtual_column_id) override;

	idx_t                                    GetNRowGroups() const;
	idx_t                                    GetNTuples(idx_t row_group_idx) const;
	idx_t                                    GetNVectors(idx_t row_group_idx) const;
	size_t                                   GetTotalTuples() const;
	size_t                                   GetTotalVectors() const;
	fastlanes::up<fastlanes::RowgroupReader> CreateRowGroupReader(idx_t                        rowgroup_idx,
	                                                              const std::vector<uint32_t>& projected_ids);

private:
	void Initialize();
	struct ProjectionExpansion {
		std::vector<uint32_t>               expanded_ids;
		std::unordered_map<uint32_t, idx_t> index_map;
	};
	struct ScanBatch {
		idx_t start_tuple = 0;
		idx_t count = 0;
		idx_t input_vector_count = 0;

		bool IsValid() const {
			return input_vector_count > 0;
		}
	};
	static bool         IsExternalDictOperatorToken(fastlanes::OperatorToken token);
	static bool         HasMccEncoding(const fastlanes::RowgroupDescriptor& rowgroup_descriptor,
	                                   const std::vector<uint32_t>&         column_ids);
	static optional_idx GetMccDependency(const fastlanes::ColumnDescriptor& column_descriptor, idx_t column_count);
	static bool         HasPhysicalProjection(const FastLanesReadLocalState& local_state);
	bool                TryAssignNextRowGroup(FastLanesReadGlobalState& global_state, FastLanesReadLocalState& local_state);
	void                InitializeScanFilters(ClientContext& ctx, FastLanesReadLocalState& local_state);
	void                InitializePhysicalProjection(FastLanesReadLocalState& local_state);
	void                InitializeColumnDecoders(FastLanesReadLocalState& local_state);
	ScanBatch           GetScanBatch(const FastLanesReadLocalState& local_state) const;
	void                DecodePhysicalColumns(FastLanesReadLocalState& local_state,
	                                          DataChunk&               chunk,
	                                          idx_t                    start_vector,
	                                          idx_t                    input_vector_count);
	void                PopulateVirtualColumns(const FastLanesReadLocalState& local_state,
	                                           DataChunk&               chunk,
	                                           idx_t                    row_start,
	                                           idx_t                    count);
	ProjectionExpansion ExpandProjectedColumns(idx_t rowgroup_idx);
	const std::vector<idx_t>& GetRowGroupsToScan();
	void EnsureFileRowNumberColumn();
	bool IsFileRowNumberColumn(column_t column_id) const;

private:
	size_t                    total_vectors;
	size_t                    total_tuples;
	unique_ptr<TableMetadata> table_metadata;
	atomic<idx_t>             vectors_read;
	RowGroupStatistics        rowgroup_statistics;
	RowGroupFilter            rowgroup_filter_catalog;
	//! Path of the directory containing both the FastLanes data file and metadata file.
	std::filesystem::path      dir_path;
	optional_idx               file_row_number_local_idx;
	std::vector<idx_t>         row_group_offsets;
	FastLanesFileReaderOptions reader_options;
};
} // namespace duckdb
