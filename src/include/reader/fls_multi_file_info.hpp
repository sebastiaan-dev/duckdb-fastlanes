#pragma once

#include "duckdb/common/constants.hpp"
#include "duckdb/common/multi_file/multi_file_function.hpp"
#include "fls/reader/rowgroup_reader.hpp"
#include <duckdb/execution/adaptive_filter.hpp>

namespace duckdb {
class ColumnDecoder;

struct FastLanesReadBindData final : TableFunctionData {
	//! Number of rows in the first file, used for estimating the total cardinality of the to-be-read file(s).
	idx_t initial_file_cardinality;
	//! Number of vectors in the first file, used to determine the number of threads.
	idx_t initial_file_n_rowgroups;
};

struct FastLanesScanFilter {
	FastLanesScanFilter(ClientContext& context, idx_t filter_idx, TableFilter& filter);
	~FastLanesScanFilter();
	FastLanesScanFilter(FastLanesScanFilter&&) = default;

	idx_t                        filter_idx;
	TableFilter&                 filter;
	unique_ptr<TableFilterState> filter_state;
};

struct FastLanesReadLocalState final : LocalTableFunctionState {
	idx_t cur_vector;
	//! Rowgroup which is currently being processed.
	idx_t                                    cur_rowgroup;
	fastlanes::up<fastlanes::RowgroupReader> row_group_reader;
	std::vector<unique_ptr<ColumnDecoder>>   column_decoders;
	bool                                     is_initialized = false;
	std::vector<FastLanesScanFilter>         scan_filters;
	unique_ptr<AdaptiveFilter>               adaptive_filter;
};

struct FastLanesReadGlobalState final : GlobalTableFunctionState {
	//! Index of the row group within the current file that is staged for scanning.
	idx_t cur_rowgroup;
};

/**
 * Define all the required functions from the MultiFileFunction template class.
 */
struct FastLanesMultiFileInfo : public MultiFileReaderInterface {
	static unique_ptr<MultiFileReaderInterface>
	InitializeInterface(ClientContext& context, MultiFileReader& reader, MultiFileList& file_list);

	bool ParseCopyOption(ClientContext&         context,
	                     const string&          key,
	                     const vector<Value>&   values,
	                     BaseFileReaderOptions& options,
	                     vector<string>&        expected_names,
	                     vector<LogicalType>&   expected_types) override;

	unique_ptr<BaseFileReaderOptions> InitializeOptions(ClientContext&                  context,
	                                                    optional_ptr<TableFunctionInfo> info) override;
	bool                              ParseOption(ClientContext&         context,
	                                              const string&          key,
	                                              const Value&           val,
	                                              MultiFileOptions&      file_options,
	                                              BaseFileReaderOptions& options) override;
	/*!
	 * Save user-provided options (currently none) and allocate FastLanesReadBindData (TableFunctionData) object.
	 */
	unique_ptr<TableFunctionData> InitializeBindData(MultiFileBindData&                multi_file_data,
	                                                 unique_ptr<BaseFileReaderOptions> options) override;
	void                          BindReader(ClientContext&       context,
	                                         vector<LogicalType>& return_types,
	                                         vector<string>&      names,
	                                         MultiFileBindData&   bind_data_p) override;

	void                                 FinalizeBindData(MultiFileBindData& multi_file_data) override;
	void                                 GetBindInfo(const TableFunctionData& bind_data_p, BindInfo& info) override;
	unique_ptr<GlobalTableFunctionState> InitializeGlobalState(ClientContext&        context,
	                                                           MultiFileBindData&    bind_data_p,
	                                                           MultiFileGlobalState& global_state_p) override;
	optional_idx                         MaxThreads(const MultiFileBindData&    bind_data_p,
	                                                const MultiFileGlobalState& global_state_p,
	                                                FileExpandResult            expand_result) override;
	unique_ptr<LocalTableFunctionState>  InitializeLocalState(ExecutionContext&,
	                                                          GlobalTableFunctionState& global_state_p) override;
	shared_ptr<BaseFileReader>           CreateReader(ClientContext&            context,
	                                                  GlobalTableFunctionState& global_state_p,
	                                                  BaseUnionData&            union_data,
	                                                  const MultiFileBindData&  bind_data_p) override;
	shared_ptr<BaseFileReader>           CreateReader(ClientContext&            context,
	                                                  GlobalTableFunctionState& global_state_p,
	                                                  const OpenFileInfo&       file,
	                                                  idx_t                     file_idx,
	                                                  const MultiFileBindData&  bind_data_p) override;
	shared_ptr<BaseFileReader>           CreateReader(ClientContext&          context,
	                                                  const OpenFileInfo&     file,
	                                                  BaseFileReaderOptions&  options,
	                                                  const MultiFileOptions& file_options) override;
	void                                 FinishReading(ClientContext&            context,
	                                                   GlobalTableFunctionState& global_state_p,
	                                                   LocalTableFunctionState&  local_state_p) override;
	/*!
	 * Estimate the cardinality of the to-be-read files, the estimate is based on the first file.
	 */
	unique_ptr<NodeStatistics> GetCardinality(const MultiFileBindData& bind_data_p, idx_t file_count) override;
	void
	GetVirtualColumns(ClientContext& context, MultiFileBindData& bind_data_p, virtual_column_map_t& result) override;
};

} // namespace duckdb
