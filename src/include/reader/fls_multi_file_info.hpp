#pragma once

#include "duckdb/common/constants.hpp"
#include "duckdb/common/multi_file/multi_file_function.hpp"
#include "fls/reader/rowgroup_reader.hpp"
#include "materializer/column_decoder.hpp"
#include <duckdb/execution/adaptive_filter.hpp>

namespace duckdb {
struct FastLanesScanFilter;
class ColumnDecoder;

class FastLanesFileReaderOptions final : public BaseFileReaderOptions {
public:
	explicit FastLanesFileReaderOptions() {
	}

	idx_t explicit_cardinality = 0;
	bool  file_row_number      = false;
};

struct FastLanesReadBindData final : TableFunctionData {
	//! Number of rows in the first file, used for estimating the total cardinality of the to-be-read file(s).
	idx_t initial_file_cardinality;
	//! Number of vectors in the first file, used to determine the number of threads.
	idx_t initial_file_n_rowgroups;

	unique_ptr<FastLanesFileReaderOptions> options;
};

struct FastLanesReadLocalState final : LocalTableFunctionState {
	//! Row group which is being scanned by the worker, used to fetch row group related metadata.
	idx_t cur_rowgroup;
	//! Vector in the row group that is up for decoding, starts at 0 for every row group.
	idx_t cur_vector;
	//! Local row group reader derived from a global FastLanes instance. Each local reader manages its own buffers.
	fastlanes::up<fastlanes::RowgroupReader> row_group_reader;
	//! Column decoders provide a wrapper over decoding kernels for cross-vector column state.
	std::vector<unique_ptr<materializer::ColumnDecoder>> column_decoders;
	//! Container for table filters that have to be applied.
	std::vector<FastLanesScanFilter> scan_filters;
	//! Adaptive reordering of scan filters.
	unique_ptr<AdaptiveFilter> adaptive_filter;
	//!
	std::vector<std::vector<FastLanesScanFilter*>> filters_by_col;
};

struct FastLanesReadGlobalState final : GlobalTableFunctionState {
	//! Index into rowgroups_to_scan, indicating the row group within the current file that is staged for scanning.
	idx_t next_rowgroup;
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
