#pragma once

#include "duckdb/common/multi_file/multi_file_function.hpp"
#include "fls/reader/rowgroup_reader.hpp"

namespace duckdb {
struct FastLanesReadBindData : public TableFunctionData {
	//! Number of rows in the first file, used for estimating the total cardinality of the to-be-read file(s).
	idx_t initial_file_cardinality;
	//! Number of vectors in the first file, used to determine the number of threads.
	idx_t initial_file_n_rowgroups;
};

struct FastLanesReadLocalState : public LocalTableFunctionState {
	idx_t cur_vector;
	//! Rowgroup which is currently being processed.
	idx_t cur_rowgroup;
	fastlanes::up<fastlanes::RowgroupReader> row_group_reader;
};

struct FastLanesReadGlobalState : public GlobalTableFunctionState {
	//! Index of the row group within the current file that is staged for scanning.
	idx_t cur_rowgroup;
};

/**
 * Define all the required functions from the MultiFileFunction template class.
 */
struct FastLanesMultiFileInfo {
	/* ---- MultiFileBind ---- */
	static unique_ptr<BaseFileReaderOptions> InitializeOptions(ClientContext &context,
															   optional_ptr<TableFunctionInfo> info);
	static bool ParseOption(ClientContext &context, const string &key, const Value &val, MultiFileOptions &file_options,
							BaseFileReaderOptions &options);
	/* ---- MultiFileBindInternal ---- */
	/*!
	 * Save user-provided options (currently none) and allocate FastLanesReadBindData (TableFunctionData) object.
	 */
	static unique_ptr<TableFunctionData> InitializeBindData(MultiFileBindData &multi_file_data,
															unique_ptr<BaseFileReaderOptions> options);
	static void BindReader(ClientContext &context, vector<LogicalType> &return_types, vector<string> &names,
						   MultiFileBindData &bind_data);
	static shared_ptr<BaseUnionData> GetUnionData(shared_ptr<BaseFileReader> scan_p, idx_t file_idx);

	static void FinalizeBindData(const MultiFileBindData &multi_file_data);
	/* ---- MultiFileGetBindInfo ---- */
	static void GetBindInfo(const TableFunctionData &bind_data, BindInfo &info);

	/* ---- MultiInitGlobal ---- */
	static unique_ptr<GlobalTableFunctionState>
	InitializeGlobalState(ClientContext &context, MultiFileBindData &bind_data, MultiFileGlobalState &global_state);
	static optional_idx MaxThreads(const MultiFileBindData &bind_data, const MultiFileGlobalState &global_state,
								   FileExpandResult expand_result);
	/* ---- MultiInitLocal ---- */
	static unique_ptr<LocalTableFunctionState> InitializeLocalState(ExecutionContext &, GlobalTableFunctionState &);

	/* ---- MultiFileScan ---- */
	static void Scan(ClientContext &context, BaseFileReader &reader, GlobalTableFunctionState &global_state,
					 LocalTableFunctionState &local_state, DataChunk &chunk);

	/* --- TryOpenNextFile ---- */
	static shared_ptr<BaseFileReader> CreateReader(ClientContext &context, GlobalTableFunctionState &gstate,
												   BaseUnionData &union_data, const MultiFileBindData &bind_data);
	static shared_ptr<BaseFileReader> CreateReader(ClientContext &context, GlobalTableFunctionState &gstate,
												   const OpenFileInfo &file, idx_t file_idx,
												   const MultiFileBindData &bind_data);
	static shared_ptr<BaseFileReader> CreateReader(ClientContext &context, const OpenFileInfo &file,
												   const BaseFileReaderOptions &options,
												   const MultiFileOptions &file_options);
	static void FinalizeReader(ClientContext &context, BaseFileReader &reader, GlobalTableFunctionState &);
	/* ---- TryInitializeNextBatch ---- */
	static void FinishReading(ClientContext &context, GlobalTableFunctionState &global_state,
							  LocalTableFunctionState &local_state);
	static bool TryInitializeScan(ClientContext &context, shared_ptr<BaseFileReader> &reader,
								  GlobalTableFunctionState &gstate, LocalTableFunctionState &lstate);
	static void FinishFile(ClientContext &context, GlobalTableFunctionState &global_state, BaseFileReader &reader);

	/* ---- MultiFileCardinality ---- */
	/*!
	 * Estimate the cardinality of the to-be-read files, the estimate is based on the first file.
	 */
	static unique_ptr<NodeStatistics> GetCardinality(const MultiFileBindData &bind_data_p, idx_t file_count);
	/* ---- MultiFileProgress ---- */
	static double GetProgressInFile(ClientContext &context, const BaseFileReader &reader);
	/* ---- MultiFileGetVirtualColumns ---- */
	static void GetVirtualColumns(ClientContext &context, MultiFileBindData &bind_data, virtual_column_map_t &result);
	static unique_ptr<BaseStatistics> GetStatistics(ClientContext &context, BaseFileReader &reader_p, const string &name);
};

}
