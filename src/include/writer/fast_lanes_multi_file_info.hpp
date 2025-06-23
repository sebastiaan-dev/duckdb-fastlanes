#pragma once

#include "write_fast_lanes.hpp"

namespace duckdb {

struct FastLanesWriteBindData final : TableFunctionData {
	//! Options for writing FastLanes files

	//! Row group size, denoted in vector count.
	uint64_t row_group_size = fastlanes::CFG::N_VEC_PER_RG;
	//! Maximum row groups that will be written to one file.
	optional_idx row_groups_per_file;
	bool inline_footer = true;

	vector<LogicalType> types;
	vector<string> names;
};

struct FastLanesWriteLocalState final : LocalFunctionData {
	//! Current row group being written
	idx_t current_row_group;
	//! Number of rows written to the current row group
	idx_t rows_in_row_group;
};

struct FastLanesWriteGlobalState final : GlobalFunctionData {
	explicit FastLanesWriteGlobalState(const std::string &path) : file_path(path), num_row_groups(0) {
	}

	fastlanes::Connection conn;
	unique_ptr<fastlanes::Writer> writer;
	const std::string &file_path;
	//! Total number of row groups written.
	idx_t num_row_groups;
	std::atomic<idx_t> next_row_group;
};

struct FastLanesWriteBatchData final : PreparedBatchData {
	unique_ptr<fastlanes::Rowgroup> row_group;
};

/**
 * Define all the required functions from the MultiFileWriter template class.
 */
// struct FastLanesMultiFileWriterInfo {
// 	/* ---- MultiFileBind ---- */
// 	static unique_ptr<BaseFileWriterOptions> InitializeOptions(ClientContext &context,
// 	                                                          optional_ptr<TableFunctionInfo> info);
// 	static bool ParseOption(ClientContext &context, const string &key, const Value &val,
// 	                       MultiFileOptions &file_options, BaseFileWriterOptions &options);
//
// 	/* ---- MultiFileBindInternal ---- */
// 	static unique_ptr<TableFunctionData> InitializeBindData(MultiFileWriterBindData &multi_file_data,
// 	                                                       unique_ptr<BaseFileWriterOptions> options);
// 	static void BindWriter(ClientContext &context, vector<LogicalType> &return_types, vector<string> &names,
// 	                      MultiFileWriterBindData &bind_data);
// 	static void FinalizeBindData(const MultiFileWriterBindData &multi_file_data);
//
// 	/* ---- MultiInitGlobal ---- */
// 	static unique_ptr<GlobalTableFunctionState>
// 	InitializeGlobalState(ClientContext &context, MultiFileWriterBindData &bind_data,
// 	                     MultiFileWriterGlobalState &global_state);
//
// 	/* ---- MultiInitLocal ---- */
// 	static unique_ptr<LocalTableFunctionState> InitializeLocalState(ExecutionContext &,
// 	                                                              GlobalTableFunctionState &);
//
// 	/* ---- MultiFileWrite ---- */
// 	static void Write(ClientContext &context, BaseFileWriter &writer, GlobalTableFunctionState &global_state,
// 	                 LocalTableFunctionState &local_state, DataChunk &chunk);
//
// 	/* ---- CreateWriter ---- */
// 	static unique_ptr<BaseFileWriter> CreateWriter(ClientContext &context, const string &file_path,
// 	                                             const vector<LogicalType> &types, const vector<string> &names,
// 	                                             const BaseFileWriterOptions &options,
// 	                                             const MultiFileOptions &file_options);
//
// 	/* ---- FinalizeWriter ---- */
// 	static void FinalizeWriter(ClientContext &context, BaseFileWriter &writer, GlobalTableFunctionState &global_state);
// };

} // namespace duckdb
