#pragma once

#include "write_fast_lanes.hpp"

namespace duckdb {

//! Options for writing FastLanes files
struct FastLanesWriteBindData final : TableFunctionData {
	//! Row group size, denoted in the tuple count. Must be a multiple of 1024.
	uint64_t row_group_size = fastlanes::CFG::N_VEC_PER_RG * fastlanes::CFG::VEC_SZ;
	//! Maximum row groups that will be written to one file.
	size_t row_groups_per_file = 0;
	//! If the footer should be included or separate from the FastLanes file.
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
	unique_ptr<fastlanes::FileWriter> writer;
	const std::filesystem::path file_path;
	//! Total number of row groups written.
	idx_t num_row_groups;
	std::atomic<idx_t> next_row_group;
	time_point<steady_clock> start;
};

struct FastLanesWriteBatchData final : PreparedBatchData {
	std::unique_ptr<fastlanes::RowGroupWriter> rg_writer;
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
