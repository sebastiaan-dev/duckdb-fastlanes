#pragma once

#include "duckdb/common/types/column/column_data_collection.hpp"
#include "duckdb/common/types/column/column_data_scan_states.hpp"
#include "duckdb/function/copy_function.hpp"
#include "fls/connection.hpp"
#include "fls/writer/rowgroup_writer.hpp"
#include "fls/writer/writer.hpp"

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
	explicit FastLanesWriteLocalState(ClientContext &context, const vector<LogicalType> &types)
	    : buffer(context, types) {
		buffer.SetPartitionIndex(0); // Makes the buffer manager less likely to spill this data
		buffer.InitializeAppend(append_state);
	}

	std::unique_ptr<fastlanes::RowGroupWriter> rg_writer;

	ColumnDataCollection buffer;
	ColumnDataAppendState append_state;
	//! Current row group being written
	idx_t current_row_group = 0;
	//! Number of rows written to the current row group
	idx_t rows_in_row_group = 0;
};

struct FastLanesWriteGlobalState final : GlobalFunctionData {
	explicit FastLanesWriteGlobalState(const std::string &path) : file_path(path), num_row_groups(0) {
	}

	mutex combine_lock;
	unique_ptr<ColumnDataCollection> combine_buffer;

	fastlanes::Connection conn;
	std::unique_ptr<fastlanes::FileWriter> writer;
	const std::filesystem::path file_path;
	//! Total number of row groups written.
	idx_t num_row_groups;
	std::atomic<idx_t> next_row_group;
};

struct FastLanesWriteBatchData final : PreparedBatchData {
	std::unique_ptr<fastlanes::RowGroupWriter> rg_writer;
};

struct FastLanesFileWriter {
	static unique_ptr<LocalFunctionData> InitLocal(ExecutionContext &context, FunctionData &bind_data_p);
	static void Sink(ExecutionContext &context, FunctionData &bind_data_p, GlobalFunctionData &global_state_p,
	                 LocalFunctionData &local_state_p, DataChunk &input);
	static void Combine(ExecutionContext &context, FunctionData &bind_data_p, GlobalFunctionData &global_state_p,
	                    LocalFunctionData &local_state_p);
	static bool RotateFiles(FunctionData &bind_data_p, const optional_idx &file_size_bytes);
	static bool RotateNextFile(GlobalFunctionData &global_state_p, FunctionData &bind_data_p,
	                           const optional_idx &file_size_bytes);
	static unique_ptr<FunctionData> Bind(ClientContext &context, CopyFunctionBindInput &input,
	                                     const vector<string> &names, const vector<LogicalType> &sql_types);
	static unique_ptr<GlobalFunctionData> InitGlobal(ClientContext &context, FunctionData &bind_data_p,
	                                                 const string &file_path);
	static CopyFunctionExecutionMode GetExecutionMode(bool preserve_insertion_order, bool supports_batch_index);
	static idx_t GetDesiredBatchsize(ClientContext &context, FunctionData &bind_data_p);
	static unique_ptr<PreparedBatchData> PrepareBatch(ClientContext &context, FunctionData &bind_data_p,
	                                                  GlobalFunctionData &global_state_p,
	                                                  unique_ptr<ColumnDataCollection> collection);
	static void FlushBatch(ClientContext &context, FunctionData &bind_data_p, GlobalFunctionData &global_state_p,
	                       PreparedBatchData &batch_p);
	static void Finalize(ClientContext &context, FunctionData &bind_data_p, GlobalFunctionData &global_state_p);
};
} // namespace duckdb
