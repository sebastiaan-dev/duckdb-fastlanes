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

struct FastLanesWriteLocalState : public LocalFunctionData {
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

} // namespace duckdb
