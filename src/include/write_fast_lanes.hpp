#pragma once

#include <duckdb/main/database.hpp>

namespace duckdb {

class WriteFastLanes {
public:
	/**
	 * @brief Register is responsible for registering functions available to the end user.
	 * It registers the write_fls function that allows writing data to FastLanes format.
	 *
	 * @param db The currently running DuckDB database instance.
	 */
	static void Register(DatabaseInstance &db);
};

/**
 * Provides an abstraction over the FastLanes file format for writing data.
 */
// class FastLanesWriter : public BaseFileWriter {
// public:
// 	explicit FastLanesWriter(const string &file_path, const vector<LogicalType> &types, const vector<string> &names);
// 	~FastLanesWriter() override;
//
// 	std::string GetWriterType() const override {
// 		return "FastLanes";
// 	}
//
// 	// Initialize the writer with the given schema
// 	void Initialize();
//
// 	// Flush any buffered data and finalize the file
// 	void Finalize() override;
//
// 	// Write a chunk of data to the file
// 	void WriteChunk(DataChunk &chunk) override;
//
// 	// Flush the current row group and start a new one
// 	void FlushRowGroup();
//
// 	// Get the number of rows written so far
// 	idx_t GetRowsWritten() const {
// 		return total_rows_written;
// 	}
//
// 	// Get the number of row groups written so far
// 	idx_t GetRowGroupsWritten() const {
// 		return current_row_group;
// 	}
//
// 	// Set the maximum number of rows per row group
// 	void SetRowsPerRowGroup(idx_t rows_per_group) {
// 		rows_per_row_group = rows_per_group;
// 	}
//
// private:
// 	// Convert DuckDB types to FastLanes types
// 	vector<fastlanes::Type> ConvertTypes();
//
// 	//! Path of the directory where the FastLanes data will be written
// 	std::filesystem::path dir_path;
// 	//! Connection to the FastLanes library
// 	fastlanes::Connection conn;
// 	//! Table writer for FastLanes
// 	unique_ptr<fastlanes::TableWriter> table_writer;
// 	//! Schema information
// 	vector<LogicalType> column_types;
// 	vector<string> column_names;
// 	//! Current row group being written
// 	idx_t current_row_group;
// 	//! Number of rows in the current row group
// 	idx_t rows_in_row_group;
// 	//! Total number of rows written
// 	idx_t total_rows_written;
// 	//! Maximum number of rows per row group
// 	idx_t rows_per_row_group;
// 	//! Default maximum number of rows per row group
// 	static constexpr idx_t DEFAULT_ROWS_PER_ROW_GROUP = 100000;
// };

} // namespace duckdb
