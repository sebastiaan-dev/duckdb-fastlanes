#pragma once

#include "duckdb/common/multi_file/base_file_reader.hpp"
#include "fls/footer/table_descriptor.hpp"
#include "fls/reader/rowgroup_reader.hpp"

#include <string>

namespace duckdb {
/**
 * Provides an abstraction over the FastLanes file format which allows for implicit multithreaded support with DuckDB.
 */
class FastLanesReader : public BaseFileReader {
public:
	explicit FastLanesReader(OpenFileInfo file_p);
	~FastLanesReader() override;

	// TODO: What does this do?x
	std::string GetReaderType() const override {
		return "FastLanes";
	}

	const fastlanes::TableDescriptorT& GetFileMetadata() const;
	idx_t GetNRowGroups() const;
	idx_t GetNRows() const;
	idx_t GetNVectors(idx_t row_group_idx) const;
	fastlanes::up<fastlanes::RowgroupReader> CreateRowGroupReader(idx_t rowgroup_idx);

private:
	//! Path of the directory containing both the FastLanes data file and metadata file.
	std::filesystem::path dir_path;
	fastlanes::Connection conn;
	unique_ptr<fastlanes::TableReader> table_reader;
};
}