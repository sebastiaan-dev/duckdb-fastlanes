#pragma once

#include "duckdb/common/types.hpp"
#include "fls/connection.hpp"
#include "fls/footer/rowgroup_descriptor.hpp"
#include "fls/footer/table_descriptor.hpp"
#include <memory>
#include <string>

namespace duckdb {

class TableMetadata {
public:
	explicit TableMetadata(const std::string& path);

	const fastlanes::TableDescriptor&    Descriptor() const;
	idx_t                                RowGroupCount() const;
	const fastlanes::RowgroupDescriptor& RowGroupDescriptor(idx_t index) const;
	const fastlanes::TableReader&        TableReader() const;

private:
	fastlanes::Connection              conn;
	unique_ptr<fastlanes::TableReader> table_reader;

	std::unique_ptr<fastlanes::TableDescriptorT> descriptor;
};

} // namespace duckdb
