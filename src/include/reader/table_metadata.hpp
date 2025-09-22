#pragma once

#include "duckdb/common/types.hpp"
#include "fls/footer/rowgroup_descriptor.hpp"
#include "fls/footer/table_descriptor.hpp"
#include <memory>
#include <string>

namespace duckdb {

class TableMetadata {
public:
	explicit TableMetadata(const std::string& file_path);

	const fastlanes::TableDescriptorT& Descriptor() const;
	idx_t                               RowGroupCount() const;
	const fastlanes::RowgroupDescriptorT& RowGroupDescriptor(idx_t index) const;

private:
	std::unique_ptr<fastlanes::TableDescriptorT> descriptor;
};

} // namespace duckdb
