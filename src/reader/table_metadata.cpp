#include "reader/table_metadata.hpp"
#include "fls/common/status.hpp"
#include "fls/file/file_footer.hpp"
#include "fls/file/file_header.hpp"
#include "fls/footer/table_descriptor.hpp"
#include "fls/reader/table_reader.hpp"
#include <filesystem>
#include <sstream>
#include <stdexcept>

namespace duckdb {

TableMetadata::TableMetadata(const std::string& path) {
	std::filesystem::path full_path = path;
	table_reader                    = make_uniq<fastlanes::TableReader>(full_path, conn);
}

const fastlanes::TableDescriptorT& TableMetadata::Descriptor() const {
	return table_reader->get_descriptor();
}

idx_t TableMetadata::RowGroupCount() const {
	return table_reader->get_descriptor().m_rowgroup_descriptors.size();
}

const fastlanes::RowgroupDescriptorT& TableMetadata::RowGroupDescriptor(idx_t index) const {
	if (index >= table_reader->get_descriptor().m_rowgroup_descriptors.size()) {
		throw std::out_of_range("Row group index out of bounds");
	}
	return *table_reader->get_descriptor().m_rowgroup_descriptors[index];
}

const fastlanes::TableReader& TableMetadata::TableReader() const {
	return *table_reader;
}

} // namespace duckdb
