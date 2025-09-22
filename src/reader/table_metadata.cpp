#include "reader/table_metadata.hpp"
#include "fls/common/status.hpp"
#include "fls/file/file_footer.hpp"
#include "fls/file/file_header.hpp"
#include "fls/footer/table_descriptor.hpp"
#include <filesystem>
#include <sstream>
#include <stdexcept>

namespace duckdb {

namespace {
static constexpr const char* TABLE_DESCRIPTOR_FILE_NAME = "table_descriptor.fbb";

void VerifyStatus(const fastlanes::Status& status, const std::string& what) {
	if (!status.success) {
		std::ostringstream err;
		err << what << ": " << fastlanes::Status::message_for(status.code);
		throw std::runtime_error(err.str());
	}
}

} // namespace

TableMetadata::TableMetadata(const std::string& file_path) {
	fastlanes::FileHeader header {};
	fastlanes::FileFooter footer {};

	VerifyStatus(fastlanes::FileHeader::Load(header, file_path), "Failed to load FastLanes file header");
	VerifyStatus(fastlanes::FileFooter::Load(footer, file_path), "Failed to load FastLanes file footer");

	if (header.settings.inline_footer) {
		descriptor =
		    fastlanes::make_table_descriptor(file_path, footer.table_descriptor_offset, footer.table_descriptor_size);
	} else {
		auto table_descriptor_path = std::filesystem::path(file_path).parent_path() / TABLE_DESCRIPTOR_FILE_NAME;
		descriptor                 = fastlanes::make_table_descriptor(table_descriptor_path);
	}

	if (!descriptor) {
		throw std::runtime_error("Failed to load FastLanes table descriptor");
	}
}

const fastlanes::TableDescriptorT& TableMetadata::Descriptor() const {
	if (!descriptor) {
		throw std::runtime_error("Table descriptor not loaded");
	}
	return *descriptor;
}

idx_t TableMetadata::RowGroupCount() const {
	if (!descriptor) {
		throw std::runtime_error("Table descriptor not loaded");
	}
	return descriptor->m_rowgroup_descriptors.size();
}

const fastlanes::RowgroupDescriptorT& TableMetadata::RowGroupDescriptor(idx_t index) const {
	if (!descriptor) {
		throw std::runtime_error("Table descriptor not loaded");
	}
	if (index >= descriptor->m_rowgroup_descriptors.size()) {
		throw std::out_of_range("Row group index out of bounds");
	}
	return *descriptor->m_rowgroup_descriptors[index];
}

} // namespace duckdb
