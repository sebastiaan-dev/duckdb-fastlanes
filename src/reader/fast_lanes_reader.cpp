#include "reader/fast_lanes_reader.hpp"

#include "reader/translation_utils.hpp"

namespace duckdb {

FastLanesReader::FastLanesReader(OpenFileInfo file_p) : BaseFileReader(std::move(file_p)) {
	D_ASSERT(StringUtil::EndsWith(file.path, ".fls"));

	std::filesystem::path full_path = file.path;
	dir_path = full_path.parent_path();
	table_reader = make_uniq<fastlanes::TableReader>(dir_path, conn);
	fastlanes::RowgroupDescriptor rowgroup_descriptor = table_reader->get_file_metadata().m_rowgroup_descriptors[0];
	auto column_descriptors = rowgroup_descriptor.GetColumnDescriptors();

	// Configure the schema based on the data provided by the footer
	for (auto &column_descriptor : column_descriptors) {
		auto type = TranslateUtils::TranslateType(column_descriptor.data_type);
		auto name = column_descriptor.name;

		MultiFileColumnDefinition result(name, type);

		columns.push_back(result);
	}
}

FastLanesReader::~FastLanesReader() {
}

fastlanes::TableDescriptor &FastLanesReader::GetFileMetadata() const {
	return table_reader->get_file_metadata();
}


idx_t FastLanesReader::GetNRowGroups() const {
	return table_reader->get_n_rowgroups();
}

idx_t FastLanesReader::GetNVectors(idx_t row_group_idx) const {
	const fastlanes::vector<fastlanes::RowgroupDescriptor>& descriptors =
	    table_reader->get_file_metadata().m_rowgroup_descriptors;
	D_ASSERT(row_group_idx < descriptors.size());

	return descriptors[row_group_idx].GetNVectors();
}


idx_t FastLanesReader::GetNRows() const {
	const fastlanes::TableDescriptorT& table_descriptor = table_reader->get_file_metadata();

	idx_t total_n_vectors = 0;
	for (auto& row_group_descriptor : table_descriptor.m_rowgroup_descriptors) {
		total_n_vectors += row_group_descriptor.GetNVectors();
	}

	return total_n_vectors * fastlanes::CFG::VEC_SZ;
}

fastlanes::up<fastlanes::RowgroupReader> FastLanesReader::CreateRowGroupReader(const idx_t rowgroup_idx) {
	return table_reader->get_rowgroup_reader(rowgroup_idx);
}

}