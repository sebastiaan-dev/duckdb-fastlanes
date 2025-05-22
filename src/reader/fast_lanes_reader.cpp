#include "reader/fast_lanes_reader.hpp"

#include "reader/translation_utils.hpp"

#include <iostream>

namespace duckdb {

FastLanesReader::FastLanesReader(OpenFileInfo file_p) : BaseFileReader(std::move(file_p)) {
	D_ASSERT(StringUtil::EndsWith(file.path, ".fls"));

	std::filesystem::path full_path = file.path;
	table_reader = make_uniq<fastlanes::TableReader>(full_path, conn);
	auto& column_descriptors = table_reader->get_file_metadata().m_rowgroup_descriptors[0]->m_column_descriptors;

	// Configure the schema based on the data provided by the footer
	for (auto &column_descriptor : column_descriptors) {
		auto type = TranslateUtils::TranslateType(column_descriptor->data_type);
		auto name = column_descriptor->name;

		MultiFileColumnDefinition result(name, type);

		columns.push_back(result);
	}
}

FastLanesReader::~FastLanesReader() {
}

fastlanes::TableDescriptorT& FastLanesReader::GetFileMetadata() const {
	return table_reader->get_file_metadata();
}


idx_t FastLanesReader::GetNRowGroups() const {
	return table_reader->get_n_rowgroups();
}

idx_t FastLanesReader::GetNVectors(idx_t row_group_idx) const {
	fastlanes::TableDescriptorT& table = table_reader->get_file_metadata();
	auto& row_descriptors = table.m_rowgroup_descriptors;

	D_ASSERT(row_group_idx < row_descriptors.size());

	return row_descriptors[row_group_idx]->m_n_vec;
}


idx_t FastLanesReader::GetNRows() const {
	const fastlanes::TableDescriptorT& table_descriptor = table_reader->get_file_metadata();

	idx_t total_n_vectors = 0;
	for (auto& row_group_descriptor : table_descriptor.m_rowgroup_descriptors) {
		total_n_vectors += row_group_descriptor->m_n_vec;
	}

	return total_n_vectors * fastlanes::CFG::VEC_SZ;
}

fastlanes::up<fastlanes::RowgroupReader> FastLanesReader::CreateRowGroupReader(const idx_t rowgroup_idx) {
	return table_reader->get_rowgroup_reader(rowgroup_idx);
}

}