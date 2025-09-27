#include "reader/fls_reader.hpp"
#include "duckdb/common/constants.hpp"
#include "fls/reader/table_reader.hpp"
#include "reader/schema_builder.hpp"
#include "reader/translation_utils.hpp"
#include <algorithm>
#include <atomic>
#include <duckdb/common/multi_file/multi_file_reader.hpp>
#include <duckdb/execution/adaptive_filter.hpp>
#include <filesystem>
#include <iostream>
#include <vector>

namespace duckdb {

FastLanesReader::FastLanesReader(OpenFileInfo file_p)
    : BaseFileReader(std::move(file_p))
    , table_metadata(file.path)
    , vectors_read(0) {
	Initialize();
}

FastLanesReader::FastLanesReader(OpenFileInfo file_p, FastLanesFileReaderOptions& options)
    : BaseFileReader(std::move(file_p))
    , table_metadata(file.path)
    , vectors_read(0) {
	Initialize();

	if (options.file_row_number) {
		MultiFileColumnDefinition result("file_row_number", LogicalType::BIGINT);
		result.identifier = Value::INTEGER(MultiFileReader::ORDINAL_FIELD_ID);
		columns.push_back(result);
	}
}

FastLanesReader::~FastLanesReader() {
}

void FastLanesReader::Initialize() {
	std::filesystem::path full_path = file.path;
	table_reader                    = make_uniq<fastlanes::TableReader>(full_path, conn);

	const auto&         descriptor = table_metadata.Descriptor();
	const SchemaBuilder schema_builder(descriptor, file.path);
	auto [column_names, promoted_types] = schema_builder.Build();

	columns.reserve(column_names.size());
	for (idx_t col_idx = 0; col_idx < column_names.size(); ++col_idx) {
		auto type = TranslateUtils::TranslateType(promoted_types[col_idx]);
		columns.emplace_back(column_names[col_idx], type);
	}

	rowgroup_statistics.Initialize(descriptor, columns);
}

void FastLanesReader::AddVirtualColumn(column_t virtual_column_id) {
	std::cout << "Adding virtual column " << virtual_column_id << std::endl;
	if (virtual_column_id != MultiFileReader::COLUMN_IDENTIFIER_FILE_ROW_NUMBER) {
		// columns.emplace_back("file_row_number", LogicalType::BIGINT);
		throw InternalException("Unsupported virtual column id %d for parquet reader", virtual_column_id);
	}
}

idx_t FastLanesReader::GetNRowGroups() const {
	return table_metadata.RowGroupCount();
}

idx_t FastLanesReader::GetNVectors(const idx_t row_group_idx) const {
	const auto& rowgroup_descriptor = table_metadata.RowGroupDescriptor(row_group_idx);
	return rowgroup_descriptor.m_n_vec;
}

idx_t FastLanesReader::GetNTuples(const idx_t row_group_idx) const {
	const auto& rowgroup_descriptor = table_metadata.RowGroupDescriptor(row_group_idx);
	return rowgroup_descriptor.m_n_tuples;
}

idx_t FastLanesReader::GetTotalTuples() const {
	idx_t total_n_tuples = 0;
	for (idx_t rowgroup_idx = 0; rowgroup_idx < table_metadata.RowGroupCount(); ++rowgroup_idx) {
		total_n_tuples += table_metadata.RowGroupDescriptor(rowgroup_idx).m_n_tuples;
	}
	return total_n_tuples;
}

fastlanes::up<fastlanes::RowgroupReader> FastLanesReader::CreateRowGroupReader(const idx_t rowgroup_idx) {
	std::vector<uint32_t> projected_ids;
	projected_ids.reserve(column_ids.size());

	for (idx_t i = 0; i < column_ids.size(); i++) {
		const auto col_idx = column_ids[MultiFileLocalIndex(i)].GetId();
		// if (IsVirtualColumn(col_idx)) {
		// 	continue;
		// }
		projected_ids.emplace_back(col_idx);
	}
	return table_reader->get_rowgroup_reader(rowgroup_idx, projected_ids);
}

const std::vector<idx_t>& FastLanesReader::GetRowGroupsToScan() {
	rowgroup_filter_catalog.Initialize(rowgroup_statistics, filters.get(), column_indexes);
	return rowgroup_filter_catalog.EnsureRowGroups();
}
} // namespace duckdb
