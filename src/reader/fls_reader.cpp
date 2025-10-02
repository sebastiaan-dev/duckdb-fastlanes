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
    , vectors_read(0) {
	Initialize();
}

FastLanesReader::FastLanesReader(OpenFileInfo file_p, FastLanesFileReaderOptions& options)
    : BaseFileReader(std::move(file_p))
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
	table_metadata = make_uniq<TableMetadata>(file.path);

	const auto&         descriptor = table_metadata->Descriptor();
	const SchemaBuilder schema_builder(descriptor, file.path);
	auto [column_names, promoted_types] = schema_builder.Build();

	columns.reserve(column_names.size());
	for (idx_t col_idx = 0; col_idx < column_names.size(); ++col_idx) {
		// If we are dealing with a decimal column we skip the promoted type.
		if (const auto& dtype =
		        descriptor.m_rowgroup_descriptors[0]->m_column_descriptors[col_idx]->fix_me_decimal_type) {
			columns.emplace_back(column_names[col_idx], LogicalType::DECIMAL(dtype->precision, dtype->scale));
			continue;
		}

		auto type = TranslateUtils::TranslateType(promoted_types[col_idx]);
		columns.emplace_back(column_names[col_idx], type);
	}

	for (idx_t rowgroup_idx = 0; rowgroup_idx < table_metadata->RowGroupCount(); ++rowgroup_idx) {
		total_tuples += table_metadata->RowGroupDescriptor(rowgroup_idx).m_n_tuples;
		total_vectors += table_metadata->RowGroupDescriptor(rowgroup_idx).m_n_vec;
		// std::cout << "rowgroup_idx: " << rowgroup_idx
		//           << " has vectors: " << table_metadata->RowGroupDescriptor(rowgroup_idx).m_n_vec << "\n";
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

unique_ptr<BaseStatistics> FastLanesReader::GetStatistics(ClientContext& context, const string& name) {
	idx_t file_col_idx;
	for (file_col_idx = 0; file_col_idx < columns.size(); file_col_idx++) {
		if (columns[file_col_idx].name == name) {
			break;
		}
	}
	if (file_col_idx == columns.size()) {
		return nullptr;
	}

	auto type = columns[file_col_idx].type;

	switch (type.id()) {
	case LogicalTypeId::UTINYINT:
	case LogicalTypeId::USMALLINT:
	case LogicalTypeId::UINTEGER:
	case LogicalTypeId::UBIGINT:
	case LogicalTypeId::TINYINT:
	case LogicalTypeId::SMALLINT:
	case LogicalTypeId::INTEGER:
	case LogicalTypeId::BIGINT:
	case LogicalTypeId::DATE:
	case LogicalTypeId::TIME:
	case LogicalTypeId::TIME_TZ:
	case LogicalTypeId::TIMESTAMP:
	case LogicalTypeId::TIMESTAMP_TZ:
	case LogicalTypeId::TIMESTAMP_SEC:
	case LogicalTypeId::TIMESTAMP_MS:
	case LogicalTypeId::TIMESTAMP_NS:
	case LogicalTypeId::DECIMAL:
	case LogicalTypeId::FLOAT:
	case LogicalTypeId::DOUBLE:
		break;
	case LogicalTypeId::VARCHAR:
	default:
		return nullptr;
	}

	unique_ptr<BaseStatistics> column_stats;
	for (idx_t row_group_idx = 0; row_group_idx < table_metadata->RowGroupCount(); row_group_idx++) {
		auto        chunk_stats    = NumericStats::CreateUnknown(type);
		const auto* internal_stats = rowgroup_statistics.GetStats(row_group_idx, file_col_idx);

		// TODO: Move this conversion into statistics helper.
		Value casted;
		internal_stats->min.DefaultTryCastAs(type, casted, nullptr);
		NumericStats::SetMin(chunk_stats, casted);

		internal_stats->max.DefaultTryCastAs(type, casted, nullptr);
		NumericStats::SetMax(chunk_stats, casted);

		// FIXME: FastLanes does not support NULL values currently.
		chunk_stats.Set(StatsInfo::CANNOT_HAVE_NULL_VALUES);

		if (!column_stats) {
			column_stats = chunk_stats.ToUnique();
		} else {
			column_stats->Merge(*chunk_stats.ToUnique());
		}
	}

	return column_stats;
}

idx_t FastLanesReader::GetNRowGroups() const {
	return table_metadata->RowGroupCount();
}

idx_t FastLanesReader::GetNVectors(const idx_t row_group_idx) const {
	const auto& rowgroup_descriptor = table_metadata->RowGroupDescriptor(row_group_idx);
	return rowgroup_descriptor.m_n_vec;
}

idx_t FastLanesReader::GetNTuples(const idx_t row_group_idx) const {
	const auto& rowgroup_descriptor = table_metadata->RowGroupDescriptor(row_group_idx);
	return rowgroup_descriptor.m_n_tuples;
}

size_t FastLanesReader::GetTotalTuples() const {
	return total_tuples;
}

size_t FastLanesReader::GetTotalVectors() const {
	return total_vectors;
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

	return table_metadata->TableReader().get_rowgroup_reader(rowgroup_idx, projected_ids);
}

const std::vector<idx_t>& FastLanesReader::GetRowGroupsToScan() {
	rowgroup_filter_catalog.Initialize(rowgroup_statistics, filters.get(), column_indexes);
	return rowgroup_filter_catalog.EnsureRowGroups();
}
} // namespace duckdb
