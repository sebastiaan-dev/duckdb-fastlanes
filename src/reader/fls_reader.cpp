#include "reader/fls_reader.hpp"
#include "duckdb/common/constants.hpp"
#include "duckdb/common/exception.hpp"
#include "fls/footer/operator_token_generated.h"
#include "fls/reader/table_reader.hpp"
#include "reader/schema_builder.hpp"
#include "reader/translation_utils.hpp"
#include <algorithm>
#include <atomic>
#include <deque>
#include <duckdb/common/multi_file/multi_file_reader.hpp>
#include <duckdb/execution/adaptive_filter.hpp>
#include <iostream>
#include <unordered_map>
#include <utility>
#include <vector>

namespace duckdb {

bool FastLanesReader::IsExternalDictOperatorToken(fastlanes::OperatorToken token) {
	switch (token) {
	case fastlanes::OperatorToken::EXP_DICT_I64_U32:
	case fastlanes::OperatorToken::EXP_DICT_I64_U16:
	case fastlanes::OperatorToken::EXP_DICT_I64_U08:
	case fastlanes::OperatorToken::EXP_DICT_I32_U32:
	case fastlanes::OperatorToken::EXP_DICT_I32_U16:
	case fastlanes::OperatorToken::EXP_DICT_I32_U08:
	case fastlanes::OperatorToken::EXP_DICT_I16_U16:
	case fastlanes::OperatorToken::EXP_DICT_I16_U08:
	case fastlanes::OperatorToken::EXP_DICT_I08_U08:
	case fastlanes::OperatorToken::EXP_DICT_U08_U08:
	case fastlanes::OperatorToken::EXP_DICT_DBL_U32:
	case fastlanes::OperatorToken::EXP_DICT_DBL_U16:
	case fastlanes::OperatorToken::EXP_DICT_DBL_U08:
	case fastlanes::OperatorToken::EXP_DICT_FLT_U08:
	case fastlanes::OperatorToken::EXP_DICT_STR_U32:
	case fastlanes::OperatorToken::EXP_DICT_STR_U16:
	case fastlanes::OperatorToken::EXP_DICT_STR_U08:
		return true;
	default:
		return false;
	}
}

bool FastLanesReader::HasMccEncoding(const fastlanes::RowgroupDescriptor& rowgroup_descriptor,
                                     const std::vector<uint32_t>&         column_ids) {
	const auto column_count = static_cast<idx_t>(rowgroup_descriptor.m_column_descriptors()->size());
	for (const auto col_id : column_ids) {
		if (col_id >= column_count) {
			throw InternalException("Projected column index out of range");
		}
		const auto* column_descriptor = rowgroup_descriptor.m_column_descriptors()->Get(col_id);
		const auto* rpn               = column_descriptor->encoding_rpn();
		if (!rpn) {
			continue;
		}
		const auto* operator_tokens = rpn->operator_tokens();
		if (!operator_tokens || operator_tokens->empty()) {
			continue;
		}
		const auto* operand_tokens = rpn->operand_tokens();
		for (auto i = 0U; i < operator_tokens->size(); ++i) {
			switch (operator_tokens->Get(i)) {
			case fastlanes::OperatorToken::EXP_EQUAL:
				if (!operand_tokens || operand_tokens->empty()) {
					throw InternalException("EXP_EQUAL without operand token");
				}
				return true;
			default:
				if (IsExternalDictOperatorToken(operator_tokens->Get(i))) {
					if (!operand_tokens || operand_tokens->empty()) {
						throw InternalException("External dictionary encoding without operand token");
					}
					const auto dep = operand_tokens->Get(0);
					if (dep < column_count) {
						return true;
					}
				}
				break;
			}
		}
	}
	return false;
}

optional_idx FastLanesReader::GetMccDependency(const fastlanes::ColumnDescriptor& column_descriptor,
                                               idx_t                              column_count) {
	const auto* rpn = column_descriptor.encoding_rpn();
	if (!rpn) {
		return optional_idx();
	}
	const auto* operator_tokens = rpn->operator_tokens();
	if (!operator_tokens || operator_tokens->empty()) {
		return optional_idx();
	}
	const auto* operand_tokens = rpn->operand_tokens();

	bool has_equal         = false;
	bool has_external_dict = false;
	bool has_choose_dict   = false;

	for (auto i = 0U; i < operator_tokens->size(); ++i) {
		switch (operator_tokens->Get(i)) {
		case fastlanes::OperatorToken::EXP_EQUAL:
			has_equal = true;
			break;
		case fastlanes::OperatorToken::WIZARD_CHOOSE_DICT:
			has_choose_dict = true;
			break;
		default:
			if (IsExternalDictOperatorToken(operator_tokens->Get(i))) {
				has_external_dict = true;
			}
			break;
		}
	}

	if (has_equal && (has_external_dict || has_choose_dict)) {
		throw InternalException("Unsupported MCC encoding with multiple dependencies");
	}

	if (has_equal) {
		if (!operand_tokens || operand_tokens->empty()) {
			throw InternalException("EXP_EQUAL without operand token");
		}
		const auto dep = operand_tokens->Get(0);
		if (dep >= column_count) {
			throw InternalException("EXP_EQUAL dependency index out of range");
		}
		return optional_idx(static_cast<idx_t>(dep));
	}

	if (has_external_dict) {
		if (!operand_tokens || operand_tokens->empty()) {
			throw InternalException("External dictionary encoding without operand token");
		}
		const auto dep = operand_tokens->Get(0);
		if (dep >= column_count) {
			throw InternalException("External dictionary dependency index out of range");
		}
		return optional_idx(static_cast<idx_t>(dep));
	}

	if (has_choose_dict) {
		if (operand_tokens && operand_tokens->size() >= 2) {
			const auto dep = operand_tokens->Get(0);
			if (dep >= column_count) {
				throw InternalException("External dictionary dependency index out of range");
			}
			return optional_idx(static_cast<idx_t>(dep));
		}
	}

	return optional_idx();
}

// FIXME:
// - Late initialization with projected columns for statistics
// - Even more so, do we need to construct all columns or can we do away with all other columns?
// - Check that multi-file stuff works correctly (are stats reset?)
// - Can the columns_id of a reader change for a given Reader?

// DUCKDB reads all column types during bind (verify), so there is no way to optimize for this.

FastLanesReader::FastLanesReader(OpenFileInfo file_p)
    : BaseFileReader(std::move(file_p))
    , vectors_read(0) {
	reader_options.explicit_cardinality = 0;
	reader_options.file_row_number      = false;
	Initialize();
}

FastLanesReader::FastLanesReader(OpenFileInfo file_p, const FastLanesFileReaderOptions& options)
    : BaseFileReader(std::move(file_p))
    , vectors_read(0)
    , reader_options(options) {
	Initialize();

	if (reader_options.file_row_number) {
		EnsureFileRowNumberColumn();
	}
}

FastLanesReader::~FastLanesReader() {
}

void FastLanesReader::Initialize() {
	file_row_number_local_idx.SetInvalid();

	table_metadata = make_uniq<TableMetadata>(file.path);

	const auto&         descriptor = table_metadata->Descriptor();
	const SchemaBuilder schema_builder(descriptor, file.path);
	auto [column_names, promoted_types] = schema_builder.Build();

	columns.clear();
	columns.reserve(column_names.size());
	for (idx_t col_idx = 0; col_idx < column_names.size(); ++col_idx) {
		// If we are dealing with a decimal column we skip the promoted type.
		if (const auto& dtype = descriptor.m_rowgroup_descriptors()
		                            ->Get(0)
		                            ->m_column_descriptors()
		                            ->Get(col_idx)
		                            ->fix_me_decimal_type()) {
			columns.emplace_back(column_names[col_idx], LogicalType::DECIMAL(dtype->precision(), dtype->scale()));
			continue;
		}

		auto type = TranslateUtils::TranslateType(promoted_types[col_idx]);
		columns.emplace_back(column_names[col_idx], type);
	}

	row_group_offsets.reserve(table_metadata->RowGroupCount());
	idx_t cumulative_offset = 0;
	for (idx_t rowgroup_idx = 0; rowgroup_idx < table_metadata->RowGroupCount(); ++rowgroup_idx) {
		row_group_offsets.push_back(cumulative_offset);
		auto& rowgroup_descriptor = table_metadata->RowGroupDescriptor(rowgroup_idx);
		total_tuples += rowgroup_descriptor.m_n_tuples();
		total_vectors += rowgroup_descriptor.m_n_vec();
		cumulative_offset += rowgroup_descriptor.m_n_tuples();
	}

	rowgroup_statistics.Initialize(descriptor, columns);
}

void FastLanesReader::EnsureFileRowNumberColumn() {
	if (file_row_number_local_idx.IsValid()) {
		return;
	}
	MultiFileColumnDefinition result("file_row_number", LogicalType::BIGINT);
	result.identifier         = Value::INTEGER(MultiFileReader::ORDINAL_FIELD_ID);
	file_row_number_local_idx = optional_idx(columns.size());
	columns.push_back(std::move(result));
	reader_options.file_row_number = true;
}

bool FastLanesReader::IsFileRowNumberColumn(column_t column_id) const {
	if (!file_row_number_local_idx.IsValid()) {
		return false;
	}
	return column_id == file_row_number_local_idx.GetIndex();
}

void FastLanesReader::AddVirtualColumn(column_t virtual_column_id) {
	if (virtual_column_id == MultiFileReader::COLUMN_IDENTIFIER_FILE_ROW_NUMBER) {
		EnsureFileRowNumberColumn();
		return;
	}
	throw InternalException("Unsupported virtual column id %d for FastLanes reader", virtual_column_id);
}

const FastLanesFileReaderOptions& FastLanesReader::GetOptions() const {
	return reader_options;
}

shared_ptr<BaseUnionData> FastLanesReader::GetUnionData(idx_t file_idx) {
	auto data = make_shared_ptr<FastLanesUnionData>(file);
	data->names.reserve(columns.size());
	data->types.reserve(columns.size());
	for (auto& column : columns) {
		data->names.push_back(column.name);
		data->types.push_back(column.type);
	}
	data->options = reader_options;
	if (file_idx == 0) {
		data->reader = shared_from_this();
	}
	return data;
}

unique_ptr<BaseStatistics> GetNumericalStats(const ColumnStats& internal_stats, const LogicalType& type) {
	auto chunk_stats = NumericStats::CreateUnknown(type);
	// TODO: Move this conversion into statistics helper.
	Value casted;
	internal_stats.min.DefaultTryCastAs(type, casted, nullptr);
	NumericStats::SetMin(chunk_stats, casted);

	internal_stats.max.DefaultTryCastAs(type, casted, nullptr);
	NumericStats::SetMax(chunk_stats, casted);

	// FIXME: FastLanes does not support NULL values currently.
	chunk_stats.Set(StatsInfo::CANNOT_HAVE_NULL_VALUES);

	return chunk_stats.ToUnique();
}

unique_ptr<BaseStatistics> GetStringStats(const ColumnStats& internal_stats, const LogicalType& type) {
	auto chunk_stats = StringStats::CreateUnknown(type);

	// FIXME: FastLanes does not support NULL values currently.
	chunk_stats.Set(StatsInfo::CANNOT_HAVE_NULL_VALUES);

	return chunk_stats.ToUnique();
}

unique_ptr<BaseStatistics> GetColumnStats(const ColumnStats& internal_stats, const LogicalType& type) {
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
		return GetNumericalStats(internal_stats, type);
	case LogicalTypeId::VARCHAR:
		return GetStringStats(internal_stats, type);
	default:
		return nullptr;
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

	const auto& type = columns[file_col_idx].type;
	if (IsFileRowNumberColumn(file_col_idx)) {
		unique_ptr<BaseStatistics> column_stats;
		for (idx_t row_group_idx = 0; row_group_idx < table_metadata->RowGroupCount(); row_group_idx++) {
			auto        chunk_stats      = NumericStats::CreateUnknown(type);
			const idx_t row_group_offset = row_group_offsets[row_group_idx];
			const idx_t row_group_count  = table_metadata->RowGroupDescriptor(row_group_idx).m_n_tuples();
			NumericStats::SetMin(chunk_stats, Value::BIGINT(static_cast<int64_t>(row_group_offset)));
			NumericStats::SetMax(chunk_stats, Value::BIGINT(static_cast<int64_t>(row_group_offset + row_group_count)));
			chunk_stats.Set(StatsInfo::CANNOT_HAVE_NULL_VALUES);
			auto chunk_stats_unique = chunk_stats.ToUnique();
			if (!column_stats) {
				column_stats = std::move(chunk_stats_unique);
			} else {
				column_stats->Merge(*chunk_stats_unique);
			}
		}
		return column_stats;
	}

	unique_ptr<BaseStatistics> column_stats;

	for (idx_t row_group_idx = 0; row_group_idx < table_metadata->RowGroupCount(); row_group_idx++) {
		const auto* internal_stats = rowgroup_statistics.GetStats(row_group_idx, file_col_idx);
		auto        chunk_stats    = GetColumnStats(*internal_stats, type);
		if (!chunk_stats) {
			return nullptr;
		}

		if (!column_stats) {
			column_stats = std::move(chunk_stats);
		} else {
			column_stats->Merge(*chunk_stats);
		}
	}

	return column_stats;
}

idx_t FastLanesReader::GetNRowGroups() const {
	return table_metadata->RowGroupCount();
}

idx_t FastLanesReader::GetNVectors(const idx_t row_group_idx) const {
	const auto& rowgroup_descriptor = table_metadata->RowGroupDescriptor(row_group_idx);
	return rowgroup_descriptor.m_n_vec();
}

idx_t FastLanesReader::GetNTuples(const idx_t row_group_idx) const {
	const auto& rowgroup_descriptor = table_metadata->RowGroupDescriptor(row_group_idx);
	return rowgroup_descriptor.m_n_tuples();
}

size_t FastLanesReader::GetTotalTuples() const {
	return total_tuples;
}

size_t FastLanesReader::GetTotalVectors() const {
	return total_vectors;
}

FastLanesReader::ProjectionExpansion FastLanesReader::ExpandProjectedColumns(idx_t rowgroup_idx) {
	const auto&           descriptor          = table_metadata->Descriptor();
	const auto*           rowgroup_descriptor = descriptor.m_rowgroup_descriptors()->Get(rowgroup_idx);
	const auto            column_count        = static_cast<idx_t>(rowgroup_descriptor->m_column_descriptors()->size());
	std::vector<uint32_t> base_ids;
	base_ids.reserve(column_ids.size());
	for (idx_t i = 0; i < column_ids.size(); i++) {
		const auto col_idx = column_ids[MultiFileLocalIndex(i)].GetId();
		if (IsFileRowNumberColumn(col_idx)) {
			continue;
		}
		if (col_idx >= column_count) {
			throw InternalException("Projected column index out of range");
		}
		base_ids.emplace_back(static_cast<uint32_t>(col_idx));
	}

	if (HasMccEncoding(*rowgroup_descriptor, base_ids)) {
		ProjectionExpansion expansion;
		expansion.expanded_ids.reserve(column_count);
		for (idx_t col_idx = 0; col_idx < column_count; ++col_idx) {
			expansion.expanded_ids.emplace_back(static_cast<uint32_t>(col_idx));
		}
		expansion.index_map.reserve(expansion.expanded_ids.size());
		for (idx_t idx = 0; idx < expansion.expanded_ids.size(); ++idx) {
			expansion.index_map.emplace(expansion.expanded_ids[idx], idx);
		}
		return expansion;
	}

	std::vector<bool>    seen(column_count, false);
	std::deque<uint32_t> queue;

	for (auto col_id : base_ids) {
		if (!seen[col_id]) {
			seen[col_id] = true;
			queue.push_back(col_id);
		}
	}

	std::vector<uint32_t> expanded_ids = base_ids;
	while (!queue.empty()) {
		const auto col_id = queue.front();
		queue.pop_front();
		const auto* column_descriptor = rowgroup_descriptor->m_column_descriptors()->Get(col_id);
		const auto  dependency        = GetMccDependency(*column_descriptor, column_count);
		if (!dependency.IsValid()) {
			continue;
		}
		const auto dep = static_cast<uint32_t>(dependency.GetIndex());
		if (!seen[dep]) {
			seen[dep] = true;
			expanded_ids.emplace_back(dep);
			queue.push_back(dep);
		}
	}

	ProjectionExpansion expansion;
	expansion.expanded_ids = std::move(expanded_ids);
	expansion.index_map.reserve(expansion.expanded_ids.size());
	for (idx_t idx = 0; idx < expansion.expanded_ids.size(); ++idx) {
		expansion.index_map.emplace(expansion.expanded_ids[idx], idx);
	}
	return expansion;
}

fastlanes::up<fastlanes::RowgroupReader>
FastLanesReader::CreateRowGroupReader(const idx_t rowgroup_idx, const std::vector<uint32_t>& projected_ids) {
	return table_metadata->TableReader().get_rowgroup_reader(rowgroup_idx, projected_ids);
}

const std::vector<idx_t>& FastLanesReader::GetRowGroupsToScan() {
	rowgroup_filter_catalog.Initialize(rowgroup_statistics, filters.get(), column_indexes);
	if (file_row_number_local_idx.IsValid()) {
		rowgroup_filter_catalog.SetRowIdInfo(
		    &row_group_offsets, static_cast<idx_t>(total_tuples), file_row_number_local_idx);
	} else {
		rowgroup_filter_catalog.SetRowIdInfo(nullptr, static_cast<idx_t>(total_tuples), optional_idx());
	}
	return rowgroup_filter_catalog.EnsureRowGroups();
}
} // namespace duckdb
