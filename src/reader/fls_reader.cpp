#include "reader/fls_reader.hpp"
#include "duckdb/common/constants.hpp"
#include "duckdb/common/exception.hpp"
#include "fls/expression/physical_expression.hpp"
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

void FastLanesReader::GetPartitionStats(vector<PartitionStatistics>& result) const {
	// TODO: How do we deal with row group filtering? (Potential FIXME)
	const auto n_row_groups = table_metadata->RowGroupCount();
	result.reserve(n_row_groups);

	idx_t offset = 0;
	for (idx_t rowgroup_idx {0}; rowgroup_idx < n_row_groups; ++rowgroup_idx) {
		auto& rg_descriptor = table_metadata->RowGroupDescriptor(rowgroup_idx);

		PartitionStatistics partition_stats;
		partition_stats.row_start  = offset;
		partition_stats.count      = rg_descriptor.m_n_tuples();
		partition_stats.count_type = CountType::COUNT_EXACT;
		offset += partition_stats.count;
		result.emplace_back(partition_stats);
	}
}

void FastLanesReader::PrepareReader(ClientContext& context, GlobalTableFunctionState&) {
}

bool FastLanesReader::TryInitializeScan(ClientContext&            ctx,
                                        GlobalTableFunctionState& global_state_p,
                                        LocalTableFunctionState&  local_state_p) {
	auto& global_state = global_state_p.Cast<FastLanesReadGlobalState>();
	auto& local_state  = local_state_p.Cast<FastLanesReadLocalState>();

	const auto& rowgroups_to_scan       = GetRowGroupsToScan();
	const auto  selected_rowgroup_count = rowgroups_to_scan.size();
	if (global_state.next_rowgroup >= selected_rowgroup_count) {
		// If the index of the next row group falls outside the range of the filtered rowgroup vector we are done.
		return false;
	}

	// Prepare the local state of the current worker by assigning it a row group.
	local_state.cur_rowgroup          = rowgroups_to_scan[global_state.next_rowgroup];
	auto expansion                    = ExpandProjectedColumns(local_state.cur_rowgroup);
	local_state.expanded_column_ids   = expansion.expanded_ids;
	local_state.expanded_column_index = std::move(expansion.index_map);
	local_state.row_group_reader      = CreateRowGroupReader(local_state.cur_rowgroup, local_state.expanded_column_ids);
	local_state.row_group_base        = row_group_offsets[local_state.cur_rowgroup];
	// Reset the row group related local state.
	local_state.cur_vector = 0;
	local_state.n_vectors  = GetNTuples(local_state.cur_rowgroup);

	// Filters are equal across a file, only construct them once, if they exist.
	if (filters && !local_state.adaptive_filter && local_state.scan_filters.empty()) {
		local_state.adaptive_filter = make_uniq<AdaptiveFilter>(*filters);
		for (auto& [fst, snd] : filters->filters) {
			local_state.scan_filters.emplace_back(ctx, fst, *snd);
		}

		local_state.filters_by_col.assign(column_ids.size(), {});
		for (auto& f : local_state.scan_filters) {
			if (const idx_t col = f.filter_idx; col < column_ids.size()) {
				local_state.filters_by_col[col].push_back(&f);
			}
		}
	}

	if (filters) {
		for (auto& filter : local_state.scan_filters) {
			filter.ResetForRowGroup();
		}
	}

	local_state.physical_projection_map.clear();
	local_state.physical_projection_map.resize(column_ids.size());
	idx_t physical_projection_count = local_state.expanded_column_ids.size();
	for (idx_t i {0}; i < column_ids.size(); ++i) {
		const auto local_column_id = column_ids[MultiFileLocalIndex(i)].GetId();
		if (IsFileRowNumberColumn(local_column_id)) {
			continue;
		}
		auto it = local_state.expanded_column_index.find(static_cast<uint32_t>(local_column_id));
		if (it == local_state.expanded_column_index.end()) {
			throw InternalException("Missing projected column in expanded projection");
		}
		local_state.physical_projection_map[i] = optional_idx(it->second);
	}
	if (local_state.column_decoders.size() != column_ids.size()) {
		local_state.column_decoders.clear();
		local_state.column_decoders.resize(column_ids.size());
		for (idx_t i {0}; i < column_ids.size(); ++i) {
			if (local_state.physical_projection_map[i].IsValid()) {
				local_state.column_decoders[i] = make_uniq<materializer::ColumnDecoder>();
			}
		}
	} else {
		for (auto& decoder : local_state.column_decoders) {
			if (decoder) {
				decoder->Reset();
			}
		}
	}

	if (physical_projection_count > 0) {
		const auto& expressions = local_state.row_group_reader->get_chunk(0);
		for (idx_t i {0}; i < column_ids.size(); ++i) {
			auto physical_index = local_state.physical_projection_map[i];
			if (!physical_index.IsValid()) {
				continue;
			}
			auto&      type = columns[column_ids[MultiFileLocalIndex(i)].GetId()].type;
			const auto expr = expressions[physical_index.GetIndex()];
			expr->PointTo(0);
			auto& op = expr->operators[expr->operators.size() - 1];
			if (filters) {
				local_state.column_decoders[i]->Init(op, type, &local_state.filters_by_col[i]);
			} else {
				local_state.column_decoders[i]->Init(op, type, nullptr);
			}
		}
	}

	// Mark the row group as consumed.
	global_state.next_rowgroup++;

	return true;
}

void FastLanesReader::Scan(ClientContext&            context,
                           GlobalTableFunctionState& global_state_p,
                           LocalTableFunctionState&  local_state_p,
                           DataChunk&                chunk) {
	auto& local_state = local_state_p.Cast<FastLanesReadLocalState>();

	while (true) {
		const auto  cur_vec     = local_state.cur_vector;
		const idx_t start_tuple = cur_vec * fastlanes::CFG::VEC_SZ;
		if (start_tuple >= local_state.n_vectors) {
			return;
		}

		const idx_t tuples_left = local_state.n_vectors - start_tuple;
		const idx_t count       = std::min(tuples_left, fastlanes::CFG::VEC_SZ * 2);
		const auto  n_vectors   = (count + fastlanes::CFG::VEC_SZ - 1) / fastlanes::CFG::VEC_SZ;
		if (!n_vectors) {
			return;
		}

		const bool has_physical_columns = std::any_of(local_state.physical_projection_map.begin(),
		                                              local_state.physical_projection_map.end(),
		                                              [](const optional_idx& entry) { return entry.IsValid(); });

		// Try to fill up to the standard vector size.
		for (idx_t batch_idx = 0; batch_idx < n_vectors; batch_idx++) {
			const idx_t vector_idx = cur_vec + batch_idx;
			if (has_physical_columns) {
				const auto& rowgroup_descriptor = table_metadata->RowGroupDescriptor(local_state.cur_rowgroup);
				const auto& expressions         = local_state.row_group_reader->get_chunk(vector_idx);
				for (idx_t i = 0; i < column_ids.size(); i++) {
					auto physical_entry = local_state.physical_projection_map[i];
					if (!physical_entry.IsValid()) {
						continue;
					}
					auto&      target_col = chunk.data[i];
					const auto expr       = expressions[physical_entry.GetIndex()];

					expr->PointTo(vector_idx);
					auto& op = expr->operators[expr->operators.size() - 1];

					const auto& src_type = rowgroup_descriptor.m_column_descriptors()
					                           ->Get(column_ids[MultiFileLocalIndex(i)].GetId())
					                           ->data_type();

					if (batch_idx == 0) {
						local_state.column_decoders[i]->Decode<materializer::Pass::First>(
						    op, src_type, target_col, vector_idx);
					} else {
						local_state.column_decoders[i]->Decode<materializer::Pass::Second>(
						    op, src_type, target_col, vector_idx);
					}
				}
			}
		}

		const idx_t row_start = local_state.row_group_base + start_tuple;
		for (idx_t i = 0; i < column_ids.size(); i++) {
			if (const auto local_column_id = column_ids[MultiFileLocalIndex(i)].GetId();
			    !IsFileRowNumberColumn(local_column_id)) {
				continue;
			}
			auto& target_col = chunk.data[i];
			// TODO: This can probably be removed.
			target_col.SetVectorType(VectorType::FLAT_VECTOR);
			FlatVector::Validity(target_col).Reset();
			const auto data = FlatVector::GetData<int64_t>(target_col);
			for (idx_t row = 0; row < count; ++row) {
				data[row] = static_cast<int64_t>(row_start + row);
			}
		}

		chunk.SetCardinality(count);

		FilterExecutor::Apply(chunk, local_state.adaptive_filter, local_state.scan_filters, filters.get());

		local_state.cur_vector += n_vectors;
		vectors_read += n_vectors;

		if (chunk.size() == 0) {
			chunk.Reset();
			continue;
		}

		return;
	}
}

void FastLanesReader::FinishFile(ClientContext& context, GlobalTableFunctionState& global_state_p) {
	auto& g_state = global_state_p.Cast<FastLanesReadGlobalState>();

	// Reset progression trackers of the current file.
	g_state.next_rowgroup = 0;
}

double FastLanesReader::GetProgressInFile(ClientContext& context) {
	return 100.0 * (static_cast<double>(vectors_read.load()) / static_cast<double>(GetTotalVectors()));
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
