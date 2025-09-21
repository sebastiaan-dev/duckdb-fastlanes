#define private public
#include "fls/reader/table_reader.hpp"
#undef private
#include "duckdb/common/exception.hpp"
#include "duckdb/common/value_operations/value_operations.hpp"
#include "duckdb/planner/filter/constant_filter.hpp"
#include "fls/footer/datatype_generated.h"
#include "fls/footer/footer_generated.h"
#include "reader/fls_reader.hpp"
#include "reader/materializer.hpp"
#include "reader/translation_utils.hpp"
#include <atomic>
#include <cstring>
#include <iostream>
#include <limits>
#include <mutex>
#include <numeric>
#include <optional>
#include <sstream>
#include <vector>

namespace duckdb {

namespace {

using fastlanes::DataType;

template <typename T>
std::optional<T> ReadBinaryAs(const fastlanes::BinaryValueT& binary) {
	if (binary.binary_data.size() < sizeof(T)) {
		return std::nullopt;
	}
	T result;
	std::memcpy(&result, binary.binary_data.data(), sizeof(T));
	return result;
}

std::string DataTypeName(DataType type) {
	return std::string(fastlanes::EnumNameDataType(type));
}

bool IsSignedInteger(DataType type) {
	switch (type) {
	case DataType::INT8:
	case DataType::INT16:
	case DataType::INT32:
	case DataType::INT64:
		return true;
	default:
		return false;
	}
}

bool IsUnsignedInteger(DataType type) {
	switch (type) {
	case DataType::UINT8:
	case DataType::UINT16:
	case DataType::UINT32:
	case DataType::UINT64:
		return true;
	default:
		return false;
	}
}

bool IsFloatingPoint(DataType type) {
	return type == DataType::DOUBLE || type == DataType::FLOAT;
}

bool IsStringLike(DataType type) {
	switch (type) {
	case DataType::STR:
	case DataType::FLS_STR:
	case DataType::FALLBACK:
		return true;
	default:
		return false;
	}
}

int SignedIntegerRank(DataType type) {
	switch (type) {
	case DataType::INT8:
		return 1;
	case DataType::INT16:
		return 2;
	case DataType::INT32:
		return 3;
	case DataType::INT64:
		return 4;
	default:
		return -1;
	}
}

int UnsignedIntegerRank(DataType type) {
	switch (type) {
	case DataType::UINT8:
		return 1;
	case DataType::UINT16:
		return 2;
	case DataType::UINT32:
		return 3;
	case DataType::UINT64:
		return 4;
	default:
		return -1;
	}
}

int StringRank(DataType type) {
	switch (type) {
	case DataType::STR:
	case DataType::FLS_STR:
		return 1;
	case DataType::FALLBACK:
		return 2;
	default:
		return -1;
	}
}

std::optional<DataType> PromoteType(DataType first, DataType second) {
	if (first == DataType::INVALID) {
		return second == DataType::INVALID ? std::nullopt : std::optional<DataType>(second);
	}
	if (second == DataType::INVALID) {
		return std::nullopt;
	}
	if (first == second) {
		return first;
	}

	if (IsSignedInteger(first) && IsSignedInteger(second)) {
		return SignedIntegerRank(first) >= SignedIntegerRank(second) ? first : second;
	}
	if (IsUnsignedInteger(first) && IsUnsignedInteger(second)) {
		return UnsignedIntegerRank(first) >= UnsignedIntegerRank(second) ? first : second;
	}
	if (IsFloatingPoint(first) && IsFloatingPoint(second)) {
		return first == DataType::DOUBLE || second == DataType::DOUBLE ? DataType::DOUBLE : DataType::FLOAT;
	}
	if (IsStringLike(first) && IsStringLike(second)) {
		return StringRank(first) >= StringRank(second) ? first : second;
	}

	return std::nullopt;
}

} // namespace

FastLanesReader::FastLanesReader(OpenFileInfo file_p)
    : BaseFileReader(std::move(file_p))
    , vectors_read(0) {
	D_ASSERT(StringUtil::EndsWith(file.path, ".fls"));

	std::filesystem::path full_path = file.path;
	table_reader                    = make_uniq<fastlanes::TableReader>(full_path, conn);

	const fastlanes::TableDescriptorT& table_metadata = *table_reader->m_table_descriptor;
	if (table_metadata.m_rowgroup_descriptors.empty()) {
		throw std::runtime_error("FastLanesReader: no row-groups found in file \"" + file.path + "\"");
	}

	const auto& rowgroup_descriptors     = table_metadata.m_rowgroup_descriptors;
	const auto& first_column_descriptors = rowgroup_descriptors[0]->m_column_descriptors;
	const idx_t column_count             = first_column_descriptors.size();

	std::vector<DataType>    promoted_types(column_count, DataType::INVALID);
	std::vector<std::string> column_names(column_count);

	for (idx_t rowgroup_idx = 0; rowgroup_idx < rowgroup_descriptors.size(); ++rowgroup_idx) {
		const auto& column_descriptors = rowgroup_descriptors[rowgroup_idx]->m_column_descriptors;
		if (column_descriptors.size() != column_count) {
			throw std::runtime_error("FastLanesReader: inconsistent column counts across row groups");
		}

		for (idx_t col_idx = 0; col_idx < column_count; ++col_idx) {
			const auto& column_descriptor = column_descriptors[col_idx];
			const auto  current_type      = column_descriptor->data_type;

			if (rowgroup_idx == 0) {
				column_names[col_idx]   = column_descriptor->name;
				promoted_types[col_idx] = current_type;
				continue;
			}

			if (column_descriptor->name != column_names[col_idx]) {
				std::ostringstream err;
				err << "FastLanesReader: column index " << col_idx << " has name \"" << column_descriptor->name
				    << "\" but expected \"" << column_names[col_idx] << "\"";
				throw std::runtime_error(err.str());
			}

			auto promoted_type = PromoteType(promoted_types[col_idx], current_type);
			if (!promoted_type.has_value()) {
				std::ostringstream err;
				err << "FastLanesReader: column \"" << column_names[col_idx] << "\" has incompatible types ("
				    << DataTypeName(promoted_types[col_idx]) << " vs " << DataTypeName(current_type)
				    << ") across row groups";
				throw std::runtime_error(err.str());
			}

			promoted_types[col_idx] = *promoted_type;
		}
	}

	columns.reserve(column_count);
	for (idx_t col_idx = 0; col_idx < column_count; ++col_idx) {
		auto type = TranslateUtils::TranslateType(promoted_types[col_idx]);
		columns.emplace_back(column_names[col_idx], type);
	}

	InitializeRowGroupStats();
}

FastLanesReader::~FastLanesReader() {
}

idx_t FastLanesReader::GetNRowGroups() const {
	const fastlanes::TableDescriptorT& table = *table_reader->m_table_descriptor;
	return table.m_rowgroup_descriptors.size();
}

idx_t FastLanesReader::GetNVectors(const idx_t row_group_idx) const {
	const fastlanes::TableDescriptorT& table           = *table_reader->m_table_descriptor;
	auto&                              row_descriptors = table.m_rowgroup_descriptors;

	D_ASSERT(row_group_idx < row_descriptors.size());

	return row_descriptors[row_group_idx]->m_n_vec;
}

idx_t FastLanesReader::GetNTuples(const idx_t row_group_idx) const {
	const fastlanes::TableDescriptorT& table           = *table_reader->m_table_descriptor;
	auto&                              row_descriptors = table.m_rowgroup_descriptors;

	D_ASSERT(row_group_idx < row_descriptors.size());

	return row_descriptors[row_group_idx]->m_n_tuples;
}

idx_t FastLanesReader::GetTotalTuples() const {
	const fastlanes::TableDescriptorT& table = *table_reader->m_table_descriptor;

	idx_t total_n_tuples = 0;
	for (auto& row_group_descriptor : table.m_rowgroup_descriptors) {
		total_n_tuples += row_group_descriptor->m_n_tuples;
	}
	return total_n_tuples;
}

fastlanes::up<fastlanes::RowgroupReader> FastLanesReader::CreateRowGroupReader(const idx_t rowgroup_idx) {
	std::vector<uint32_t> projected_ids;
	projected_ids.reserve(column_ids.size());

	for (idx_t i = 0; i < column_ids.size(); i++) {
		const auto col_idx = column_ids[MultiFileLocalIndex(i)].GetId();
		projected_ids.emplace_back(col_idx);
	}
	return table_reader->get_rowgroup_reader(rowgroup_idx, projected_ids);
}

void FastLanesReader::InitializeRowGroupStats() {
	const auto& table_metadata = *table_reader->m_table_descriptor;
	const idx_t rowgroup_count = table_metadata.m_rowgroup_descriptors.size();
	rowgroup_max_values.clear();
	rowgroup_max_values.resize(rowgroup_count);

	for (idx_t rowgroup_idx = 0; rowgroup_idx < rowgroup_count; ++rowgroup_idx) {
		auto& rowgroup_desc      = *table_metadata.m_rowgroup_descriptors[rowgroup_idx];
		auto& column_descriptors = rowgroup_desc.m_column_descriptors;
		rowgroup_max_values[rowgroup_idx].resize(column_descriptors.size());

		for (idx_t col_idx = 0; col_idx < column_descriptors.size(); ++col_idx) {
			const auto& column_descriptor = *column_descriptors[col_idx];
			const auto& logical_type      = col_idx < columns.size() ? columns[col_idx].type : LogicalType::SQLNULL;
			rowgroup_max_values[rowgroup_idx][col_idx] = ExtractMaxValue(column_descriptor, logical_type);
		}
	}
}

Value FastLanesReader::ExtractMaxValue(const fastlanes::ColumnDescriptorT& column_descriptor,
                                       const LogicalType&                  logical_type) const {
	if (!column_descriptor.max) {
		return Value();
	}

	const auto& binary = column_descriptor.max->binary_data;
	if (binary.empty()) {
		return Value();
	}

	Value base_value;
	switch (column_descriptor.data_type) {
	case fastlanes::DataType::INT8: {
		auto value = ReadBinaryAs<int8_t>(*column_descriptor.max);
		if (value) {
			base_value = Value::TINYINT(*value);
		}
		break;
	}
	case fastlanes::DataType::INT16: {
		auto value = ReadBinaryAs<int16_t>(*column_descriptor.max);
		if (value) {
			base_value = Value::SMALLINT(*value);
		}
		break;
	}
	case fastlanes::DataType::INT32: {
		auto value = ReadBinaryAs<int32_t>(*column_descriptor.max);
		if (value) {
			base_value = Value::INTEGER(*value);
		}
		break;
	}
	case fastlanes::DataType::INT64: {
		auto value = ReadBinaryAs<int64_t>(*column_descriptor.max);
		if (value) {
			base_value = Value::BIGINT(*value);
		}
		break;
	}
	case fastlanes::DataType::UINT8: {
		auto value = ReadBinaryAs<uint8_t>(*column_descriptor.max);
		if (value) {
			base_value = Value::UBIGINT(*value);
		}
		break;
	}
	case fastlanes::DataType::UINT16: {
		auto value = ReadBinaryAs<uint16_t>(*column_descriptor.max);
		if (value) {
			base_value = Value::UBIGINT(*value);
		}
		break;
	}
	case fastlanes::DataType::UINT32: {
		auto value = ReadBinaryAs<uint32_t>(*column_descriptor.max);
		if (value) {
			base_value = Value::UBIGINT(*value);
		}
		break;
	}
	case fastlanes::DataType::UINT64: {
		auto value = ReadBinaryAs<uint64_t>(*column_descriptor.max);
		if (value) {
			base_value = Value::UBIGINT(*value);
		}
		break;
	}
	case fastlanes::DataType::DOUBLE: {
		auto value = ReadBinaryAs<double>(*column_descriptor.max);
		if (value) {
			base_value = Value::DOUBLE(*value);
		}
		break;
	}
	case fastlanes::DataType::FLOAT: {
		auto value = ReadBinaryAs<float>(*column_descriptor.max);
		if (value) {
			base_value = Value::FLOAT(*value);
		}
		break;
	}
	case fastlanes::DataType::BOOLEAN: {
		auto value = ReadBinaryAs<uint8_t>(*column_descriptor.max);
		if (value) {
			base_value = Value::BOOLEAN(*value != 0);
		}
		break;
	}
	default:
		return Value();
	}

	if (base_value.IsNull()) {
		return Value();
	}

	Value casted;
	if (base_value.DefaultTryCastAs(logical_type, casted, nullptr)) {
		return casted;
	}
	return Value();
}

void FastLanesReader::EnsureRowGroupFilterState() {
	std::lock_guard<std::mutex> guard(rowgroup_filter_lock);
	if (rowgroup_filters_ready.load()) {
		return;
	}
	BuildRowGroupFilterList();
	rowgroup_filters_ready.store(true);
}

void FastLanesReader::BuildRowGroupFilterList() {
	rowgroups_to_scan.clear();
	const idx_t total = GetNRowGroups();
	if (!filters || filters->filters.empty()) {
		rowgroups_to_scan.resize(total);
		std::iota(rowgroups_to_scan.begin(), rowgroups_to_scan.end(), idx_t(0));
		return;
	}

	rowgroups_to_scan.reserve(total);
	for (idx_t rowgroup_idx = 0; rowgroup_idx < total; ++rowgroup_idx) {
		if (RowGroupMaySatisfyFilters(rowgroup_idx)) {
			rowgroups_to_scan.push_back(rowgroup_idx);
		}
	}
}

bool FastLanesReader::RowGroupMaySatisfyFilters(idx_t rowgroup_idx) {
	if (!filters || filters->filters.empty()) {
		return true;
	}
	if (rowgroup_idx >= rowgroup_max_values.size()) {
		throw std::runtime_error("rowgroup_max_values out of range");
	}

	const auto& max_values = rowgroup_max_values[rowgroup_idx];
	for (auto& entry : filters->filters) {
		const idx_t local_column_id = entry.first;
		auto&       filter          = *entry.second;

		if (filter.filter_type != TableFilterType::CONSTANT_COMPARISON) {
			continue;
		}
		auto& constant_filter = filter.Cast<ConstantFilter>();
		auto  comparison_type = constant_filter.comparison_type;
		if (comparison_type != ExpressionType::COMPARE_GREATERTHAN &&
		    comparison_type != ExpressionType::COMPARE_GREATERTHANOREQUALTO) {
			continue;
		}
		if (local_column_id >= column_indexes.size()) {
			continue;
		}
		const idx_t primary_index = column_indexes[local_column_id].GetPrimaryIndex();
		if (primary_index >= max_values.size()) {
			continue;
		}

		const auto& max_value = max_values[primary_index];
		const auto& constant  = constant_filter.constant;
		if (max_value.IsNull() || constant.IsNull()) {
			continue;
		}

		Value max_casted;
		Value constant_casted;
		if (max_value.DefaultTryCastAs(constant.type(), max_casted, nullptr)) {
			constant_casted = constant;
		} else if (constant.DefaultTryCastAs(max_value.type(), constant_casted, nullptr)) {
			max_casted = max_value;
		} else {
			continue;
		}

		bool skip_rowgroup = false;
		switch (comparison_type) {
		case ExpressionType::COMPARE_GREATERTHAN:
			skip_rowgroup = ValueOperations::LessThanEquals(max_casted, constant_casted);
			break;
		case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
			skip_rowgroup = ValueOperations::LessThan(max_casted, constant_casted);
			break;
		default:
			break;
		}
		if (skip_rowgroup) {
			return false;
		}
	}
	return true;
}

} // namespace duckdb
