#define private public
#include "fls/reader/table_reader.hpp"
#undef private
#include "fls/footer/datatype_generated.h"
#include "reader/fls_reader.hpp"
#include "reader/materializer.hpp"
#include "reader/translation_utils.hpp"
#include <iostream>
#include <optional>
#include <sstream>
#include <vector>

namespace duckdb {

namespace {

using fastlanes::DataType;

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
    : BaseFileReader(std::move(file_p)) {
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
	return table_reader->get_rowgroup_reader(rowgroup_idx);
}

} // namespace duckdb
