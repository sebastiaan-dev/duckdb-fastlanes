#include "reader/schema_builder.hpp"
#include "fls/utl/to_str.hpp"
#include <fls/footer/table_descriptor_generated.h>
#include <iostream>
#include <optional>
#include <sstream>
#include <stdexcept>

namespace duckdb {
using fastlanes::DataType;

std::string DataTypeName(const DataType type) {
	return std::string(fastlanes::EnumNameDataType(type));
}

bool IsSignedInteger(const DataType type) {
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

bool IsUnsignedInteger(const DataType type) {
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

bool IsFloatingPoint(const DataType type) {
	return type == DataType::DOUBLE || type == DataType::FLOAT;
}

bool IsStringLike(const DataType type) {
	switch (type) {
	case DataType::STR:
	case DataType::FLS_STR:
	case DataType::FALLBACK:
		return true;
	default:
		return false;
	}
}

int SignedIntegerRank(const DataType type) {
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

int UnsignedIntegerRank(const DataType type) {
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

int StringRank(const DataType type) {
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

bool IsInteger(DataType t) {
	return IsSignedInteger(t) || IsUnsignedInteger(t);
}

int BitWidth(DataType t) {
	switch (t) {
	case DataType::INT8:
	case DataType::UINT8:
		return 8;
	case DataType::INT16:
	case DataType::UINT16:
		return 16;
	case DataType::INT32:
	case DataType::UINT32:
		return 32;
	case DataType::INT64:
	case DataType::UINT64:
		return 64;
	default:
		return -1;
	}
}

std::optional<DataType> SmallestSignedAtLeastBits(int bits) {
	if (bits <= 8)
		return DataType::INT8;
	if (bits <= 16)
		return DataType::INT16;
	if (bits <= 32)
		return DataType::INT32;
	if (bits <= 64)
		return DataType::INT64;
	return std::nullopt;
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
	// TODO: This should probably go from unsigned -> signed only
	if (IsInteger(first) && IsInteger(second)) {
		const DataType signed_type   = IsSignedInteger(first) ? first : second;
		const DataType unsigned_type = IsUnsignedInteger(first) ? first : second;

		const int s_bits = BitWidth(signed_type);
		const int u_bits = BitWidth(unsigned_type);

		const int needed_signed_bits = std::max(s_bits, u_bits + 1);

		if (auto t = SmallestSignedAtLeastBits(needed_signed_bits)) {
			return *t;
		}
		return std::nullopt;
	}

	return std::nullopt;
}

SchemaBuilder::SchemaBuilder(const fastlanes::TableDescriptor& table_descriptor_p, const std::string& file_path_p)
    : table_descriptor(table_descriptor_p)
    , file_path(file_path_p) {
}

SchemaBuildResult SchemaBuilder::Build() const {
	SchemaBuildResult result;

	const auto& rowgroup_descriptors = table_descriptor.m_rowgroup_descriptors();
	if (rowgroup_descriptors->empty()) {
		std::ostringstream err;
		err << "FastLanesReader: no row-groups found in file \"" << file_path << "\"";
		throw std::runtime_error(err.str());
	}

	const auto& first_column_descriptors = rowgroup_descriptors->Get(0)->m_column_descriptors();
	const idx_t column_count             = first_column_descriptors->size();

	result.column_names.resize(column_count);
	result.promoted_types.resize(column_count, DataType::INVALID);

	for (idx_t rowgroup_idx = 0; rowgroup_idx < rowgroup_descriptors->size(); ++rowgroup_idx) {
		const auto& column_descriptors = rowgroup_descriptors->Get(rowgroup_idx)->m_column_descriptors();
		if (column_descriptors->size() != column_count) {
			throw std::runtime_error("FastLanesReader: inconsistent column counts across row groups");
		}

		for (idx_t col_idx = 0; col_idx < column_count; ++col_idx) {
			const auto& column_descriptor = column_descriptors->Get(col_idx);
			const auto  current_type      = column_descriptor->data_type();
			const auto  current_name      = column_descriptor->name()->c_str();

			if (rowgroup_idx == 0) {
				result.column_names[col_idx]   = current_name;
				result.promoted_types[col_idx] = current_type;
				continue;
			}

			if (current_name != result.column_names[col_idx]) {
				std::ostringstream err;
				err << "FastLanesReader: column index " << col_idx << " has name \"" << current_name
				    << "\" but expected \"" << result.column_names[col_idx] << "\"";
				throw std::runtime_error(err.str());
			}

			auto promoted_type = PromoteType(result.promoted_types[col_idx], current_type);
			if (!promoted_type.has_value()) {
				std::ostringstream err;
				err << "FastLanesReader: column \"" << result.column_names[col_idx] << "\" has incompatible types ("
				    << DataTypeName(result.promoted_types[col_idx]) << " vs " << DataTypeName(current_type)
				    << ") across row groups";
				throw std::runtime_error(err.str());
			}

			result.promoted_types[col_idx] = *promoted_type;
		}
	}

	return result;
}

} // namespace duckdb
