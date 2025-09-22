#include "reader/row_group_statistics.hpp"
#include "duckdb/common/assert.hpp"
#include "duckdb/common/exception.hpp"
#include <duckdb/common/multi_file/multi_file_data.hpp>
#include <fls/footer/table_descriptor_generated.h>
#include <optional>

namespace duckdb {

template <typename T>
std::optional<T> ReadBinaryAs(const fastlanes::BinaryValueT& binary) {
	if (binary.binary_data.size() < sizeof(T)) {
		return std::nullopt;
	}
	T result;
	std::memcpy(&result, binary.binary_data.data(), sizeof(T));
	return result;
}

RowGroupStatistics::RowGroupStatistics() = default;

void RowGroupStatistics::Initialize(const fastlanes::TableDescriptorT&       table_descriptor,
                                    const vector<MultiFileColumnDefinition>& definitions) {
	const auto rowgroup_count = table_descriptor.m_rowgroup_descriptors.size();
	rowgroup_statistics.clear();
	rowgroup_statistics.resize(rowgroup_count);

	for (idx_t rowgroup_idx = 0; rowgroup_idx < rowgroup_count; ++rowgroup_idx) {
		auto& rowgroup_desc      = *table_descriptor.m_rowgroup_descriptors[rowgroup_idx];
		auto& column_descriptors = rowgroup_desc.m_column_descriptors;
		rowgroup_statistics[rowgroup_idx].resize(column_descriptors.size());

		for (idx_t col_idx = 0; col_idx < column_descriptors.size(); ++col_idx) {
			if (col_idx >= definitions.size()) {
				throw InternalException("RowGroupStatistics: column definitions do not match descriptor size");
			}
			const auto& column_descriptor = *column_descriptors[col_idx];
			const auto& logical_type      = definitions[col_idx].type;
			auto&       statistics_map    = rowgroup_statistics[rowgroup_idx][col_idx];

			for (const auto& statistic_key : {"min", "max"}) {
				statistics_map[statistic_key] = ExtractColumnStatistic(column_descriptor, logical_type, statistic_key);
			}
		}
	}
}

const Value*
RowGroupStatistics::GetStatistic(idx_t rowgroup_idx, idx_t column_idx, const std::string& statistic_key) const {
	if (rowgroup_idx >= rowgroup_statistics.size()) {
		return nullptr;
	}
	const auto& column_statistics = rowgroup_statistics[rowgroup_idx];
	if (column_idx >= column_statistics.size()) {
		return nullptr;
	}
	const auto& statistics_map = column_statistics[column_idx];
	const auto  stat_it        = statistics_map.find(statistic_key);
	if (stat_it == statistics_map.end()) {
		return nullptr;
	}
	return &stat_it->second;
}

Value RowGroupStatistics::ExtractColumnStatistic(const fastlanes::ColumnDescriptorT& column_descriptor,
                                                 const LogicalType&                  logical_type,
                                                 const std::string&                  statistic_key) const {
	const fastlanes::BinaryValueT* statistic_binary = nullptr;
	if (statistic_key == "max") {
		statistic_binary = column_descriptor.max.get();
	} else if (statistic_key == "min") {
		statistic_binary = column_descriptor.min.get();
	} else {
		return Value();
	}

	if (!statistic_binary || statistic_binary->binary_data.empty()) {
		return Value();
	}

	Value base_value;
	switch (column_descriptor.data_type) {
	case fastlanes::DataType::INT8: {
		auto value = ReadBinaryAs<int8_t>(*statistic_binary);
		if (value) {
			base_value = Value::TINYINT(*value);
		}
		break;
	}
	case fastlanes::DataType::INT16: {
		auto value = ReadBinaryAs<int16_t>(*statistic_binary);
		if (value) {
			base_value = Value::SMALLINT(*value);
		}
		break;
	}
	case fastlanes::DataType::INT32: {
		auto value = ReadBinaryAs<int32_t>(*statistic_binary);
		if (value) {
			base_value = Value::INTEGER(*value);
		}
		break;
	}
	case fastlanes::DataType::INT64: {
		auto value = ReadBinaryAs<int64_t>(*statistic_binary);
		if (value) {
			base_value = Value::BIGINT(*value);
		}
		break;
	}
	case fastlanes::DataType::UINT8: {
		auto value = ReadBinaryAs<uint8_t>(*statistic_binary);
		if (value) {
			base_value = Value::UBIGINT(*value);
		}
		break;
	}
	case fastlanes::DataType::UINT16: {
		auto value = ReadBinaryAs<uint16_t>(*statistic_binary);
		if (value) {
			base_value = Value::UBIGINT(*value);
		}
		break;
	}
	case fastlanes::DataType::UINT32: {
		auto value = ReadBinaryAs<uint32_t>(*statistic_binary);
		if (value) {
			base_value = Value::UBIGINT(*value);
		}
		break;
	}
	case fastlanes::DataType::UINT64: {
		auto value = ReadBinaryAs<uint64_t>(*statistic_binary);
		if (value) {
			base_value = Value::UBIGINT(*value);
		}
		break;
	}
	case fastlanes::DataType::DOUBLE: {
		auto value = ReadBinaryAs<double>(*statistic_binary);
		if (value) {
			base_value = Value::DOUBLE(*value);
		}
		break;
	}
	case fastlanes::DataType::FLOAT: {
		auto value = ReadBinaryAs<float>(*statistic_binary);
		if (value) {
			base_value = Value::FLOAT(*value);
		}
		break;
	}
	case fastlanes::DataType::BOOLEAN: {
		auto value = ReadBinaryAs<uint8_t>(*statistic_binary);
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

} // namespace duckdb
