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
	stats_.clear();
	stats_.resize(rowgroup_count);

	for (idx_t rowgroup_idx = 0; rowgroup_idx < rowgroup_count; ++rowgroup_idx) {
		auto& rowgroup_desc      = *table_descriptor.m_rowgroup_descriptors[rowgroup_idx];
		auto& column_descriptors = rowgroup_desc.m_column_descriptors;
		stats_[rowgroup_idx].resize(column_descriptors.size());

		for (idx_t col_idx = 0; col_idx < column_descriptors.size(); ++col_idx) {
			if (col_idx >= definitions.size()) {
				throw InternalException("RowGroupStatistics: column definitions do not match descriptor size");
			}
			const auto& column_descriptor = *column_descriptors[col_idx];
			const auto& logical_type      = definitions[col_idx].type;
			auto&       column_statistics = stats_[rowgroup_idx][col_idx];

			for (const auto& statistic_key : {StatKey::Min, StatKey::Max}) {
				const auto val = ExtractColumnStatistic(column_descriptor, logical_type, statistic_key);
				switch (statistic_key) {
				case StatKey::Min: {
					column_statistics.min = val;
					break;
				}
				case StatKey::Max: {
					column_statistics.max = val;
					break;
				}
				default:
					throw InternalException("RowGroupStatistics: unknown statistic");
				}
			}
		}
	}
}

const Value*
RowGroupStatistics::GetStatistic(const idx_t rowgroup_idx, const idx_t column_idx, const StatKey key) const {
	const auto* cs = GetStats(rowgroup_idx, column_idx);
	if (cs == nullptr) {
		return nullptr;
	}

	switch (key) {
	case StatKey::Max: {
		return &cs->max;
	}
	case StatKey::Min: {
		return &cs->min;
	}
	default:
		return nullptr;
	}
}

Value RowGroupStatistics::ExtractColumnStatistic(const fastlanes::ColumnDescriptorT& column_descriptor,
                                                 const LogicalType&                  logical_type,
                                                 const StatKey                       statistic_key) const {
	const fastlanes::BinaryValueT* statistic_binary = nullptr;
	if (statistic_key == StatKey::Max) {
		statistic_binary = column_descriptor.max.get();
	} else if (statistic_key == StatKey::Min) {
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
		if (auto value = ReadBinaryAs<int8_t>(*statistic_binary)) {
			base_value = Value::TINYINT(*value);
		}
		break;
	}
	case fastlanes::DataType::INT16: {
		if (auto value = ReadBinaryAs<int16_t>(*statistic_binary)) {
			base_value = Value::SMALLINT(*value);
		}
		break;
	}
	case fastlanes::DataType::INT32: {
		if (auto value = ReadBinaryAs<int32_t>(*statistic_binary)) {
			base_value = Value::INTEGER(*value);
		}
		break;
	}
	case fastlanes::DataType::INT64: {
		if (auto value = ReadBinaryAs<int64_t>(*statistic_binary)) {
			base_value = Value::BIGINT(*value);
		}
		break;
	}
	case fastlanes::DataType::UINT8: {
		if (auto value = ReadBinaryAs<uint8_t>(*statistic_binary)) {
			base_value = Value::UBIGINT(*value);
		}
		break;
	}
	case fastlanes::DataType::UINT16: {
		if (auto value = ReadBinaryAs<uint16_t>(*statistic_binary)) {
			base_value = Value::UBIGINT(*value);
		}
		break;
	}
	case fastlanes::DataType::UINT32: {
		if (auto value = ReadBinaryAs<uint32_t>(*statistic_binary)) {
			base_value = Value::UBIGINT(*value);
		}
		break;
	}
	case fastlanes::DataType::UINT64: {
		if (auto value = ReadBinaryAs<uint64_t>(*statistic_binary)) {
			base_value = Value::UBIGINT(*value);
		}
		break;
	}
	case fastlanes::DataType::DOUBLE: {
		if (auto value = ReadBinaryAs<double>(*statistic_binary)) {
			base_value = Value::DOUBLE(*value);
		}
		break;
	}
	case fastlanes::DataType::FLOAT: {
		if (auto value = ReadBinaryAs<float>(*statistic_binary)) {
			base_value = Value::FLOAT(*value);
		}
		break;
	}
	case fastlanes::DataType::BOOLEAN: {
		if (auto value = ReadBinaryAs<uint8_t>(*statistic_binary)) {
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

	if (Value casted; base_value.DefaultTryCastAs(logical_type, casted, nullptr)) {
		return casted;
	}
	return Value();
}

} // namespace duckdb
