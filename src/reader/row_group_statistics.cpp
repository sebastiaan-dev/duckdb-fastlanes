#include "reader/row_group_statistics.hpp"
#include "duckdb/common/exception.hpp"
#include <duckdb/common/multi_file/multi_file_data.hpp>
#include <fls/footer/table_descriptor_generated.h>
#include <iostream>
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

template <typename T>
void ReadDecimal(Value& base_value, const fastlanes::DecimalType& dtype, const fastlanes::BinaryValueT& binary) {
	const auto width = static_cast<int8_t>(dtype.precision());
	const auto scale = static_cast<int8_t>(dtype.scale());

	if (const auto value = ReadBinaryAs<T>(binary)) {
		base_value = Value::DECIMAL(static_cast<int64_t>(*value), width, scale);
	}
}

template <typename T>
void BinaryIntoValue(Value& base_value, const fastlanes::BinaryValueT& binary, Value (*f)(T)) {
	if (const auto value = ReadBinaryAs<T>(binary)) {
		base_value = f(*value);
	}
}

template <typename T>
void ReadNumericalStats(bool                               is_decimal,
                        ColumnStats&                       column_statistics,
                        const fastlanes::ColumnDescriptor& column_descriptor,
                        Value (*f)(T)) {
	auto&       c_max = column_statistics.max;
	auto&       c_min = column_statistics.min;
	const auto& b_max = *column_descriptor.max()->UnPack();
	const auto& b_min = *column_descriptor.min()->UnPack();

	if (is_decimal) {
		const auto& dtype_info = *column_descriptor.fix_me_decimal_type();

		ReadDecimal<T>(c_max, dtype_info, b_max);
		ReadDecimal<T>(c_min, dtype_info, b_min);
	} else {
		BinaryIntoValue<T>(c_max, b_max, f);
		BinaryIntoValue<T>(c_min, b_min, f);
	}
}

void ExtractStatisticSet(ColumnStats& stats, const fastlanes::ColumnDescriptor& descriptor) {
	const auto is_decimal = descriptor.fix_me_decimal_type() != nullptr;

	switch (descriptor.data_type()) {
	case fastlanes::DataType::INT8: {
		ReadNumericalStats<int8_t>(is_decimal, stats, descriptor, Value::TINYINT);
		break;
	}
	case fastlanes::DataType::INT16: {
		ReadNumericalStats<int16_t>(is_decimal, stats, descriptor, Value::SMALLINT);
		break;
	}
	case fastlanes::DataType::INT32: {
		ReadNumericalStats<int32_t>(is_decimal, stats, descriptor, Value::INTEGER);
		break;
	}
	case fastlanes::DataType::INT64: {
		ReadNumericalStats<int64_t>(is_decimal, stats, descriptor, Value::BIGINT);
		break;
	}
	case fastlanes::DataType::UINT8: {
		ReadNumericalStats<uint8_t>(is_decimal, stats, descriptor, Value::UTINYINT);
		break;
	}
	case fastlanes::DataType::UINT16: {
		ReadNumericalStats<uint16_t>(is_decimal, stats, descriptor, Value::USMALLINT);
		break;
	}
	case fastlanes::DataType::UINT32: {
		ReadNumericalStats<uint32_t>(is_decimal, stats, descriptor, Value::UINTEGER);
		break;
	}
	case fastlanes::DataType::UINT64: {
		ReadNumericalStats<uint64_t>(is_decimal, stats, descriptor, Value::UBIGINT);
		break;
	}
	case fastlanes::DataType::FLOAT: {
		ReadNumericalStats<float>(false, stats, descriptor, Value::FLOAT);
		break;
	}
	case fastlanes::DataType::DOUBLE: {
		ReadNumericalStats<double>(false, stats, descriptor, Value::DOUBLE);
		break;
	}
	case fastlanes::DataType::BOOLEAN: {
		ReadNumericalStats<bool>(false, stats, descriptor, Value::BOOLEAN);
		break;
	}
	case fastlanes::DataType::STR:
	case fastlanes::DataType::LIST:
	case fastlanes::DataType::MAP:
	case fastlanes::DataType::STRUCT:
	case fastlanes::DataType::DECIMAL:
		// Currently decimal is not explicitly set in the column descriptor.
	case fastlanes::DataType::TIMESTAMP:
	case fastlanes::DataType::FALLBACK:
	case fastlanes::DataType::JPEG:
	case fastlanes::DataType::BYTE_ARRAY:
	case fastlanes::DataType::DATE:
	case fastlanes::DataType::FLS_STR: {
		// No statistics.
		break;
	}
	default: {
		std::cout << fastlanes::ToStr(descriptor.data_type()) << " is not a valid data type." << std::endl;
		throw InternalException("ExtractStatisticSet: unknown data type");
	}
	}
}

RowGroupStatistics::RowGroupStatistics() = default;

void RowGroupStatistics::Initialize(const fastlanes::TableDescriptor&        table_descriptor,
                                    const vector<MultiFileColumnDefinition>& definitions) {
	const auto rowgroup_count = table_descriptor.m_rowgroup_descriptors()->size();
	stats_.clear();
	stats_.resize(rowgroup_count);

	for (idx_t rowgroup_idx = 0; rowgroup_idx < rowgroup_count; ++rowgroup_idx) {
		auto&       rowgroup_desc      = *table_descriptor.m_rowgroup_descriptors()->Get(rowgroup_idx);
		const auto& column_descriptors = rowgroup_desc.m_column_descriptors();
		stats_[rowgroup_idx].resize(column_descriptors->size());

		for (idx_t col_idx = 0; col_idx < column_descriptors->size(); ++col_idx) {
			if (col_idx >= definitions.size()) {
				throw InternalException("RowGroupStatistics: column definitions do not match descriptor size");
			}
			const auto& column_descriptor = *column_descriptors->Get(col_idx);
			auto&       column_statistics = stats_[rowgroup_idx][col_idx];

			ExtractStatisticSet(column_statistics, column_descriptor);
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

Value RowGroupStatistics::ExtractColumnStatistic(const fastlanes::ColumnDescriptor& column_descriptor,
                                                 const LogicalType&                 logical_type,
                                                 const StatKey                      statistic_key) {
	const fastlanes::BinaryValueT* statistic_binary = nullptr;
	if (statistic_key == StatKey::Max) {
		statistic_binary = column_descriptor.max()->UnPack();
	} else if (statistic_key == StatKey::Min) {
		statistic_binary = column_descriptor.min()->UnPack();
	} else {
		return Value();
	}

	if (!statistic_binary || statistic_binary->binary_data.empty()) {
		return Value();
	}

	Value base_value;
	if (const auto& dtype = column_descriptor.fix_me_decimal_type()) {
		auto width = static_cast<int8_t>(dtype->precision());
		auto scale = static_cast<int8_t>(dtype->scale());

		switch (column_descriptor.data_type()) {
		case fastlanes::DataType::INT16: {
			if (auto value = ReadBinaryAs<int16_t>(*statistic_binary)) {
				base_value = Value::DECIMAL(*value, width, scale);
			}
			break;
		}
		case fastlanes::DataType::INT32: {
			if (auto value = ReadBinaryAs<int32_t>(*statistic_binary)) {
				base_value = Value::DECIMAL(*value, width, scale);
			}
			break;
		}
		case fastlanes::DataType::INT64: {
			if (auto value = ReadBinaryAs<int64_t>(*statistic_binary)) {
				base_value = Value::DECIMAL(*value, width, scale);
			}
			break;
		}
		case fastlanes::DataType::INT8: {
			if (auto value = ReadBinaryAs<int8_t>(*statistic_binary)) {
				base_value = Value::DECIMAL(static_cast<int64_t>(*value), width, scale);
			}
			break;
		}
		case fastlanes::DataType::UINT8: {
			if (auto value = ReadBinaryAs<uint8_t>(*statistic_binary)) {
				base_value = Value::DECIMAL(static_cast<int64_t>(*value), width, scale);
			}
			break;
		}
		case fastlanes::DataType::UINT16: {
			if (auto value = ReadBinaryAs<uint16_t>(*statistic_binary)) {
				base_value = Value::DECIMAL(static_cast<int64_t>(*value), width, scale);
			}
			break;
		}
		case fastlanes::DataType::UINT32: {
			if (auto value = ReadBinaryAs<uint32_t>(*statistic_binary)) {
				base_value = Value::DECIMAL(static_cast<int64_t>(*value), width, scale);
			}
			break;
		}
		case fastlanes::DataType::UINT64: {
			if (auto value = ReadBinaryAs<uint64_t>(*statistic_binary)) {
				base_value = Value::DECIMAL(static_cast<int64_t>(*value), width, scale);
			}
			break;
		}
		default:
			throw InternalException("RowGroupStatistics: cannot parse physical type into decimal.");
		}
	} else {
		switch (column_descriptor.data_type()) {
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
