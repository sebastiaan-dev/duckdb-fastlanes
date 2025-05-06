#include "reader/translation_utils.hpp"

namespace duckdb {

LogicalType TranslateUtils::TranslateType(const fastlanes::DataType type) {
	switch (type) {
	case fastlanes::DataType::DOUBLE:
		return LogicalType::DOUBLE;
	case fastlanes::DataType::FLOAT:
		return LogicalType::FLOAT;
	case fastlanes::DataType::INT8:
		return LogicalType::TINYINT;
	case fastlanes::DataType::INT16:
		return LogicalType::SMALLINT;
	case fastlanes::DataType::INT32:
		return LogicalType::INTEGER;
	case fastlanes::DataType::INT64:
		return LogicalType::BIGINT;
	case fastlanes::DataType::UINT8:
	case fastlanes::DataType::UINT16:
	case fastlanes::DataType::UINT32:
	case fastlanes::DataType::UINT64:
		// Unsigned types map to DuckDB's unsigned bigint
		return LogicalType::UBIGINT;
	case fastlanes::DataType::STR:
	case fastlanes::DataType::FLS_STR:
		return LogicalType::VARCHAR;
	case fastlanes::DataType::BOOLEAN:
		return LogicalType::BOOLEAN;
	case fastlanes::DataType::DATE:
		return LogicalType::DATE;
	case fastlanes::DataType::BYTE_ARRAY:
		return LogicalType::BLOB;
	case fastlanes::DataType::LIST:
		return LogicalType::LIST(LogicalType::SQLNULL);
	case fastlanes::DataType::STRUCT:
		return LogicalType::STRUCT({});
	case fastlanes::DataType::MAP:
		return LogicalType::MAP(LogicalType::SQLNULL, LogicalType::SQLNULL);
	case fastlanes::DataType::FALLBACK:
		return LogicalType::VARCHAR;
	case fastlanes::DataType::INVALID:
	default:
		throw InternalException("TranslateType: column type is not supported");
	}
}

} // namespace duckdb