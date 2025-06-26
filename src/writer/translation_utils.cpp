#include "writer/translation_utils.hpp"

namespace duckdb {

fastlanes::DataType WriterTranslateUtils::TranslateType(const LogicalType &type) {
	// TODO: Check completeness
	switch (type.id()) {
	case LogicalTypeId::DOUBLE:
		return fastlanes::DataType::DOUBLE;
	case LogicalTypeId::FLOAT:
		return fastlanes::DataType::FLOAT;
	case LogicalTypeId::TINYINT:
		return fastlanes::DataType::INT8;
	case LogicalTypeId::SMALLINT:
		return fastlanes::DataType::INT16;
	case LogicalTypeId::INTEGER:
		return fastlanes::DataType::INT32;
	case LogicalTypeId::BIGINT:
		return fastlanes::DataType::INT64;
	case LogicalTypeId::UTINYINT:
		return fastlanes::DataType::UINT8;
	case LogicalTypeId::USMALLINT:
		return fastlanes::DataType::UINT16;
	case LogicalTypeId::UINTEGER:
		return fastlanes::DataType::UINT32;
	case LogicalTypeId::UBIGINT:
		return fastlanes::DataType::UINT64;
	case LogicalTypeId::VARCHAR:
	case LogicalTypeId::CHAR:
		// FIXME: DataType::STR does not work, this causes a crash as TypedStats in the TypedCol initialises to a
		// nullptr.
		return fastlanes::DataType::FLS_STR;
	case LogicalTypeId::BOOLEAN:
		return fastlanes::DataType::BOOLEAN;
	case LogicalTypeId::DATE:
		return fastlanes::DataType::DATE;
	case LogicalTypeId::BLOB:
		return fastlanes::DataType::BYTE_ARRAY;
	case LogicalTypeId::LIST:
		return fastlanes::DataType::LIST;
	case LogicalTypeId::STRUCT:
		return fastlanes::DataType::STRUCT;
	case LogicalTypeId::MAP:
		return fastlanes::DataType::MAP;
	default:
		// TODO: Use fallback?
		return fastlanes::DataType::INVALID;
	}
}

} // namespace duckdb
