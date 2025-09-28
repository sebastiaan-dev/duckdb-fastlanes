#pragma once

#include "duckdb/common/types.hpp"
#include "fls/footer/rowgroup_descriptor.hpp"

namespace duckdb {

class WriterTranslateUtils {
public:
	/**
	 * @brief Translates a DuckDB LogicalType to a FastLanes DataType
	 *
	 * @param type The DuckDB LogicalType to translate
	 * @return fastlanes::DataType The corresponding FastLanes DataType
	 */
	static fastlanes::DataType TranslateType(const LogicalType &type);
};

} // namespace duckdb