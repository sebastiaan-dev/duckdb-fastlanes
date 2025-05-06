#pragma once

#include "duckdb/common/types.hpp"
#include "fls/footer/rowgroup_descriptor.hpp"

namespace duckdb {

class TranslateUtils {
public:
	static LogicalType TranslateType(fastlanes::DataType type);
};

}
