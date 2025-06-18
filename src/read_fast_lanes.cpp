#include "read_fast_lanes.hpp"

#include <duckdb/main/extension_util.hpp>
#include "duckdb/common/multi_file/multi_file_function.hpp"
#include "fls/common/magic_enum.hpp"
#include "reader/fast_lanes_multi_file_info.hpp"

namespace duckdb {


void ReadFastLanes::Register(DatabaseInstance &db) {
	const MultiFileFunction<FastLanesMultiFileInfo> fn("read_fls");
	// table_function.statistics = MultiFileFunction<FastLanesMultiFileInfo>::MultiFileScanStats;
	// table_function.filter_pushdown = true;
	// table_function.filter_prune = true;
	// table_function.projection_pushdown = true;

	ExtensionUtil::RegisterFunction(db, MultiFileReader::CreateFunctionSet(static_cast<TableFunction>(fn)));
}
} // namespace duckdb