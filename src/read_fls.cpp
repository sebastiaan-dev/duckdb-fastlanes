#include "read_fls.hpp"
#include "duckdb/common/multi_file/multi_file_function.hpp"
#include "fls/common/magic_enum.hpp"
#include "reader/fls_multi_file_info.hpp"
#include <duckdb/main/extension_util.hpp>

namespace duckdb {

void ReadFastLanes::Register(DatabaseInstance& db) {
	MultiFileFunction<FastLanesMultiFileInfo> fn("read_fls");
	// fn.statistics = MultiFileFunction<FastLanesMultiFileInfo>::MultiFileScanStats;
	fn.filter_pushdown = true;
	fn.filter_prune    = true;

	ExtensionUtil::RegisterFunction(db, MultiFileReader::CreateFunctionSet(static_cast<TableFunction>(fn)));
}
} // namespace duckdb
