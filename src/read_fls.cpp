#include "read_fls.hpp"
#include "duckdb/common/multi_file/multi_file_function.hpp"
#include "fls/common/magic_enum.hpp"
#include "reader/fls_multi_file_info.hpp"
#include <duckdb/main/extension_util.hpp>

namespace duckdb {

static bool FastLanesScanPushdownExpression(ClientContext& context, const LogicalGet& get, Expression& expr) {
	return true;
}

vector<column_t> FastLanesGetRowIdColumns(ClientContext& context, optional_ptr<FunctionData> bind_data) {
	vector<column_t> result;
	result.emplace_back(MultiFileReader::COLUMN_IDENTIFIER_FILE_INDEX);
	result.emplace_back(MultiFileReader::COLUMN_IDENTIFIER_FILE_ROW_NUMBER);
	return result;
}

void ReadFastLanes::Register(DatabaseInstance& db) {
	MultiFileFunction<FastLanesMultiFileInfo> fn("read_fls");
	fn.named_parameters["explicit_cardinality"] = LogicalType::UBIGINT;
	fn.named_parameters["file_row_number"]      = LogicalType::BOOLEAN;

	// fn.statistics = MultiFileFunction<FastLanesMultiFileInfo>::MultiFileScanStats;
	fn.get_row_id_columns   = FastLanesGetRowIdColumns;
	fn.pushdown_expression  = FastLanesScanPushdownExpression;
	fn.filter_pushdown      = true;
	fn.filter_prune         = true;
	fn.late_materialization = false;

	ExtensionUtil::RegisterFunction(db, MultiFileReader::CreateFunctionSet(static_cast<TableFunction>(fn)));
}
} // namespace duckdb
