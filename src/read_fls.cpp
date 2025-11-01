#include "read_fls.hpp"
#include "duckdb/common/multi_file/multi_file_function.hpp"
#include "fls/common/magic_enum.hpp"
#include "reader/fls_multi_file_info.hpp"
#include "reader/fls_reader.hpp"

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

vector<PartitionStatistics> FastLanesGetPartitionStats(ClientContext& context, GetPartitionStatsInput& input) {
	auto&                       bind_data = input.bind_data->Cast<MultiFileBindData>();
	vector<PartitionStatistics> result;

	if (bind_data.file_list->GetExpandResult() == FileExpandResult::SINGLE_FILE && bind_data.initial_reader) {
		const auto& reader = bind_data.initial_reader->Cast<FastLanesReader>();
		reader.GetPartitionStats(result);
	}
	// TODO: Implement cache to support multiple files.
	return result;
}

void ReadFastLanes::Register(ExtensionLoader& loader) {
	MultiFileFunction<FastLanesMultiFileInfo> fn("read_fls");
	fn.named_parameters["explicit_cardinality"] = LogicalType::UBIGINT;
	fn.named_parameters["file_row_number"]      = LogicalType::BOOLEAN;

	fn.statistics           = MultiFileFunction<FastLanesMultiFileInfo>::MultiFileScanStats;
	fn.get_row_id_columns   = FastLanesGetRowIdColumns;
	fn.pushdown_expression  = FastLanesScanPushdownExpression;
	fn.get_partition_stats  = FastLanesGetPartitionStats;
	fn.filter_pushdown      = true;
	fn.filter_prune         = true;
	fn.late_materialization = false;

	loader.RegisterFunction(MultiFileReader::CreateFunctionSet(static_cast<TableFunction>(fn)));
}
} // namespace duckdb
