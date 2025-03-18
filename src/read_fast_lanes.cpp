#include "read_fast_lanes.hpp"

#include <duckdb/main/extension_util.hpp>
#include <fls/connection.hpp>
#include <fls/encoder/materializer.hpp>
#include <sys/param.h>

namespace duckdb {

//-------------------------------------------------------------------
// Translations
//-------------------------------------------------------------------
static LogicalType TranslateType(const fastlanes::DataType type) {
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
		// TODO
		return LogicalType::LIST(LogicalType::SQLNULL);
	case fastlanes::DataType::STRUCT:
		// TODO
		return LogicalType::STRUCT({});
	case fastlanes::DataType::MAP:
		// TODO
		return LogicalType::MAP(LogicalType::SQLNULL, LogicalType::SQLNULL);
	case fastlanes::DataType::FALLBACK:
		return LogicalType::VARCHAR;
	case fastlanes::DataType::INVALID:
	default:
		throw InternalException("TranslateType: column type is not supported");
	}
}

//-------------------------------------------------------------------
// Local
//-------------------------------------------------------------------
static unique_ptr<LocalTableFunctionState> LocalInitFn(ExecutionContext &context, TableFunctionInitInput &input,
                                                       GlobalTableFunctionState *global_state) {
	auto local_state = make_uniq<FastLanesReadLocalState>();
	auto &bind_data = input.bind_data->Cast<FastLanesReadBindData>();

	local_state->conn = fastlanes::Connection {};
	local_state->reader = std::make_unique<fastlanes::Reader>(bind_data.directory, local_state->conn);
	local_state->row_group = std::make_unique<fastlanes::Rowgroup>(local_state->reader->footer());
	local_state->materializer = std::make_unique<fastlanes::Materializer>(*local_state->row_group);

	return local_state;
};

//-------------------------------------------------------------------
// Global
//-------------------------------------------------------------------
static unique_ptr<GlobalTableFunctionState> GlobalInitFn(ClientContext &context, TableFunctionInitInput &input) {
	auto global_state = make_uniq<FastLanesReadGlobalState>();
	auto &bind_data = input.bind_data->Cast<FastLanesReadBindData>();

	// Verify that a Fastlanes vector fits in the output Vector of DuckDB
	// TODO: DuckDB 2048
	D_ASSERT(fastlanes::CFG::VEC_SZ <= 1024);
	// Vector size should be a power of 2 to allow shift based multiplication.
	D_ASSERT(powerof2(fastlanes::CFG::VEC_SZ));

	global_state->vec_sz = fastlanes::CFG::VEC_SZ;
	global_state->vec_sz_exp = std::log2(global_state->vec_sz);
	global_state->n_vector = bind_data.n_vector;
	global_state->cur_vector = 0;

	return global_state;
};

//-------------------------------------------------------------------
// Bind
//-------------------------------------------------------------------
static unique_ptr<FunctionData> BindFn(ClientContext &context, TableFunctionBindInput &input,
                                       vector<LogicalType> &return_types, vector<string> &names) {
	auto read_bind_data = make_uniq<FastLanesReadBindData>();
	read_bind_data->directory = input.inputs[0].ToString();

	// Set up a connection to the file
	fastlanes::Connection conn;
	const fastlanes::Reader &fls_reader = conn.reset().read_fls(read_bind_data->directory);

	const auto footer = fls_reader.footer();
	const auto column_descriptors = footer.GetColumnDescriptors();
	// Fill the data types and return types.
	for (idx_t i = 0; i < column_descriptors.size(); ++i) {
		return_types.push_back(TranslateType(column_descriptors[i].data_type));
		names.push_back(column_descriptors[i].name);
	}

	read_bind_data->n_vector = footer.GetNVectors();

	return read_bind_data;
};

//-------------------------------------------------------------------
// Table
//-------------------------------------------------------------------
static void LoadFlsVector(FastLanesReadGlobalState &global_state, TableFunctionInput &data, DataChunk &output, int16_t offset) {
	const auto &local_state = data.local_state->Cast<FastLanesReadLocalState>();

	auto &expressions = local_state.reader->get_chunk(global_state.cur_vector);
	// ColumnCount is defined during the bind of the table function.
	for (idx_t col_idx = 0; col_idx < output.ColumnCount(); col_idx++) {
		auto &target_col = output.data[col_idx];
		local_state.materializer->MaterializeV2(expressions[col_idx], global_state.cur_vector, FlatVector::GetData(target_col));
	}
}

static void TableFn(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &global_state = data.global_state->Cast<FastLanesReadGlobalState>();

	if (global_state.cur_vector < global_state.n_vector - 1) {
		// TODO: Set cardinality to 2048 here
		output.SetCardinality(1024);
		LoadFlsVector(global_state, data, output, 0);
		// ++global_state.cur_vector;
		// LoadFlsVector(global_state, data, output, 1024);
	} else if (global_state.cur_vector < global_state.n_vector) {
		output.SetCardinality(1024);
		LoadFlsVector(global_state, data, output, 0);
	} else {
		// Stop the stream of data, table function is no longer called.
		output.SetCardinality(0);
		return;
	}

	// Go to the next vector in the row group
	++global_state.cur_vector;
}

//-------------------------------------------------------------------
// Register
//-------------------------------------------------------------------
void ReadFastLanes::Register(DatabaseInstance &db) {
	auto table_function = TableFunction("read_fls", {LogicalType::VARCHAR}, TableFn, BindFn, GlobalInitFn, LocalInitFn);
	// TODO: support
	// table_function.projection_pushdown = true;
	ExtensionUtil::RegisterFunction(db, table_function);
}
} // namespace duckdb