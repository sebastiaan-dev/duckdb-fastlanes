#include "write_fast_lanes.hpp"
#include "include/write_fast_lanes.hpp"

#include "details/common.hpp"
#include "fls/connection.hpp"
#include "fls/cfg/cfg.hpp"
#include "fls/common/double.hpp"

#include <duckdb/function/copy_function.hpp>
#include <duckdb/main/extension_util.hpp>
#include "fls/common/magic_enum.hpp"
#include "fls/table/attribute.hpp"
#include "fls/utl/cpu/arch.hpp"
#include "include/writer/fast_lanes_multi_file_info.hpp"
#include "reader/translation_utils.hpp"
#include "writer/fast_lanes_multi_file_info.hpp"
#include "writer/translation_utils.hpp"
#include "writer/materializer.hpp"

#include <thread>

namespace duckdb {

/**
 * TODO
 */
static unique_ptr<LocalFunctionData> InitLocal(ExecutionContext &context, FunctionData &bind_data) {
	std::cout << "Initializing local function data..." << std::endl;

	return nullptr;
}
static void Sink(ExecutionContext &context, FunctionData &bind_data, GlobalFunctionData &gstate,
                 LocalFunctionData &lstate, DataChunk &input) {
	std::cout << "Sinking local function data..." << std::endl;
}
static void Combine(ExecutionContext &context, FunctionData &bind_data, GlobalFunctionData &gstate,
                    LocalFunctionData &lstate) {
	std::cout << "Combining local function data..." << std::endl;
}
/**
 * TODO END
 */

static bool RotateFiles(FunctionData &bind_data_p, const optional_idx &file_size_bytes) {
	const auto &bind_data = bind_data_p.Cast<FastLanesWriteBindData>();

	return bind_data.row_groups_per_file.IsValid();
}

static bool RotateNextFile(GlobalFunctionData &gstate, FunctionData &bind_data_p, const optional_idx &file_size_bytes) {
	auto &global_state = gstate.Cast<FastLanesWriteGlobalState>();
	auto &bind_data = bind_data_p.Cast<FastLanesWriteBindData>();

	if (!bind_data.row_groups_per_file.IsValid()) {
		return false;
	}

	if (global_state.num_row_groups >= bind_data.row_groups_per_file.GetIndex()) {
		std::cout << "Rotating next file..." << '\n';
		return true;
	}

	std::cout << "Continuing file..." << '\n';

	return false;
}

static unique_ptr<FunctionData> Bind(ClientContext &context, CopyFunctionBindInput &input, const vector<string> &names,
                                     const vector<LogicalType> &sql_types) {
	auto data = make_uniq<FastLanesWriteBindData>();

	for (const auto &[name, values] : input.info.options) {
		const auto key = StringUtil::Lower(name);
		if (values.size() != 1) {
			throw BinderException("Only one value allowed per option.");
		}

		if (key == "row_group_size") {
			data->row_group_size = values[0].GetValue<uint64_t>();
		} else if (key == "row_groups_per_file") {
			data->row_groups_per_file = values[0].GetValue<uint64_t>();
		} else if (key == "inline_footer") {
			data->inline_footer = values[0].GetValue<bool>();
		} else {
			throw BinderException("Unrecognized option: " + name);
		}
	}

	data->types = sql_types;
	data->names = names;

	return data;
}

static unique_ptr<GlobalFunctionData> InitGlobal(ClientContext &context, FunctionData &bind_data_p,
                                                 const string &file_path) {
	auto &bind_data = bind_data_p.Cast<FastLanesWriteBindData>();
	auto global_state = make_uniq<FastLanesWriteGlobalState>(file_path);

	global_state->conn.inline_footer();
	global_state->next_row_group = 0;

	const std::filesystem::path path = global_state->file_path;

	std::vector<std::unique_ptr<fastlanes::ColumnDescriptorT>> descriptors;
	descriptors.reserve(bind_data.names.size());

	D_ASSERT(bind_data.types.size() == bind_data.names.size());
	for (size_t i = 0; i < bind_data.types.size(); ++i) {
		auto col = make_uniq<fastlanes::ColumnDescriptorT>();

		col->name = bind_data.names.at(i);
		col->data_type = WriterTranslateUtils::TranslateType(bind_data.types.at(i));

		if (WriterTranslateUtils::TranslateType(bind_data.types.at(i)) != fastlanes::DataType::DOUBLE) {
			std::cout << "err, not double" << '\n';
		}

		descriptors.push_back(std::move(col));
	}

	global_state->writer = make_uniq<fastlanes::Writer>(path, descriptors, global_state->conn);

	return global_state;
}

static CopyFunctionExecutionMode GetExecutionMode(bool preserve_insertion_order, bool supports_batch_index) {
	std::cout << "GetExecutionMode()" << std::endl;

	if (preserve_insertion_order) {
		throw NotImplementedException("preserve insertion order is not supported.");
	}
	if (supports_batch_index) {
		return CopyFunctionExecutionMode::BATCH_COPY_TO_FILE;
	}

	throw NotImplementedException("other execution modes are not supported.");
}

static idx_t GetDesiredBatchsize(ClientContext &context, FunctionData &bind_data_p) {
	std::cout << "GetDesiredBatchsize()" << std::endl;

	const auto &bind_data = bind_data_p.Cast<FastLanesWriteBindData>();

	return fastlanes::CFG::VEC_SZ;
	// return bind_data.row_group_size * fastlanes::CFG::VEC_SZ;
}

// TODO: Why is the chunks method faster than other exposed methods?
// TODO: Handle non-multiple of fastlanes VEC_SZ
static unique_ptr<PreparedBatchData> PrepareBatch(ClientContext &context, FunctionData &bind_data_p,
                                                  GlobalFunctionData &gstate_p,
                                                  unique_ptr<ColumnDataCollection> collection) {
	std::cout << "PrepareBatch()" << '\n';

	auto &bind_data = bind_data_p.Cast<FastLanesWriteBindData>();
	auto &gstate = gstate_p.Cast<FastLanesWriteGlobalState>();
	auto batch_data = make_uniq<FastLanesWriteBatchData>();

	const auto col_count = bind_data.types.size();
	for (idx_t col_idx = 0; col_idx < col_count; col_idx++) {
		std::vector<std::span<const fastlanes::dbl_pt>> column;
		column.reserve(collection->ChunkCount());

		for (auto &chunk : collection->Chunks()) {
			auto src = chunk.data[col_idx];
			constexpr auto count = DEFAULT_STANDARD_VECTOR_SIZE;

			// Create a new vector which gives a contiguous memory space.
			src.Flatten(count);

			const auto data_ptr = FlatVector::GetData<double>(src);
			const std::span<const fastlanes::dbl_pt> vector(data_ptr, count);
			column.emplace_back(vector);
		}

		gstate.writer->WriteColumn(column, col_idx);
	}

	return batch_data;
}

static void FlushBatch(ClientContext &context, FunctionData &bind_data, GlobalFunctionData &gstate_p,
                       PreparedBatchData &batch_p) {
	std::cout << "FlushBatches()" << '\n';
	auto &gstate = gstate_p.Cast<FastLanesWriteGlobalState>();


	if (gstate.next_row_group == 0) {
		std::this_thread::sleep_for(std::chrono::seconds(10));
	}
	++gstate.next_row_group;

	auto &batch_data = batch_p.Cast<FastLanesWriteBatchData>();

	auto &connection = gstate.conn;
	auto table = make_uniq<fastlanes::Table>(connection);
	table->m_rowgroups.push_back(std::move(batch_data.row_group));
	connection.m_table = std::move(table);

	gstate.num_row_groups++;
}

static void Finalize(ClientContext &context, FunctionData &bind_data, GlobalFunctionData &gstate_p) {
	std::cout << "Finalizing local function data..." << std::endl;
	auto &gstate = gstate_p.Cast<FastLanesWriteGlobalState>();

	gstate.writer->Close();
}

void WriteFastLanes::Register(DatabaseInstance &db) {
	CopyFunction fn("fls");

	// copy_to_select_t copy_to_select;
	// copy_to_get_written_statistics_t copy_to_get_written_statistics;
	//
	// copy_to_serialize_t serialize;
	// copy_to_deserialize_t deserialize;

	fn.copy_to_bind = Bind;
	fn.copy_to_initialize_local = InitLocal;
	fn.copy_to_initialize_global = InitGlobal;
	fn.copy_to_sink = Sink;
	fn.copy_to_combine = Combine;
	fn.copy_to_finalize = Finalize;
	fn.execution_mode = GetExecutionMode;

	fn.prepare_batch = PrepareBatch;
	fn.flush_batch = FlushBatch;
	fn.desired_batch_size = GetDesiredBatchsize;

	fn.rotate_files = RotateFiles;
	fn.rotate_next_file = RotateNextFile;

	// TODO: What does this do?
	fn.extension = "fls";

	ExtensionUtil::RegisterFunction(db, fn);
}

} // namespace duckdb
