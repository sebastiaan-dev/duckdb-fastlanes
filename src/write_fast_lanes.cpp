#include "write_fast_lanes.hpp"
#include "include/write_fast_lanes.hpp"

#include "details/common.hpp"
#include "fls/connection.hpp"
#include "fls/cfg/cfg.hpp"
#include "fls/common/double.hpp"

#include <duckdb/function/copy_function.hpp>
#include <duckdb/main/extension_util.hpp>
#include "fls/common/magic_enum.hpp"
#include "fls/std/variant.hpp"
#include "fls/table/attribute.hpp"
#include "fls/utl/cpu/arch.hpp"
#include "fls/writer/writerv2.hpp"
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

	// We only rotate files if there is a limit on how much data we can store in one file.
	return bind_data.row_groups_per_file;
}

static bool RotateNextFile(GlobalFunctionData &gstate, FunctionData &bind_data_p, const optional_idx &file_size_bytes) {
	auto &global_state = gstate.Cast<FastLanesWriteGlobalState>();
	auto &bind_data = bind_data_p.Cast<FastLanesWriteBindData>();

	if (!bind_data.row_groups_per_file) {
		return false;
	}

	if (global_state.num_row_groups >= bind_data.row_groups_per_file) {
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

	const auto &bind_data = bind_data_p.Cast<FastLanesWriteBindData>();
	auto global_state = make_uniq<FastLanesWriteGlobalState>(file_path);
	// TODO: temp (prev = 7365033667ns ~7s)
	global_state->start = std::chrono::high_resolution_clock::now();

	global_state->conn.inline_footer();
	global_state->next_row_group = 0;

	std::vector<std::unique_ptr<fastlanes::ColumnDescriptorT>> descriptors;
	descriptors.reserve(bind_data.names.size());

	D_ASSERT(bind_data.types.size() == bind_data.names.size());
	for (size_t i = 0; i < bind_data.types.size(); ++i) {
		auto col = make_uniq<fastlanes::ColumnDescriptorT>();

		col->name = bind_data.names.at(i);
		col->data_type = WriterTranslateUtils::TranslateType(bind_data.types.at(i));

		descriptors.push_back(std::move(col));
	}

	auto writer = fastlanes::FileWriter::Builder()
	                  .WithSchema(std::move(descriptors))
	                  .WithPath(global_state->file_path)
	                  .WithConnection(global_state->conn)
	                  .WithMaxRowGroups(bind_data.row_groups_per_file)
	                  .WithRowGroupSize(bind_data.row_group_size)
	                  // Enable explicit flush so we can commit to a FastLanes file in the FlushBatch() function.
	                  // This is required as PrepareBatch() can be called out-of-order, whereas FlushBatch() enforces
	                  // order across row groups.
	                  .WithExplicitFlush()
	                  .Build();
	writer.Open();
	global_state->writer = make_uniq<fastlanes::FileWriter>(std::move(writer));

	return global_state;
}

static CopyFunctionExecutionMode GetExecutionMode(const bool preserve_insertion_order,
                                                  const bool supports_batch_index) {
	if (preserve_insertion_order) {
		throw NotImplementedException("preserve insertion order is not supported.");
	}
	if (supports_batch_index) {
		return CopyFunctionExecutionMode::BATCH_COPY_TO_FILE;
	}

	throw NotImplementedException("other execution modes are not supported.");
}

static idx_t GetDesiredBatchsize(ClientContext &context, FunctionData &bind_data_p) {
	const auto &bind_data = bind_data_p.Cast<FastLanesWriteBindData>();

	return bind_data.row_group_size;
}

template <typename DUCK_T, typename FAST_PT>
void FillFixedWidthColumn(Vector &src, const idx_t count, std::vector<std::span<FAST_PT>> &out) {
	const auto data_ptr = FlatVector::GetData<DUCK_T>(src);
	out.emplace_back(reinterpret_cast<FAST_PT *>(data_ptr), count);
}

void FillStringColumn(Vector &src, const idx_t count, std::vector<fastlanes::str_pt> &scratch,
                      std::vector<std::span<fastlanes::str_pt>> &out) {
	const auto data_ptr = FlatVector::GetData<string_t>(src);
	scratch.clear();
	scratch.reserve(count);
	for (idx_t i = 0; i < count; i++) {
		auto &s = data_ptr[i];
		// TODO: Probably want to pass the pointer to save a copy?
		// auto ptr = reinterpret_cast<uint8_t const *>(s.GetDataUnsafe());
		// auto len = static_cast<fastlanes::len_t>(s.GetSize());
		scratch.emplace_back(s.GetString());
	}
	out.emplace_back(scratch.data(), scratch.size());
}

using SpanVec_u08 = std::vector<std::span<fastlanes::u08_pt>>;
using SpanVec_u16 = std::vector<std::span<fastlanes::u16_pt>>;
using SpanVec_u32 = std::vector<std::span<fastlanes::u32_pt>>;
using SpanVec_u64 = std::vector<std::span<fastlanes::u64_pt>>;

using SpanVec_i08 = std::vector<std::span<fastlanes::i08_pt>>;
using SpanVec_i16 = std::vector<std::span<fastlanes::i16_pt>>;
using SpanVec_i32 = std::vector<std::span<fastlanes::i32_pt>>;
using SpanVec_i64 = std::vector<std::span<fastlanes::i64_pt>>;

// using SpanVec_flt = std::vector<std::span<fastlanes::flt_pt>>;
using SpanVec_dbl = std::vector<std::span<fastlanes::dbl_pt>>;
using SpanVec_str = std::vector<std::span<fastlanes::str_pt>>;

using AnySpanVec = std::variant<SpanVec_u08, SpanVec_u16, SpanVec_u32, SpanVec_u64, SpanVec_i08, SpanVec_i16,
                                SpanVec_i32, SpanVec_i64, SpanVec_dbl, SpanVec_str>;

static unique_ptr<PreparedBatchData> PrepareBatch(ClientContext &context, FunctionData &bind_data_p,
                                                  GlobalFunctionData &gstate_p,
                                                  unique_ptr<ColumnDataCollection> collection) {
	auto &bind_data = bind_data_p.Cast<FastLanesWriteBindData>();
	auto &gstate = gstate_p.Cast<FastLanesWriteGlobalState>();
	auto batch_data = make_uniq<FastLanesWriteBatchData>();

	batch_data->rg_writer = gstate.writer->CreateRowGroupWriter();

	const auto col_count = bind_data.types.size();
	std::vector<AnySpanVec> all_columns(col_count);

	for (idx_t col_idx = 0; col_idx < col_count; col_idx++) {
		switch (bind_data.types[col_idx].InternalType()) {
		case PhysicalType::UINT8:
			all_columns[col_idx].emplace<SpanVec_u08>();
			break;
		case PhysicalType::UINT16:
			all_columns[col_idx].emplace<SpanVec_u16>();
			break;
		case PhysicalType::UINT32:
			all_columns[col_idx].emplace<SpanVec_u32>();
			break;
		case PhysicalType::UINT64:
			all_columns[col_idx].emplace<SpanVec_u64>();
			break;
		case PhysicalType::INT8:
			all_columns[col_idx].emplace<SpanVec_i08>();
			break;
		case PhysicalType::INT16:
			all_columns[col_idx].emplace<SpanVec_i16>();
			break;
		case PhysicalType::INT32:
			all_columns[col_idx].emplace<SpanVec_i32>();
			break;
		case PhysicalType::INT64:
			all_columns[col_idx].emplace<SpanVec_i64>();
			break;
		// case PhysicalType::FLOAT:
		// 	all_columns[col_idx].emplace<SpanVec_flt>();
		// 	break;
		case PhysicalType::DOUBLE:
			all_columns[col_idx].emplace<SpanVec_dbl>();
			break;
		case PhysicalType::VARCHAR:
			all_columns[col_idx].emplace<SpanVec_str>();
			break;
		default:
			throw std::runtime_error("Unsupported type");
		}
	}

	for (auto &chunk : collection->Chunks()) {
		auto vec_sz = chunk.size();

		for (idx_t col_idx = 0; col_idx < col_count; col_idx++) {
			auto &src = chunk.data[col_idx];
			src.Flatten(vec_sz);

			std::visit(fastlanes::overloaded {[&](SpanVec_i08 &vec) {
				                                  auto ptr = FlatVector::GetData<int8_t>(src);
				                                  vec.emplace_back(ptr, vec_sz);
			                                  },

			                                  [&](SpanVec_i16 &vec) {
				                                  auto ptr = FlatVector::GetData<int16_t>(src);
				                                  vec.emplace_back(ptr, vec_sz);
			                                  },

			                                  [&](SpanVec_i32 &vec) {
				                                  auto ptr = FlatVector::GetData<int32_t>(src);
				                                  vec.emplace_back(ptr, vec_sz);
			                                  },

			                                  [&](SpanVec_i64 &vec) {
				                                  auto ptr = FlatVector::GetData<int64_t>(src);
				                                  vec.emplace_back(ptr, vec_sz);
			                                  },

			                                  [&](SpanVec_u08 &vec) {
				                                  auto ptr = FlatVector::GetData<uint8_t>(src);
				                                  vec.emplace_back(ptr, vec_sz);
			                                  },

			                                  [&](SpanVec_u16 &vec) {
				                                  auto ptr = FlatVector::GetData<uint16_t>(src);
				                                  vec.emplace_back(ptr, vec_sz);
			                                  },

			                                  [&](SpanVec_u32 &vec) {
				                                  auto ptr = FlatVector::GetData<uint32_t>(src);
				                                  vec.emplace_back(ptr, vec_sz);
			                                  },

			                                  [&](SpanVec_u64 &vec) {
				                                  auto ptr = FlatVector::GetData<uint64_t>(src);
				                                  vec.emplace_back(ptr, vec_sz);
			                                  },

			                                  [&](SpanVec_dbl &vec) {
				                                  auto ptr = FlatVector::GetData<double>(src);
				                                  vec.emplace_back(ptr, vec_sz);
			                                  },

			                                  [&](SpanVec_str &vec) {
				                                  std::vector<fastlanes::str_pt> scratch;
				                                  scratch.reserve(vec_sz);
				                                  const auto data_ptr = FlatVector::GetData<string_t>(src);
				                                  for (idx_t i = 0; i < vec_sz; i++) {
					                                  auto &s = data_ptr[i];
					                                  // TODO: Probably want to pass the pointer to save a copy?
					                                  // auto ptr = reinterpret_cast<uint8_t const
					                                  // *>(s.GetDataUnsafe()); auto len =
					                                  // static_cast<fastlanes::len_t>(s.GetSize());
					                                  scratch.emplace_back(s.GetString());
				                                  }
				                                  vec.emplace_back(scratch.data(), scratch.size());
			                                  }

			           },
			           all_columns[col_idx]);
		}
	}

	for (idx_t col_idx = 0; col_idx < col_count; col_idx++) {
		std::visit([&](auto &vec) { batch_data->rg_writer->WriteColumn(col_idx, vec); }, all_columns[col_idx]);
	}

	batch_data->rg_writer->Finalize();

	return batch_data;
}

static void FlushBatch(ClientContext &context, FunctionData &bind_data, GlobalFunctionData &gstate_p,
                       PreparedBatchData &batch_p) {
	auto &gstate = gstate_p.Cast<FastLanesWriteGlobalState>();

	const auto &batch_data = batch_p.Cast<FastLanesWriteBatchData>();
	batch_data.rg_writer->Flush();

	gstate.num_row_groups++;
}

static void Finalize(ClientContext &context, FunctionData &bind_data, GlobalFunctionData &gstate_p) {
	std::cout << "Finalizing local function data..." << std::endl;
	auto &gstate = gstate_p.Cast<FastLanesWriteGlobalState>();

	gstate.writer->Close();

	const auto end = std::chrono::high_resolution_clock::now();
	std::cout << "total time:" << end - gstate.start << "\n";
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
