#include "write_fast_lanes.hpp"

#include "details/common.hpp"
#include "fls/connection.hpp"
#include "fls/common/double.hpp"

#include <duckdb/function/copy_function.hpp>
#include <duckdb/main/extension_util.hpp>
#include "fls/common/magic_enum.hpp"
#include "fls/std/variant.hpp"
#include "fls/writer/writerv2.hpp"
#include "include/writer/fast_lanes_multi_file_info.hpp"
#include "reader/translation_utils.hpp"
#include "writer/translation_utils.hpp"
#include "writer/materializer.hpp"

#include <thread>

namespace duckdb {

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

void EmplaceSpanVec(const PhysicalType &type, AnySpanVec &vec) {
	switch (type) {
	case PhysicalType::UINT8:
		vec.emplace<SpanVec_u08>();
		break;
	case PhysicalType::UINT16:
		vec.emplace<SpanVec_u16>();
		break;
	case PhysicalType::UINT32:
		vec.emplace<SpanVec_u32>();
		break;
	case PhysicalType::UINT64:
		vec.emplace<SpanVec_u64>();
		break;
	case PhysicalType::INT8:
		vec.emplace<SpanVec_i08>();
		break;
	case PhysicalType::INT16:
		vec.emplace<SpanVec_i16>();
		break;
	case PhysicalType::INT32:
		vec.emplace<SpanVec_i32>();
		break;
	case PhysicalType::INT64:
		vec.emplace<SpanVec_i64>();
		break;
		// case PhysicalType::FLOAT:
		// 	all_columns[col_idx].emplace<SpanVec_flt>();
		// 	break;
	case PhysicalType::DOUBLE:
		vec.emplace<SpanVec_dbl>();
		break;
	case PhysicalType::VARCHAR:
		vec.emplace<SpanVec_str>();
		break;
	default:
		throw std::runtime_error("Unsupported type");
	}
}

void MapVectorToSpan(Vector &src, idx_t vec_sz, AnySpanVec &dst) {
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
	           dst);
}

void PrepareRowGroup(fastlanes::RowGroupWriter &rg_writer, const ColumnDataCollection &buf,
                     const vector<LogicalType> &types) {
	const auto col_count = types.size();
	std::vector<AnySpanVec> all_columns(col_count);

	for (idx_t col_idx = 0; col_idx < col_count; col_idx++) {
		EmplaceSpanVec(types[col_idx].InternalType(), all_columns[col_idx]);
	}

	for (auto &chunk : buf.Chunks()) {
		const auto vec_sz = chunk.size();

		for (idx_t col_idx = 0; col_idx < col_count; col_idx++) {
			auto &src = chunk.data[col_idx];
			src.Flatten(vec_sz);
			MapVectorToSpan(src, vec_sz, all_columns[col_idx]);
		}
	}

	for (idx_t col_idx = 0; col_idx < col_count; col_idx++) {
		std::visit([&](auto &vec) { rg_writer.WriteColumn(col_idx, vec); }, all_columns[col_idx]);
	}

	rg_writer.Finalize();
}

static unique_ptr<LocalFunctionData> InitLocal(ExecutionContext &context, FunctionData &bind_data_p) {
	auto &bind_data = bind_data_p.Cast<FastLanesWriteBindData>();
	return make_uniq<FastLanesWriteLocalState>(context.client, bind_data.types);
}

static void Sink(ExecutionContext &context, FunctionData &bind_data_p, GlobalFunctionData &gstate,
                 LocalFunctionData &lstate, DataChunk &input) {
	const auto &bind_data = bind_data_p.Cast<FastLanesWriteBindData>();
	const auto &global_state = gstate.Cast<FastLanesWriteGlobalState>();
	auto &local_state = lstate.Cast<FastLanesWriteLocalState>();

	local_state.buffer.Append(local_state.append_state, input);

	// Only do this when we have enough vectors
	// TODO: make exact, now because we fill with 2048 we might have spillage of 1024 vectors.
	if (local_state.buffer.Count() >= bind_data.row_group_size) {
		local_state.append_state.current_chunk_state.handles.clear();

		const auto rg_writer = global_state.writer->CreateRowGroupWriter();
		PrepareRowGroup(*rg_writer, local_state.buffer, bind_data.types);
		rg_writer->Flush();

		local_state.buffer.Reset();
		local_state.buffer.InitializeAppend(local_state.append_state);
	}
}

static void Combine(ExecutionContext &context, FunctionData &bind_data_p, GlobalFunctionData &gstate,
                    LocalFunctionData &lstate) {
	const auto &bind_data = bind_data_p.Cast<FastLanesWriteBindData>();
	auto &global_state = gstate.Cast<FastLanesWriteGlobalState>();
	auto &local_state = lstate.Cast<FastLanesWriteLocalState>();

	// Nothing to do, leave the combine buffer for other threads.
	if (local_state.buffer.Count() == 0) {
		return;
	}

	unique_lock guard(global_state.combine_lock);
	if (global_state.combine_buffer) {
		// The entire local buffer fits in the combine buffer.
		if (local_state.buffer.Count() + global_state.combine_buffer->Count() < bind_data.row_group_size) {
			global_state.combine_buffer->Combine(local_state.buffer);
			return;
		}

		// Both buffers make a row group.
		if (local_state.buffer.Count() + global_state.combine_buffer->Count() == bind_data.row_group_size) {
			global_state.combine_buffer->Combine(local_state.buffer);

			const auto owned_combine_buffer = std::move(global_state.combine_buffer);

			const auto rg_writer = global_state.writer->CreateRowGroupWriter();
			PrepareRowGroup(*rg_writer, *owned_combine_buffer, bind_data.types);
			rg_writer->Flush();
			return;
		}

		// The local buffer does not fit in the combine buffer. Fill as much as possible and put the remainder in a new
		// combine buffer.
		for (idx_t i = 0; i < local_state.buffer.ChunkCount(); i++) {
			// Get the source chunk from the ColumnDataCollection.
			DataChunk src_chunk;
			src_chunk.Initialize(context.client, bind_data.types);
			local_state.buffer.FetchChunk(i, src_chunk);

			// If the current chunk fits we just append it.
			if (global_state.combine_buffer->Count() + src_chunk.size() <= bind_data.row_group_size) {
				global_state.combine_buffer->Append(src_chunk);
				continue;
			}

			// The current chunk does not fit.
			const idx_t space_left = bind_data.row_group_size - global_state.combine_buffer->Count();
			const idx_t take = MinValue<idx_t>(space_left, src_chunk.size());

			// Fit into a destination chunk.
			DataChunk dst_chunk;
			dst_chunk.Initialize(context.client, bind_data.types);

			src_chunk.Copy(dst_chunk);
			dst_chunk.SetCardinality(take);

			// Fill up to the row group size and then flush.
			global_state.combine_buffer->Append(dst_chunk);

			const auto owned_combine_buffer = std::move(global_state.combine_buffer);

			const auto rg_writer = global_state.writer->CreateRowGroupWriter();
			PrepareRowGroup(*rg_writer, *owned_combine_buffer, bind_data.types);
			rg_writer->Flush();

			// The remaining chunks can never be larger than a row group.
			DataChunk remainder;
			remainder.Initialize(context.client, bind_data.types);
			src_chunk.Copy(remainder, take);

			global_state.combine_buffer = make_uniq<ColumnDataCollection>(context.client, local_state.buffer.Types());
			global_state.combine_buffer->Append(remainder);
		}
	} else if (local_state.buffer.Count() > 0) {
		// If there is no combine buffer but there is data in the local buffer, create a combine buffer so that other
		// threads may combine their local buffers into one of at least the row group size.
		global_state.combine_buffer = make_uniq<ColumnDataCollection>(context.client, local_state.buffer.Types());
		global_state.combine_buffer->Combine(local_state.buffer);
	}
}

// static void Combine(ExecutionContext &context, FunctionData &bind_data_p, GlobalFunctionData &gstate,
//                     LocalFunctionData &lstate) {
// 	const auto &bind_data = bind_data_p.Cast<FastLanesWriteBindData>();
// 	auto &global_state = gstate.Cast<FastLanesWriteGlobalState>();
// 	auto &local_state = lstate.Cast<FastLanesWriteLocalState>();
//
// 	// Nothing to do, leave the combine buffer for other threads.
// 	if (local_state.buffer.Count() == 0) {
// 		return;
// 	}
//
// 	// The entire local buffer fits in the combine buffer.
// 	if (local_state.buffer.Count() + global_state.combine_buffer->Count() < bind_data.row_group_size) {
// 		global_state.combine_buffer->Combine();
// 	}
//
// 	unique_lock guard(global_state.combine_lock);
// 	if (global_state.combine_buffer) {
// 		// There exists a combine buffer, if we have data in our local buffer keep adding this to the combine buffer.
// 		// If the combine buffer reaches the row group size flush, store remaining data in a new combine buffer.
//
// 		for (auto& chunk : local_state.buffer.Chunks()) {
// 			if (!global_state.combine_buffer && chunk.size() > 0) {
// 				global_state.combine_buffer =
// 				    make_uniq<ColumnDataCollection>(context.client, local_state.buffer.Types());
// 			}
//
// 			if (global_state.combine_buffer->Count() + chunk.size() < bind_data.row_group_size) {
// 				// If the chunk fits, add it to the combine buffer.
// 				global_state.combine_buffer->Append(chunk);
// 			} else if (global_state.combine_buffer->Count() + chunk.size() == bind_data.row_group_size) {
// 				global_state.combine_buffer->Append(chunk);
//
// 				auto owned_combine_buffer = std::move(global_state.combine_buffer);
//
// 				const auto rg_writer = global_state.writer->CreateRowGroupWriter();
// 				PrepareRowGroup(*rg_writer, *owned_combine_buffer, bind_data.types);
// 				rg_writer->Flush();
// 			} else {
// 				// If the chunk does no longer fit, it means we have reached the maximum. We flush and create a new
// 				// combine buffer.
// 				auto owned_combine_buffer = std::move(global_state.combine_buffer);
//
// 				const auto rg_writer = global_state.writer->CreateRowGroupWriter();
// 				PrepareRowGroup(*rg_writer, *owned_combine_buffer, bind_data.types);
// 				rg_writer->Flush();
//
// 				global_state.combine_buffer =
// 				    make_uniq<ColumnDataCollection>(context.client, local_state.buffer.Types());
// 				global_state.combine_buffer->Append(chunk);
// 			}
// 		}
//
// 	} else if (local_state.buffer.Count() > 0) {
// 		// If there is no combine buffer but there is data in the local buffer, create a combine buffer so that other
// 		// threads may combine their local buffers into one of at least the row group size.
// 		global_state.combine_buffer = make_uniq<ColumnDataCollection>(context.client, local_state.buffer.Types());
// 		global_state.combine_buffer->Combine(local_state.buffer);
// 	}
// }

// static void Combine(ExecutionContext &context, FunctionData &bind_data_p, GlobalFunctionData &gstate,
//                     LocalFunctionData &lstate) {
// 	std::cout << "Starting combine" << "\n";
// 	const auto &bind_data = bind_data_p.Cast<FastLanesWriteBindData>();
// 	auto &global_state = gstate.Cast<FastLanesWriteGlobalState>();
// 	auto &local_state = lstate.Cast<FastLanesWriteLocalState>();
//
// 	unique_lock guard(global_state.combine_lock);
// 	std::cout << "get lock" << "\n";
// 	if (global_state.combine_buffer) {
// 		std::cout << "buffer not empty" << "\n";
// 		global_state.combine_buffer->Combine(local_state.buffer);
// 		if (global_state.combine_buffer->Count() >= bind_data.row_group_size) {
// 			std::cout << "buffer ready to be flushed" << "\n";
// 			auto owned_combine_buffer = std::move(global_state.combine_buffer);
// 			guard.unlock();
//
// 			std::cout << "retrieved buffer" << "\n";
//
// 			const auto rg_writer = global_state.writer->CreateRowGroupWriter();
// 			std::cout << "will flush: " << owned_combine_buffer->Count() << " rows" << "\n";
// 			PrepareRowGroup(*rg_writer, *owned_combine_buffer, bind_data.types);
//
// 			rg_writer->Flush();
// 		}
//
// 		// Buffer is not yet big enough, wait for other threads to potentially fill more rows.
// 		return;
// 	}
//
// 	if (local_state.buffer.Count() > 0) {
// 		std::cout << "buffer empty, make new" << "\n";
// 		// If there is no combine buffer, make one and delay flushing as other threads may still enter the combine step.
// 		global_state.combine_buffer = make_uniq<ColumnDataCollection>(context.client, local_state.buffer.Types());
// 		global_state.combine_buffer->Combine(local_state.buffer);
// 		return;
// 	}
//
// 	std::cout << "buffer empty, do nothing" << "\n";
// }

static bool RotateFiles(FunctionData &bind_data_p, const optional_idx &file_size_bytes) {
	const auto &bind_data = bind_data_p.Cast<FastLanesWriteBindData>();

	// We only rotate files if there is a limit on how much data we can store in one file.
	return bind_data.row_groups_per_file;
}

static bool RotateNextFile(GlobalFunctionData &gstate, FunctionData &bind_data_p, const optional_idx &file_size_bytes) {
	const auto &global_state = gstate.Cast<FastLanesWriteGlobalState>();
	const auto &bind_data = bind_data_p.Cast<FastLanesWriteBindData>();

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

	std::vector<std::unique_ptr<fastlanes::ColumnDescriptorT>> descriptors;
	descriptors.reserve(bind_data.names.size());

	D_ASSERT(bind_data.types.size() == bind_data.names.size());
	for (size_t i = 0; i < bind_data.types.size(); ++i) {
		auto col = make_uniq<fastlanes::ColumnDescriptorT>();

		col->name = bind_data.names.at(i);
		col->data_type = WriterTranslateUtils::TranslateType(bind_data.types.at(i));

		descriptors.push_back(std::move(col));
	}

	global_state->writer = fastlanes::FileWriter::Builder()
	                           .WithSchema(std::move(descriptors))
	                           .WithPath(global_state->file_path)
	                           .WithConnection(global_state->conn)
	                           .WithMaxRowGroups(bind_data.row_groups_per_file)
	                           .WithRowGroupSize(bind_data.row_group_size)
	                           // Enable explicit flush so we can commit to a FastLanes file in the
	                           // FlushBatch() function. This is required as PrepareBatch() can be called
	                           // out-of-order, whereas FlushBatch() enforces order across row groups.
	                           .WithExplicitFlush()
	                           .Build();
	global_state->writer->Open();

	return global_state;
}

static CopyFunctionExecutionMode GetExecutionMode(const bool preserve_insertion_order,
                                                  const bool supports_batch_index) {
	if (!preserve_insertion_order) {
		return CopyFunctionExecutionMode::PARALLEL_COPY_TO_FILE;
	}
	if (supports_batch_index) {
		return CopyFunctionExecutionMode::BATCH_COPY_TO_FILE;
	}
	return CopyFunctionExecutionMode::REGULAR_COPY_TO_FILE;
}

static idx_t GetDesiredBatchsize(ClientContext &context, FunctionData &bind_data_p) {
	const auto &bind_data = bind_data_p.Cast<FastLanesWriteBindData>();

	return bind_data.row_group_size;
}

static unique_ptr<PreparedBatchData> PrepareBatch(ClientContext &context, FunctionData &bind_data_p,
                                                  GlobalFunctionData &gstate_p,
                                                  unique_ptr<ColumnDataCollection> collection) {
	const auto &bind_data = bind_data_p.Cast<FastLanesWriteBindData>();
	const auto &gstate = gstate_p.Cast<FastLanesWriteGlobalState>();
	auto batch_data = make_uniq<FastLanesWriteBatchData>();

	batch_data->rg_writer = gstate.writer->CreateRowGroupWriter();
	PrepareRowGroup(*batch_data->rg_writer, *collection, bind_data.types);

	return batch_data;
}

static void FlushBatch(ClientContext &context, FunctionData &bind_data, GlobalFunctionData &gstate_p,
                       PreparedBatchData &batch_p) {
	auto &gstate = gstate_p.Cast<FastLanesWriteGlobalState>();

	const auto &batch_data = batch_p.Cast<FastLanesWriteBatchData>();
	batch_data.rg_writer->Flush();

	gstate.num_row_groups++;
}

static void Finalize(ClientContext &context, FunctionData &bind_data_p, GlobalFunctionData &gstate_p) {
	std::cout << "Exec Finalize" << "\n";
	const auto &bind_data = bind_data_p.Cast<FastLanesWriteBindData>();
	const auto &global_state = gstate_p.Cast<FastLanesWriteGlobalState>();

	if (global_state.combine_buffer) {
		const auto rg_writer = global_state.writer->CreateRowGroupWriter();
		std::cout << "will flush: " << global_state.combine_buffer->Count() << " rows" << "\n";
		PrepareRowGroup(*rg_writer, *global_state.combine_buffer, bind_data.types);
		rg_writer->Flush();
		global_state.combine_buffer->Reset();
	}

	global_state.writer->Close();
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
