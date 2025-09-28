

#include "writer/fls_writer.hpp"
#include "duckdb/common/types/column/column_data_collection.hpp"
#include "duckdb/function/copy_function.hpp"
#include "writer/fls_view_writer.hpp"
#include "writer/translation_utils.hpp"

namespace duckdb {

static void PrepareRowGroup(fastlanes::RowGroupWriter&  rg_writer,
                            const ColumnDataCollection& buf,
                            const vector<LogicalType>&  types) {
	const auto col_count = types.size();

	vector<unique_ptr<ViewWriterFactoryBase>> buffers;
	buffers.resize(col_count);

	for (idx_t col_idx = 0; col_idx < col_count; col_idx++) {
		buffers[col_idx] = MakeViewWriterFactory(types[col_idx].InternalType());
	}

	for (auto& chunk : buf.Chunks()) {
		const auto vec_sz = chunk.size();

		for (idx_t col_idx = 0; col_idx < col_count; col_idx++) {
			auto& src = chunk.data[col_idx];
			src.Flatten(vec_sz);

			auto view = buffers[col_idx]->Build(src, vec_sz);
			rg_writer.WriteColumn(*view, col_idx);
		}
	}

	rg_writer.Finalize();
}

unique_ptr<LocalFunctionData> FastLanesFileWriter::InitLocal(ExecutionContext& context, FunctionData& bind_data_p) {
	auto& bind_data = bind_data_p.Cast<FastLanesWriteBindData>();
	return make_uniq<FastLanesWriteLocalState>(context.client, bind_data.types);
}

void FastLanesFileWriter::Sink(ExecutionContext&   context,
                               FunctionData&       bind_data_p,
                               GlobalFunctionData& global_state_p,
                               LocalFunctionData&  local_state_p,
                               DataChunk&          input) {
	const auto& bind_data    = bind_data_p.Cast<FastLanesWriteBindData>();
	auto&       global_state = global_state_p.Cast<FastLanesWriteGlobalState>();
	auto&       local_state  = local_state_p.Cast<FastLanesWriteLocalState>();

	if (local_state.buffer.Count() + input.size() < bind_data.row_group_size) {
		local_state.buffer.Append(local_state.append_state, input);
		return;
	}

	if (local_state.buffer.Count() + input.size() == bind_data.row_group_size) {
		local_state.buffer.Append(local_state.append_state, input);
		local_state.append_state.current_chunk_state.handles.clear();

		const auto rg_writer = global_state.writer->CreateRowGroupWriter();
		PrepareRowGroup(*rg_writer, local_state.buffer, bind_data.types);
		rg_writer->Flush();
		global_state.num_row_groups++;

		local_state.buffer.Reset();
		local_state.buffer.InitializeAppend(local_state.append_state);

		return;
	}

	if (local_state.buffer.Count() + input.size() > bind_data.row_group_size) {
		const idx_t space_left = bind_data.row_group_size - local_state.buffer.Count();
		const idx_t take       = MinValue<idx_t>(space_left, input.size());

		DataChunk fill_chunk;
		fill_chunk.Initialize(context.client, bind_data.types);

		fill_chunk.Reference(input);
		fill_chunk.SetCardinality(take);

		local_state.buffer.Append(local_state.append_state, fill_chunk);
		local_state.append_state.current_chunk_state.handles.clear();

		const auto rg_writer = global_state.writer->CreateRowGroupWriter();
		PrepareRowGroup(*rg_writer, local_state.buffer, bind_data.types);
		rg_writer->Flush();
		global_state.num_row_groups++;

		local_state.buffer.Reset();
		local_state.buffer.InitializeAppend(local_state.append_state);

		DataChunk remainder;
		remainder.Initialize(context.client, bind_data.types);
		input.Copy(remainder, take);

		local_state.buffer.Append(local_state.append_state, remainder);
	}
}

void FastLanesFileWriter::Combine(ExecutionContext&   context,
                                  FunctionData&       bind_data_p,
                                  GlobalFunctionData& global_state_p,
                                  LocalFunctionData&  local_state_p) {
	const auto& bind_data    = bind_data_p.Cast<FastLanesWriteBindData>();
	auto&       global_state = global_state_p.Cast<FastLanesWriteGlobalState>();
	auto&       local_state  = local_state_p.Cast<FastLanesWriteLocalState>();

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
			global_state.num_row_groups++;
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
			const idx_t take       = MinValue<idx_t>(space_left, src_chunk.size());

			// Fit into a destination chunk.
			DataChunk dst_chunk;
			dst_chunk.Initialize(context.client, bind_data.types);

			src_chunk.Copy(dst_chunk, 0);
			dst_chunk.SetCardinality(take);

			// Fill up to the row group size and then flush.
			global_state.combine_buffer->Append(dst_chunk);

			const auto owned_combine_buffer = std::move(global_state.combine_buffer);

			const auto rg_writer = global_state.writer->CreateRowGroupWriter();
			PrepareRowGroup(*rg_writer, *owned_combine_buffer, bind_data.types);
			rg_writer->Flush();
			global_state.num_row_groups++;

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

bool FastLanesFileWriter::RotateFiles(FunctionData& bind_data_p, const optional_idx& file_size_bytes) {
	const auto& bind_data = bind_data_p.Cast<FastLanesWriteBindData>();

	// We only rotate files if there is a limit on how much data we can store in one file.
	return bind_data.row_groups_per_file;
}

bool FastLanesFileWriter::RotateNextFile(GlobalFunctionData& global_state_p,
                                         FunctionData&       bind_data_p,
                                         const optional_idx& file_size_bytes) {
	const auto& global_state = global_state_p.Cast<FastLanesWriteGlobalState>();
	const auto& bind_data    = bind_data_p.Cast<FastLanesWriteBindData>();

	if (!bind_data.row_groups_per_file) {
		return false;
	}

	if (global_state.num_row_groups >= bind_data.row_groups_per_file) {
		return true;
	}

	return false;
}

unique_ptr<FunctionData> FastLanesFileWriter::Bind(ClientContext&             context,
                                                   CopyFunctionBindInput&     input,
                                                   const vector<string>&      names,
                                                   const vector<LogicalType>& sql_types) {
	auto data = make_uniq<FastLanesWriteBindData>();

	for (const auto& [name, values] : input.info.options) {
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

unique_ptr<GlobalFunctionData>
FastLanesFileWriter::InitGlobal(ClientContext& context, FunctionData& bind_data_p, const string& file_path) {
	const auto& bind_data    = bind_data_p.Cast<FastLanesWriteBindData>();
	auto        global_state = make_uniq<FastLanesWriteGlobalState>(file_path);

	std::vector<std::unique_ptr<fastlanes::ColumnDescriptorT>> descriptors;
	descriptors.reserve(bind_data.names.size());

	D_ASSERT(bind_data.types.size() == bind_data.names.size());
	for (size_t i = 0; i < bind_data.types.size(); ++i) {
		auto col = make_uniq<fastlanes::ColumnDescriptorT>();

		col->name      = bind_data.names.at(i);
		col->data_type = WriterTranslateUtils::TranslateType(bind_data.types.at(i));

		descriptors.push_back(std::move(col));
	}

	global_state->writer =
	    fastlanes::FileWriter::Builder()
	        .WithSchema(std::move(descriptors))
	        .WithPath(global_state->file_path)
	        .WithConnection(global_state->conn)
	        .WithMaxRowGroups(bind_data.row_groups_per_file)
	        .WithRowGroupSize(bind_data.row_group_size)
	        .WithInlinedFooter(bind_data.inline_footer ? fastlanes::fls_bool::FLS_TRUE : fastlanes::fls_bool::FLS_FALSE)
	        // Enable explicit flush so we can commit to a FastLanes file in the
	        // FlushBatch() function. This is required as PrepareBatch() can be called
	        // out-of-order, whereas FlushBatch() enforces order across row groups.
	        .WithExplicitFlush()
	        .Build();
	global_state->writer->Open();

	return global_state;
}

CopyFunctionExecutionMode FastLanesFileWriter::GetExecutionMode(const bool preserve_insertion_order,
                                                                const bool supports_batch_index) {
	if (!preserve_insertion_order) {
		return CopyFunctionExecutionMode::PARALLEL_COPY_TO_FILE;
	}
	if (supports_batch_index) {
		return CopyFunctionExecutionMode::BATCH_COPY_TO_FILE;
	}
	return CopyFunctionExecutionMode::REGULAR_COPY_TO_FILE;
}

idx_t FastLanesFileWriter::GetDesiredBatchsize(ClientContext& context, FunctionData& bind_data_p) {
	const auto& bind_data = bind_data_p.Cast<FastLanesWriteBindData>();

	return bind_data.row_group_size;
}

unique_ptr<PreparedBatchData> FastLanesFileWriter::PrepareBatch(ClientContext&                   context,
                                                                FunctionData&                    bind_data_p,
                                                                GlobalFunctionData&              global_state_p,
                                                                unique_ptr<ColumnDataCollection> collection) {
	const auto& bind_data    = bind_data_p.Cast<FastLanesWriteBindData>();
	const auto& global_state = global_state_p.Cast<FastLanesWriteGlobalState>();
	auto        batch_data   = make_uniq<FastLanesWriteBatchData>();

	batch_data->rg_writer = global_state.writer->CreateRowGroupWriter();
	PrepareRowGroup(*batch_data->rg_writer, *collection, bind_data.types);

	return batch_data;
}

void FastLanesFileWriter::FlushBatch(ClientContext&      context,
                                     FunctionData&       bind_data,
                                     GlobalFunctionData& global_state_p,
                                     PreparedBatchData&  batch_p) {
	auto& global_state = global_state_p.Cast<FastLanesWriteGlobalState>();

	const auto& batch_data = batch_p.Cast<FastLanesWriteBatchData>();
	batch_data.rg_writer->Flush();

	global_state.num_row_groups++;
}

void FastLanesFileWriter::Finalize(ClientContext&      context,
                                   FunctionData&       bind_data_p,
                                   GlobalFunctionData& global_state_p) {
	const auto& bind_data    = bind_data_p.Cast<FastLanesWriteBindData>();
	auto&       global_state = global_state_p.Cast<FastLanesWriteGlobalState>();

	if (global_state.combine_buffer) {
		const auto rg_writer = global_state.writer->CreateRowGroupWriter();
		PrepareRowGroup(*rg_writer, *global_state.combine_buffer, bind_data.types);
		rg_writer->Flush();
		global_state.num_row_groups++;
		global_state.combine_buffer->Reset();
	}

	global_state.writer->Close();
}
} // namespace duckdb