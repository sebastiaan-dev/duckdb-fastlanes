#include "reader/fls_multi_file_info.hpp"
#include "duckdb/planner/table_filter_state.hpp"
#include "reader/fls_reader.hpp"
#include <algorithm>
#include <duckdb/execution/adaptive_filter.hpp>
#include <duckdb/parallel/task_scheduler.hpp>
#include <fls/expression/physical_expression.hpp>
#include <thread>

namespace duckdb {

unique_ptr<MultiFileReaderInterface> FastLanesMultiFileInfo::CreateInterface(ClientContext& context) {
	return make_uniq<FastLanesMultiFileInfo>();
}

bool FastLanesMultiFileInfo::ParseCopyOption(ClientContext&         context,
                                             const string&          key,
                                             const vector<Value>&   values,
                                             BaseFileReaderOptions& options_p,
                                             vector<string>&        expected_names,
                                             vector<LogicalType>&   expected_types) {
	return false;
}

unique_ptr<BaseFileReaderOptions> FastLanesMultiFileInfo::InitializeOptions(ClientContext&                  context,
                                                                            optional_ptr<TableFunctionInfo> info) {
	return make_uniq<FastLanesFileReaderOptions>();
}

bool FastLanesMultiFileInfo::ParseOption(ClientContext&         context,
                                         const string&          original_key,
                                         const Value&           val,
                                         MultiFileOptions&      file_options,
                                         BaseFileReaderOptions& options_p) {
	auto&      options = options_p.Cast<FastLanesFileReaderOptions>();
	const auto key     = StringUtil::Lower(original_key);

	if (val.IsNull()) {
		throw BinderException("Cannot use NULL as argument to %s", original_key);
	}
	if (key == "explicit_cardinality") {
		options.explicit_cardinality = UBigIntValue::Get(val);
		return true;
	}
	if (key == "file_row_number") {
		options.file_row_number = BooleanValue::Get(val);
		return true;
	}
	return false;
}

void FastLanesMultiFileInfo::GetBindInfo(const TableFunctionData& bind_data_p, BindInfo& info) {
	// NOTE: This function is implemented, but it has no effect on the rest of the system, we keep it nonetheless in
	// case of a patch down the line.
	auto& bind_data = bind_data_p.Cast<FastLanesFileReaderOptions>();

	info.InsertOption("file_row_number", Value::BOOLEAN(bind_data.file_row_number));
	info.InsertOption("explicit_cardinality", Value::UBIGINT(bind_data.explicit_cardinality));
}

unique_ptr<TableFunctionData> FastLanesMultiFileInfo::InitializeBindData(MultiFileBindData& multi_file_data,
                                                                         unique_ptr<BaseFileReaderOptions> options_p) {
	auto bind_data      = make_uniq<FastLanesReadBindData>();
	bind_data->options  = unique_ptr_cast<BaseFileReaderOptions, FastLanesFileReaderOptions>(std::move(options_p));
	const auto& options = bind_data->options;

	if (options->explicit_cardinality) {
		const auto file_count               = multi_file_data.file_list->GetTotalFileCount();
		bind_data->initial_file_cardinality = options->explicit_cardinality / (file_count ? file_count : 1);
	}
	return std::move(bind_data);
}

void FastLanesMultiFileInfo::BindReader(ClientContext&       context,
                                        vector<LogicalType>& return_types,
                                        vector<string>&      names,
                                        MultiFileBindData&   bind_data) {
	const auto& bind = bind_data.bind_data->Cast<FastLanesReadBindData>();
	if (bind_data.file_options.union_by_name) {
		bind_data.reader_bind = bind_data.multi_file_reader->BindUnionReader(
		    context, return_types, names, *bind_data.file_list, bind_data, *bind.options, bind_data.file_options);
	} else {
		bind_data.reader_bind = bind_data.multi_file_reader->BindReader(
		    context, return_types, names, *bind_data.file_list, bind_data, *bind.options, bind_data.file_options);
	}
}

void FastLanesReader::GetPartitionStats(vector<PartitionStatistics>& result) const {
	// TODO: How do we deal with row group filtering? (Potential FIXME)
	const auto n_row_groups = table_metadata->RowGroupCount();
	result.reserve(n_row_groups);

	idx_t offset = 0;
	for (idx_t rowgroup_idx {0}; rowgroup_idx < n_row_groups; ++rowgroup_idx) {
		auto& rg_descriptor = table_metadata->RowGroupDescriptor(rowgroup_idx);

		PartitionStatistics partition_stats;
		partition_stats.row_start  = offset;
		partition_stats.count      = rg_descriptor.m_n_tuples();
		partition_stats.count_type = CountType::COUNT_EXACT;
		offset += partition_stats.count;
		result.emplace_back(partition_stats);
	}
}

void FastLanesMultiFileInfo::FinalizeBindData(MultiFileBindData& multi_file_data) {
	auto& bind_data = multi_file_data.bind_data->Cast<FastLanesReadBindData>();
	if (multi_file_data.initial_reader) {
		auto& initial_reader               = multi_file_data.initial_reader->Cast<FastLanesReader>();
		bind_data.initial_file_cardinality = initial_reader.GetTotalTuples();
		bind_data.initial_file_n_rowgroups = initial_reader.GetNRowGroups();
		if (bind_data.options) {
			bind_data.options->file_row_number = initial_reader.GetOptions().file_row_number;
		}
	} else {
		bind_data.initial_file_cardinality = 0;
		bind_data.initial_file_n_rowgroups = 0;
	}
}

optional_idx FastLanesMultiFileInfo::MaxThreads(const MultiFileBindData&    bind_data_p,
                                                const MultiFileGlobalState& global_state_p,
                                                const FileExpandResult      expand_result) {
	// If we have multiple files, we launch the maximum number of threads, this prevents situations where the first
	// file is small or empty, leading to a single thread running the query.
	if (expand_result == FileExpandResult::MULTIPLE_FILES) {
		return optional_idx();
	}

	const auto& bind_data = bind_data_p.bind_data->Cast<FastLanesReadBindData>();
	return MaxValue(bind_data.initial_file_n_rowgroups, static_cast<idx_t>(1));
}

shared_ptr<BaseFileReader> FastLanesMultiFileInfo::CreateReader(ClientContext&,
                                                                GlobalTableFunctionState& global_state_p,
                                                                BaseUnionData&            union_data_p,
                                                                const MultiFileBindData&) {
	auto& union_data = union_data_p.Cast<FastLanesUnionData>();
	return make_shared_ptr<FastLanesReader>(union_data.file, union_data.options);
}

shared_ptr<BaseFileReader> FastLanesMultiFileInfo::CreateReader(ClientContext&,
                                                                GlobalTableFunctionState& global_state_p,
                                                                const OpenFileInfo&       file,
                                                                idx_t                     file_idx,
                                                                const MultiFileBindData&  bind_data_p) {
	const auto& bind_data = bind_data_p.bind_data->Cast<FastLanesReadBindData>();
	return make_shared_ptr<FastLanesReader>(file, *bind_data.options);
}

shared_ptr<BaseFileReader> FastLanesMultiFileInfo::CreateReader(ClientContext&,
                                                                const OpenFileInfo&    file,
                                                                BaseFileReaderOptions& options_p,
                                                                const MultiFileOptions&) {
	auto& options = options_p.Cast<FastLanesFileReaderOptions>();
	return make_shared_ptr<FastLanesReader>(file, options);
};

void FastLanesReader::PrepareReader(ClientContext& context, GlobalTableFunctionState&) {
}

unique_ptr<GlobalTableFunctionState>
FastLanesMultiFileInfo::InitializeGlobalState(ClientContext&, MultiFileBindData&, MultiFileGlobalState&) {
	return make_uniq<FastLanesReadGlobalState>();
}

unique_ptr<LocalTableFunctionState> FastLanesMultiFileInfo::InitializeLocalState(ExecutionContext&,
                                                                                 GlobalTableFunctionState&) {
	return make_uniq<FastLanesReadLocalState>();
}

bool FastLanesReader::TryInitializeScan(ClientContext&            ctx,
                                        GlobalTableFunctionState& global_state_p,
                                        LocalTableFunctionState&  local_state_p) {
	auto& global_state = global_state_p.Cast<FastLanesReadGlobalState>();
	auto& local_state  = local_state_p.Cast<FastLanesReadLocalState>();

	const auto& rowgroups_to_scan       = GetRowGroupsToScan();
	const auto  selected_rowgroup_count = rowgroups_to_scan.size();
	if (global_state.next_rowgroup >= selected_rowgroup_count) {
		// If the index of the next row group falls outside the range of the filtered rowgroup vector we are done.
		return false;
	}

	// Prepare the local state of the current worker by assigning it a row group.
	local_state.cur_rowgroup     = rowgroups_to_scan[global_state.next_rowgroup];
	local_state.row_group_reader = CreateRowGroupReader(local_state.cur_rowgroup);
	local_state.row_group_base   = row_group_offsets[local_state.cur_rowgroup];
	// Reset the row group related local state.
	local_state.cur_vector = 0;
	local_state.n_vectors  = GetNTuples(local_state.cur_rowgroup);

	// Filters are equal across a file, only construct them once, if they exist.
	if (filters && !local_state.adaptive_filter && local_state.scan_filters.empty()) {
		local_state.adaptive_filter = make_uniq<AdaptiveFilter>(*filters);
		for (auto& [fst, snd] : filters->filters) {
			local_state.scan_filters.emplace_back(ctx, fst, *snd);
		}

		local_state.filters_by_col.assign(column_ids.size(), {});
		for (auto& f : local_state.scan_filters) {
			if (const idx_t col = f.filter_idx; col < column_ids.size()) {
				local_state.filters_by_col[col].push_back(&f);
			}
		}
	}

	if (filters) {
		// Reset so there is no conflicting filter metadata between row groups.
		for (auto& filter : local_state.scan_filters) {
			filter.ctx.Reset();
		}
	}

	local_state.physical_projection_map.clear();
	local_state.physical_projection_map.resize(column_ids.size());
	idx_t physical_projection_count = 0;
	for (idx_t i {0}; i < column_ids.size(); ++i) {
		const auto local_column_id = column_ids[MultiFileLocalIndex(i)].GetId();
		if (IsFileRowNumberColumn(local_column_id)) {
			continue;
		}
		local_state.physical_projection_map[i] = optional_idx(physical_projection_count);
		physical_projection_count++;
	}

	if (local_state.column_decoders.size() != column_ids.size()) {
		local_state.column_decoders.clear();
		local_state.column_decoders.resize(column_ids.size());
		for (idx_t i {0}; i < column_ids.size(); ++i) {
			if (local_state.physical_projection_map[i].IsValid()) {
				local_state.column_decoders[i] = make_uniq<materializer::ColumnDecoder>();
			}
		}
	} else {
		for (auto& decoder : local_state.column_decoders) {
			if (decoder) {
				decoder->Reset();
			}
		}
	}

	if (physical_projection_count > 0) {
		const auto& expressions = local_state.row_group_reader->get_chunk(0);
		for (idx_t i {0}; i < column_ids.size(); ++i) {
			auto physical_index = local_state.physical_projection_map[i];
			if (!physical_index.IsValid()) {
				continue;
			}
			auto&      type = columns[column_ids[MultiFileLocalIndex(i)].GetId()].type;
			const auto expr = expressions[physical_index.GetIndex()];
			expr->PointTo(0);
			auto& op = expr->operators[expr->operators.size() - 1];
			if (filters) {
				local_state.column_decoders[i]->Init(op, type, &local_state.filters_by_col[i]);
			} else {
				local_state.column_decoders[i]->Init(op, type, nullptr);
			}
		}
	}

	// Mark the row group as consumed.
	global_state.next_rowgroup++;

	return true;
}

void FastLanesReader::Scan(ClientContext& context,
                           GlobalTableFunctionState&,
                           LocalTableFunctionState& local_state_p,
                           DataChunk&               chunk) {
	auto& local_state = local_state_p.Cast<FastLanesReadLocalState>();

	while (true) {
		const auto  cur_vec     = local_state.cur_vector;
		const idx_t start_tuple = cur_vec * fastlanes::CFG::VEC_SZ;
		if (start_tuple >= local_state.n_vectors) {
			return;
		}

		const idx_t tuples_left = local_state.n_vectors - start_tuple;
		const idx_t count       = std::min(tuples_left, fastlanes::CFG::VEC_SZ * 2);
		const auto  n_vectors   = (count + fastlanes::CFG::VEC_SZ - 1) / fastlanes::CFG::VEC_SZ;
		if (!n_vectors) {
			return;
		}

		const bool has_physical_columns = std::any_of(local_state.physical_projection_map.begin(),
		                                              local_state.physical_projection_map.end(),
		                                              [](const optional_idx& entry) { return entry.IsValid(); });

		// Try to fill up to the standard vector size.
		for (idx_t batch_idx = 0; batch_idx < n_vectors; batch_idx++) {
			const idx_t vector_idx = cur_vec + batch_idx;
			if (has_physical_columns) {
				const auto& rowgroup_descriptor = table_metadata->RowGroupDescriptor(local_state.cur_rowgroup);
				const auto& expressions         = local_state.row_group_reader->get_chunk(vector_idx);
				for (idx_t i = 0; i < column_ids.size(); i++) {
					auto physical_entry = local_state.physical_projection_map[i];
					if (!physical_entry.IsValid()) {
						continue;
					}
					auto&      target_col = chunk.data[i];
					const auto expr       = expressions[physical_entry.GetIndex()];

					expr->PointTo(vector_idx);
					auto& op = expr->operators[expr->operators.size() - 1];

					const auto& src_type = rowgroup_descriptor.m_column_descriptors()
					                           ->Get(column_ids[MultiFileLocalIndex(i)].GetId())
					                           ->data_type();

					if (batch_idx == 0) {
						local_state.column_decoders[i]->Decode<materializer::Pass::First>(
						    op, src_type, target_col, vector_idx);
					} else {
						local_state.column_decoders[i]->Decode<materializer::Pass::Second>(
						    op, src_type, target_col, vector_idx);
					}
				}
			}
		}

		const idx_t row_start = local_state.row_group_base + start_tuple;
		for (idx_t i = 0; i < column_ids.size(); i++) {
			const auto local_column_id = column_ids[MultiFileLocalIndex(i)].GetId();
			if (!IsFileRowNumberColumn(local_column_id)) {
				continue;
			}
			auto& target_col = chunk.data[i];
			// TODO: This can probably be removed.
			target_col.SetVectorType(VectorType::FLAT_VECTOR);
			FlatVector::Validity(target_col).Reset();
			auto data = FlatVector::GetData<int64_t>(target_col);
			for (idx_t row = 0; row < count; ++row) {
				data[row] = static_cast<int64_t>(row_start + row);
			}
		}

		chunk.SetCardinality(count);

		FilterExecutor::Apply(chunk, local_state.adaptive_filter, local_state.scan_filters, filters.get());

		local_state.cur_vector += n_vectors;
		vectors_read += n_vectors;

		if (chunk.size() == 0) {
			chunk.Reset();
			continue;
		}

		return;
	}
}

void FastLanesMultiFileInfo::FinishReading(ClientContext&            context,
                                           GlobalTableFunctionState& global_state_p,
                                           LocalTableFunctionState&  local_state_p) {
}

void FastLanesReader::FinishFile(ClientContext& context, GlobalTableFunctionState& global_state_p) {
	auto& g_state = global_state_p.Cast<FastLanesReadGlobalState>();

	// Reset progression trackers of the current file.
	g_state.next_rowgroup = 0;
}

unique_ptr<NodeStatistics> FastLanesMultiFileInfo::GetCardinality(const MultiFileBindData& bind_data_p,
                                                                  idx_t                    file_count) {
	const auto& bind_data = bind_data_p.bind_data->Cast<FastLanesReadBindData>();

	if (auto explicit_cardinality = bind_data.options->explicit_cardinality) {
		return make_uniq<NodeStatistics>(explicit_cardinality, explicit_cardinality);
	}
	if (auto cardinality = bind_data.initial_file_cardinality; file_count == 1 && cardinality) {
		return make_uniq<NodeStatistics>(cardinality, cardinality);
	}
	// Fallback when the first file does not contain any data.
	return make_uniq<NodeStatistics>(MaxValue(bind_data.initial_file_cardinality, static_cast<idx_t>(42)) * file_count);
}

double FastLanesReader::GetProgressInFile(ClientContext& context) {
	return 100.0 * (static_cast<double>(vectors_read.load()) / static_cast<double>(GetTotalVectors()));
}

void FastLanesMultiFileInfo::GetVirtualColumns(ClientContext&        context,
                                               MultiFileBindData&    bind_data_p,
                                               virtual_column_map_t& result) {
	result.insert(make_pair(MultiFileReader::COLUMN_IDENTIFIER_FILE_ROW_NUMBER,
	                        TableColumn("file_row_number", LogicalType::BIGINT)));
}

} // namespace duckdb
