#include "reader/fls_multi_file_info.hpp"
#include "duckdb/planner/table_filter_state.hpp"
#include "reader/fls_reader.hpp"
#include <algorithm>
#include <duckdb/execution/adaptive_filter.hpp>
#include <fls/expression/physical_expression.hpp>
#include <iostream>
#include <thread>

namespace duckdb {

unique_ptr<MultiFileReaderInterface>
FastLanesMultiFileInfo::InitializeInterface(ClientContext& context, MultiFileReader& reader, MultiFileList& file_list) {
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
	const auto& bind      = bind_data.bind_data->Cast<FastLanesReadBindData>();
	bind_data.reader_bind = bind_data.multi_file_reader->BindReader(
	    context, return_types, names, *bind_data.file_list, bind_data, *bind.options, bind_data.file_options);
}

shared_ptr<BaseUnionData> FastLanesReader::GetUnionData(idx_t file_idx) {
	std::cout << "Union Data" << '\n';

	return nullptr;
}

void FastLanesMultiFileInfo::FinalizeBindData(MultiFileBindData& multi_file_data) {
	auto& bind_data = multi_file_data.bind_data->Cast<FastLanesReadBindData>();
	// If we are not using union-by-name, there must be an initial reader from which we learn the schema.
	if (!multi_file_data.file_options.union_by_name) {
		D_ASSERT(multi_file_data.initial_reader);

		const auto& initial_reader         = multi_file_data.initial_reader->Cast<FastLanesReader>();
		bind_data.initial_file_cardinality = initial_reader.GetTotalTuples();
		bind_data.initial_file_n_rowgroups = initial_reader.GetNRowGroups();

		return;
	}

	throw std::runtime_error("Union by name is not supported");
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
                                                                BaseUnionData&            union_data,
                                                                const MultiFileBindData&) {
	return make_shared_ptr<FastLanesReader>(union_data.file);
}

shared_ptr<BaseFileReader> FastLanesMultiFileInfo::CreateReader(ClientContext&,
                                                                GlobalTableFunctionState& global_state_p,
                                                                const OpenFileInfo&       file,
                                                                idx_t                     file_idx,
                                                                const MultiFileBindData&) {
	return make_shared_ptr<FastLanesReader>(file);
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
	// Reset the row group related local state.
	local_state.cur_vector = 0;

	// Filters are equal across a file, only construct them once, if they exist.
	if (filters && !local_state.adaptive_filter && local_state.scan_filters.empty()) {
		local_state.adaptive_filter = make_uniq<AdaptiveFilter>(*filters);
		for (auto& [fst, snd] : filters->filters) {
			local_state.scan_filters.emplace_back(ctx, fst, *snd);
		}

		local_state.filters_by_col.assign(column_ids.size(), {});
		for (auto& f : local_state.scan_filters) {
			const idx_t col = f.filter_idx;
			if (col < column_ids.size()) {
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

	// Create column decoders for each column, if they exist reset them as row groups can have differing cross-vector
	// state.
	if (local_state.column_decoders.size() == 0) {
		for (idx_t i {0}; i < column_ids.size(); ++i) {
			local_state.column_decoders.push_back(make_uniq<materializer::ColumnDecoder>());
		}
	} else {
		for (const auto& decoder : local_state.column_decoders) {
			decoder->Reset();
		}
	}

	// Do an initial pass to initialize the column decoders with the correct state.
	for (idx_t i {0}; i < column_ids.size(); ++i) {
		auto&       type        = columns[column_ids[MultiFileLocalIndex(i)].GetId()].type;
		const auto& expressions = local_state.row_group_reader->get_chunk(0);
		const auto  expr        = expressions[i];
		expr->PointTo(0);
		auto& op = expr->operators[expr->operators.size() - 1];
		if (filters) {
			local_state.column_decoders[i]->Init(op, type, &local_state.filters_by_col[i]);
		} else {
			local_state.column_decoders[i]->Init(op, type, nullptr);
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
		const idx_t n_tuples    = GetNTuples(local_state.cur_rowgroup);
		const idx_t start_tuple = cur_vec * fastlanes::CFG::VEC_SZ;
		if (start_tuple >= n_tuples) {
			return;
		}

		const idx_t tuples_left = n_tuples - start_tuple;
		const idx_t count       = std::min(tuples_left, fastlanes::CFG::VEC_SZ * 1);
		const auto  n_vectors   = (count + fastlanes::CFG::VEC_SZ - 1) / fastlanes::CFG::VEC_SZ;
		if (!n_vectors) {
			return;
		}

		// Try to fill up to the standard vector size.
		for (idx_t batch_idx = 0; batch_idx < n_vectors; batch_idx++) {
			const idx_t vector_idx  = cur_vec + batch_idx;
			const auto& expressions = local_state.row_group_reader->get_chunk(vector_idx);

			for (idx_t i = 0; i < column_ids.size(); i++) {
				// These indexes map to the same column due to a mapping in CreateRowGroupReader().
				auto&      target_col = chunk.data[i];
				const auto expr       = expressions[i];

				expr->PointTo(vector_idx);
				auto& op = expr->operators[expr->operators.size() - 1];

				if (batch_idx == 0) {
					local_state.column_decoders[i]->Decode<materializer::Pass::First>(op, target_col, vector_idx);
				} else {
					local_state.column_decoders[i]->Decode<materializer::Pass::Second>(op, target_col, vector_idx);
				}
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
	auto& bind_data = bind_data_p.bind_data->Cast<FastLanesReadBindData>();
	if (bind_data.options->explicit_cardinality) {
		return make_uniq<NodeStatistics>(bind_data.options->explicit_cardinality);
	}
	// Fallback when the first file does not contain any data.
	return make_uniq<NodeStatistics>(MaxValue(bind_data.initial_file_cardinality, static_cast<idx_t>(42)) * file_count);
}

double FastLanesReader::GetProgressInFile(ClientContext& context) {
	const auto read_vectors = vectors_read.load();
	return 100.0 * (static_cast<double>(read_vectors * fastlanes::CFG::VEC_SZ) / static_cast<double>(GetTotalTuples()));
}

void FastLanesMultiFileInfo::GetVirtualColumns(ClientContext&        context,
                                               MultiFileBindData&    bind_data_p,
                                               virtual_column_map_t& result) {
	result.insert(make_pair(MultiFileReader::COLUMN_IDENTIFIER_FILE_ROW_NUMBER,
	                        TableColumn("file_row_number", LogicalType::BIGINT)));
}

unique_ptr<BaseStatistics> FastLanesReader::GetStatistics(ClientContext& context, const string& name) {
	return nullptr;
}

} // namespace duckdb
