#include "reader/fls_multi_file_info.hpp"
#include "duckdb/logging/log_manager.hpp"
#include "fls/expression/fsst12_expression.hpp"
#include "fls/primitive/copy/fls_copy.hpp"
#include "reader/fls_reader.hpp"
#include "reader/materializer.hpp"
#include <fls/expression/alp_expression.hpp>
#include <fls/expression/fsst_expression.hpp>
#include <fls/expression/physical_expression.hpp>
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
	return make_uniq<BaseFileReaderOptions>();
}

bool FastLanesMultiFileInfo::ParseOption(ClientContext&         context,
                                         const string&          key,
                                         const Value&           val,
                                         MultiFileOptions&      file_options,
                                         BaseFileReaderOptions& options) {
	return false;
}

void FastLanesMultiFileInfo::GetBindInfo(const TableFunctionData& bind_data, BindInfo& info) {
}

unique_ptr<TableFunctionData> FastLanesMultiFileInfo::InitializeBindData(MultiFileBindData& multi_file_data,
                                                                         unique_ptr<BaseFileReaderOptions> options) {
	return make_uniq<FastLanesReadBindData>();
}

void FastLanesMultiFileInfo::BindReader(ClientContext&       context,
                                        vector<LogicalType>& return_types,
                                        vector<string>&      names,
                                        MultiFileBindData&   bind_data) {
	BaseFileReaderOptions options;
	bind_data.reader_bind = bind_data.multi_file_reader->BindReader(
	    context, return_types, names, *bind_data.file_list, bind_data, options, bind_data.file_options);
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
                                                                const OpenFileInfo& file,
                                                                BaseFileReaderOptions&,
                                                                const MultiFileOptions&) {
	return make_shared_ptr<FastLanesReader>(file);
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

bool FastLanesReader::TryInitializeScan(ClientContext&,
                                        GlobalTableFunctionState& global_state_p,
                                        LocalTableFunctionState&  local_state_p) {
	auto& global_state = global_state_p.Cast<FastLanesReadGlobalState>();
	auto& local_state  = local_state_p.Cast<FastLanesReadLocalState>();

	// Check if there are noo more vectors left to scan.
	if (global_state.cur_rowgroup >= GetNRowGroups()) {
		return false;
	}
	// Prepare the local state of the current thread by informing its processing responsibilities.
	// TODO: put this in a reset function?
	local_state.cur_vector       = 0;
	local_state.cur_rowgroup     = global_state.cur_rowgroup;
	local_state.row_group_reader = CreateRowGroupReader(local_state.cur_rowgroup);
	local_state.is_initialized   = false;

	if (local_state.column_decoders.size() == 0) {
		for (auto& _ : columns) {
			local_state.column_decoders.push_back(make_uniq<ColumnDecoder>());
		}
	} else {
		for (const auto& decoder : local_state.column_decoders) {
			decoder->Reset();
		}
	}

	// Consume the rowgroup in the global state.
	global_state.cur_rowgroup++;

	return true;
}

void FastLanesReader::Scan(ClientContext& context,
                           GlobalTableFunctionState&,
                           LocalTableFunctionState& local_state_p,
                           DataChunk&               chunk) {
	auto&       local_state = local_state_p.Cast<FastLanesReadLocalState>();
	const auto  cur_vec     = local_state.cur_vector;
	const idx_t n_tuples    = GetNTuples(local_state.cur_rowgroup);
	const idx_t start_tuple = cur_vec * fastlanes::CFG::VEC_SZ;
	if (start_tuple >= n_tuples)
		return;

	const idx_t tuples_left = n_tuples - start_tuple;
	const idx_t count       = std::min(tuples_left, fastlanes::CFG::VEC_SZ * 2);
	const auto  n_vectors   = (count + fastlanes::CFG::VEC_SZ - 1) / fastlanes::CFG::VEC_SZ;
	if (!n_vectors)
		return;

	for (idx_t batch_idx = 0; batch_idx < n_vectors; batch_idx++) {
		const idx_t vector_idx  = cur_vec + batch_idx;
		const auto& expressions = local_state.row_group_reader->get_chunk(vector_idx);

		for (idx_t i = 0; i < column_ids.size(); i++) {
			const auto col_idx    = column_ids[MultiFileLocalIndex(i)].GetId();
			auto&      target_col = chunk.data[i];

			const auto expr = expressions[col_idx];
			expr->PointTo(vector_idx);
			auto& op = expr->operators[expr->operators.size() - 1];

			if (!local_state.is_initialized) {
				local_state.column_decoders[i]->Init(op, target_col);
			}

			if (batch_idx == 0) {
				local_state.column_decoders[i]->Decode<Pass::First>(op, target_col, vector_idx);
			} else {
				local_state.column_decoders[i]->Decode<Pass::Second>(op, target_col, vector_idx);
			}
		}

		local_state.is_initialized = true;
	}

	chunk.SetCardinality(count);
	local_state.cur_vector += n_vectors;
	vectors_read += n_vectors;
}

void FastLanesMultiFileInfo::FinishReading(ClientContext&            context,
                                           GlobalTableFunctionState& global_state_p,
                                           LocalTableFunctionState&  local_state_p) {
}

void FastLanesReader::FinishFile(ClientContext& context, GlobalTableFunctionState& global_state_p) {
	auto& g_state = global_state_p.Cast<FastLanesReadGlobalState>();

	// Reset progression trackers of the current file.
	g_state.cur_rowgroup = 0;
}

unique_ptr<NodeStatistics> FastLanesMultiFileInfo::GetCardinality(const MultiFileBindData& bind_data_p,
                                                                  idx_t                    file_count) {
	auto& bind_data = bind_data_p.bind_data->Cast<FastLanesReadBindData>();
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
}

unique_ptr<BaseStatistics> FastLanesReader::GetStatistics(ClientContext& context, const string& name) {
	return nullptr;
}

} // namespace duckdb
