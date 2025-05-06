#include "reader/fast_lanes_multi_file_info.hpp"

#include <fls/expression/fsst_expression.hpp>
#include <syncstream>
#include "fls/expression/fsst12_expression.hpp"
#include "fls/primitive/copy/fls_copy.hpp"
#include "reader/fast_lanes_reader.hpp"
#include "reader/materializer.hpp"

#include <thread>
#include <iostream>
#include <fls/expression/physical_expression.hpp>
#include <fls/connection.hpp>
#include <fls/expression/alp_expression.hpp>

namespace duckdb {

unique_ptr<BaseFileReaderOptions> FastLanesMultiFileInfo::InitializeOptions(ClientContext &context,
                                                                            optional_ptr<TableFunctionInfo> info) {
	return make_uniq<BaseFileReaderOptions>();
}

bool FastLanesMultiFileInfo::ParseOption(ClientContext &context, const string &key, const Value &val,
                                         MultiFileOptions &file_options, BaseFileReaderOptions &options) {
	return false;
}

void FastLanesMultiFileInfo::GetBindInfo(const TableFunctionData &bind_data, BindInfo &info) {
}

unique_ptr<TableFunctionData> FastLanesMultiFileInfo::InitializeBindData(MultiFileBindData &multi_file_data,
                                                                         unique_ptr<BaseFileReaderOptions> options) {
	return make_uniq<FastLanesReadBindData>();
}

void FastLanesMultiFileInfo::BindReader(ClientContext &context, vector<LogicalType> &return_types,
                                        vector<string> &names, MultiFileBindData &bind_data) {
	BaseFileReaderOptions options;
	bind_data.reader_bind = bind_data.multi_file_reader->BindReader<FastLanesMultiFileInfo>(
	    context, return_types, names, *bind_data.file_list, bind_data, options, bind_data.file_options);
}

shared_ptr<BaseUnionData> FastLanesMultiFileInfo::GetUnionData(shared_ptr<BaseFileReader> scan_p, idx_t file_idx) {
	std::cout << "Union Data" << '\n';

	return nullptr;
}

void FastLanesMultiFileInfo::FinalizeBindData(const MultiFileBindData &multi_file_data) {
	auto &bind_data = multi_file_data.bind_data->Cast<FastLanesReadBindData>();
	// If we are not using union-by-name, there must be an initial reader from which we learn the schema.
	if (!multi_file_data.file_options.union_by_name) {
		D_ASSERT(multi_file_data.initial_reader);

		const auto &initial_reader = multi_file_data.initial_reader->Cast<FastLanesReader>();
		bind_data.initial_file_cardinality = initial_reader.GetNRows();
		bind_data.initial_file_n_rowgroups = initial_reader.GetNRowGroups();

		return;
	}

	throw std::runtime_error("Union by name is not supported");
}

optional_idx FastLanesMultiFileInfo::MaxThreads(const MultiFileBindData &bind_data_p,
                                                const MultiFileGlobalState &global_state,
                                                const FileExpandResult expand_result) {
	// If we have multiple files, we launch the maximum number of threads, this prevents situations where the first
	// file is small or empty, leading to a single thread running the query.
	if (expand_result == FileExpandResult::MULTIPLE_FILES) {
		return optional_idx();
	}

	const auto &bind_data = bind_data_p.bind_data->Cast<FastLanesReadBindData>();
	return MaxValue(bind_data.initial_file_n_rowgroups, static_cast<idx_t>(1));
}

shared_ptr<BaseFileReader> FastLanesMultiFileInfo::CreateReader(ClientContext &context,
                                                                GlobalTableFunctionState &gstate,
                                                                BaseUnionData &union_data,
                                                                const MultiFileBindData &bind_data_p) {
	return make_shared_ptr<FastLanesReader>(union_data.file);
}

shared_ptr<BaseFileReader> FastLanesMultiFileInfo::CreateReader(ClientContext &context,
                                                                GlobalTableFunctionState &gstate,
                                                                const OpenFileInfo &file, idx_t file_idx,
                                                                const MultiFileBindData &bind_data) {
	return make_shared_ptr<FastLanesReader>(file);
}

shared_ptr<BaseFileReader> FastLanesMultiFileInfo::CreateReader(ClientContext &context, const OpenFileInfo &file,
                                                                const BaseFileReaderOptions &options,
                                                                const MultiFileOptions &file_options) {
	return make_shared_ptr<FastLanesReader>(file);
};

void FastLanesMultiFileInfo::FinalizeReader(ClientContext &context, BaseFileReader &reader,
                                            GlobalTableFunctionState &) {
}

unique_ptr<GlobalTableFunctionState>
FastLanesMultiFileInfo::InitializeGlobalState(ClientContext &context, MultiFileBindData &bind_data_p,
                                              MultiFileGlobalState &global_state_p) {
	return make_uniq<FastLanesReadGlobalState>();
}

unique_ptr<LocalTableFunctionState> FastLanesMultiFileInfo::InitializeLocalState(ExecutionContext &context,
                                                                                 GlobalTableFunctionState &gstate) {
	return make_uniq<FastLanesReadLocalState>();
}

bool FastLanesMultiFileInfo::TryInitializeScan(ClientContext &context, shared_ptr<BaseFileReader> &reader_p,
                                               GlobalTableFunctionState &gstate_p, LocalTableFunctionState &lstate_p) {
	auto &gstate = gstate_p.Cast<FastLanesReadGlobalState>();
	auto &lstate = lstate_p.Cast<FastLanesReadLocalState>();
	auto &reader = reader_p->Cast<FastLanesReader>();

	// Check if there are noo more vectors left to scan.
	if (gstate.cur_rowgroup >= reader.GetNRowGroups()) {
		return false;
	}
	// Prepare the local state of the current thread by informing its processing responsibilities.
	lstate.cur_vector = 0;
	lstate.cur_rowgroup = gstate.cur_rowgroup;

	lstate.row_group_reader = reader.CreateRowGroupReader(lstate.cur_rowgroup);

	// Consume the rowgroup in the global state.
	gstate.cur_rowgroup++;

	return true;
}

void FastLanesMultiFileInfo::Scan(ClientContext &context, BaseFileReader &reader_p,
                                  GlobalTableFunctionState &global_state_p, LocalTableFunctionState &local_state_p,
                                  DataChunk &chunk) {
	D_ASSERT(fastlanes::CFG::VEC_SZ * 2 == STANDARD_VECTOR_SIZE);

	auto &local_state = local_state_p.Cast<FastLanesReadLocalState>();
	const auto &reader = reader_p.Cast<FastLanesReader>();
	const auto cur_vec = local_state.cur_vector;
	const auto n_vectors = reader.GetNVectors(local_state.cur_rowgroup);

	if (cur_vec >= n_vectors) {
		return;
	}

	idx_t batch_size = 1;
	if (cur_vec + 1 < n_vectors) {
		batch_size = 2;
	}

	for (idx_t batch_idx = 0; batch_idx < batch_size; batch_idx++) {
		const idx_t vector_idx = cur_vec + batch_idx;
		const auto &expressions = local_state.row_group_reader->get_chunk(vector_idx);

		// ColumnCount is defined during the bind of the table function.
		for (idx_t col_idx = 0; col_idx < chunk.ColumnCount(); col_idx++) {
			auto &target_col = chunk.data[col_idx];
			const auto expr = expressions[col_idx];

			expr->PointTo(vector_idx);
			std::visit(material_visitor {vector_idx, target_col}, expr->operators[expr->operators.size() - 1]);
		}
	}

	chunk.SetCardinality(fastlanes::CFG::VEC_SZ * batch_size);
	local_state.cur_vector += batch_size;
}

void FastLanesMultiFileInfo::FinishReading(ClientContext &context, GlobalTableFunctionState &global_state,
                                           LocalTableFunctionState &local_state) {
}

void FastLanesMultiFileInfo::FinishFile(ClientContext &context, GlobalTableFunctionState &global_state_p,
                                        BaseFileReader &reader) {
	auto &g_state = global_state_p.Cast<FastLanesReadGlobalState>();

	// Reset progression trackers of the current file.
	g_state.cur_rowgroup = 0;
}

unique_ptr<NodeStatistics> FastLanesMultiFileInfo::GetCardinality(const MultiFileBindData &bind_data_p,
                                                                  idx_t file_count) {
	auto &bind_data = bind_data_p.bind_data->Cast<FastLanesReadBindData>();
	// Fallback when the first file does not contain any data.
	return make_uniq<NodeStatistics>(MaxValue(bind_data.initial_file_cardinality, static_cast<idx_t>(42)) * file_count);
}

double FastLanesMultiFileInfo::GetProgressInFile(ClientContext &context, const BaseFileReader &reader) {
	return 0;
}

void FastLanesMultiFileInfo::GetVirtualColumns(ClientContext &context, MultiFileBindData &bind_data,
                                               virtual_column_map_t &result) {
}

unique_ptr<BaseStatistics> FastLanesMultiFileInfo::GetStatistics(ClientContext &context, BaseFileReader &reader_p,
                                                                 const string &name) {
	const auto &reader = reader_p.Cast<FastLanesReader>();
	unique_ptr<BaseStatistics> stats;

	const fastlanes::TableDescriptor &table_descriptor = reader.GetFileMetadata();
	for (idx_t row_group_idx = 0; row_group_idx < table_descriptor.m_rowgroup_descriptors.size(); row_group_idx++) {
		auto &row_group_descriptor = table_descriptor.m_rowgroup_descriptors[row_group_idx];
		auto &column_descriptors = row_group_descriptor.GetColumnDescriptors();
		auto column_idx = row_group_descriptor.LookUp(name);
		auto &column_descriptor = column_descriptors[column_idx];

		if (column_descriptor.n_null > 0) {
			stats->SetHasNull();
		} else {
			stats->SetHasNoNull();
		}
	}

	return stats;
}

} // namespace duckdb