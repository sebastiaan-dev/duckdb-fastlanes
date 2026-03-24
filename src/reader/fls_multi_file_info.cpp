#include "reader/fls_multi_file_info.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "reader/fls_reader.hpp"

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

unique_ptr<GlobalTableFunctionState>
FastLanesMultiFileInfo::InitializeGlobalState(ClientContext&, MultiFileBindData&, MultiFileGlobalState&) {
	return make_uniq<FastLanesReadGlobalState>();
}

unique_ptr<LocalTableFunctionState> FastLanesMultiFileInfo::InitializeLocalState(ExecutionContext&,
                                                                                 GlobalTableFunctionState&) {
	return make_uniq<FastLanesReadLocalState>();
}

void FastLanesMultiFileInfo::FinishReading(ClientContext&            context,
                                           GlobalTableFunctionState& global_state_p,
                                           LocalTableFunctionState&  local_state_p) {
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

void FastLanesMultiFileInfo::GetVirtualColumns(ClientContext&        context,
                                               MultiFileBindData&    bind_data_p,
                                               virtual_column_map_t& result) {
	result.insert(make_pair(MultiFileReader::COLUMN_IDENTIFIER_FILE_ROW_NUMBER,
	                        TableColumn("file_row_number", LogicalType::BIGINT)));
}

} // namespace duckdb
