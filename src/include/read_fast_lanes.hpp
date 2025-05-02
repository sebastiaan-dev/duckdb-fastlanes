#pragma once

#include "duckdb/common/multi_file/base_file_reader.hpp"
#include "duckdb/common/multi_file/multi_file_function.hpp"

#include <duckdb/main/database.hpp>
#include <fls/connection.hpp>
#include <fls/encoder/materializer.hpp>

namespace duckdb {

struct FastLanesReadBindData : public TableFunctionData {
	// OLD
	fastlanes::path directory;
	fastlanes::n_t n_vector;
	// END OLD

	//! Number of rows in the first file, used for estimating the total cardinality of the to-be-read file(s).
	idx_t initial_file_cardinality;
	//! Number of vectors in the first file, used to determine the number of threads.
	idx_t initial_file_n_rowgroups;
};

struct FastLanesReadLocalState : public LocalTableFunctionState {
	// OLD
	// fastlanes::Connection conn;
	// fastlanes::up<fastlanes::Reader> reader;
	// fastlanes::up<fastlanes::Rowgroup> row_group;
	// fastlanes::up<fastlanes::Materializer> materializer;
	// END OLD
	//! Vector for which the current thread is responsible.
	// idx_t cur_vector;
	// idx_t to_vector;
	idx_t cur_vector;
	//! Rowgroup which is currenty being processed.
	idx_t cur_rowgroup;
	fastlanes::up<fastlanes::RowgroupReader> row_group_reader;
};

struct FastLanesReadGlobalState : public GlobalTableFunctionState {
	// OLD
	uint16_t vec_sz;
	// Exponent of base 2, representing the vector size.
	uint16_t vec_sz_exp;
	fastlanes::n_t n_vector;
	// atomic<fastlanes::n_t> cur_vector;
	vector<uint8_t> byte_arr_vec;
	// END OLD

	//! TODO: Index if the row group within the current file that is staged for scanning.
	idx_t cur_rowgroup;
};

template <class OP>
class FastLanesMultiFileFunction : public MultiFileFunction<OP> {
public:
	explicit FastLanesMultiFileFunction(string name_p) : MultiFileFunction<OP>(std::move(name_p)) {
		// Override the bind function pointer to use our custom implementation
		this->bind = CustomMultiFileBind;
	}

private:
	// Custom bind method – replace or extend logic here
	static unique_ptr<FunctionData> CustomMultiFileBind(ClientContext &context, TableFunctionBindInput &input,
	                                                    vector<LogicalType> &return_types, vector<string> &names) {
		// Insert any pre-bind logic here

		// Delegate to the base class's bind by default
		auto result = MultiFileFunction<OP>::MultiFileBind(context, input, return_types, names);

		// Insert any post-bind logic here

		return result;
	}
};

/**
 * Provides an abstraction over the FastLanes file format which allows for implicit multithreaded support with DuckDB.
 */
class FastLanesReader : public BaseFileReader {
public:
	explicit FastLanesReader(OpenFileInfo file_p);
	~FastLanesReader() override;

	// TODO: What does this do?
	string GetReaderType() const override {
		return "FastLanes";
	}

	fastlanes::TableDescriptor& GetFileMetadata() const;
	idx_t GetNRowGroups() const;
	idx_t GetNRows() const;
	idx_t GetNVectors(idx_t row_group_idx) const;
	fastlanes::up<fastlanes::RowgroupReader> CreateRowGroupReader(idx_t rowgroup_idx);

private:
	//! Path of the directory containing both the FastLanes data file and metadata file.
	std::filesystem::path dir_path;
	fastlanes::Connection conn;
	unique_ptr<fastlanes::TableReader> table_reader;
};

class ReadFastLanes {
public:
	/**
	 * @brief Register is responsible for registering functions available to the end user. It furthermore configures
	 * optional functionality associated with certain functions.
	 *
	 * When registering a function the following parameters are supplied:
	 *	string name - The name of the function, determines the usage in query syntax.
	 *	vector<LogicalType> arguments - Values that can be passed to the function.
	 *	table_function_t function - The code which gets executed when calling the respective function with query syntax.
	 *	table_function_bind_t bind - Determines the return type of a table producing function (what does this mean?)
	 *	table_function_init_global_t init_global - Tracks the progress of the table producing function across threads.
	 *	table_function_init_local_t init_local - Tracks the progress of the table producing function being thread local.
	 *
	 * @param db The currently running DuckDB database.
	 */
	static void Register(DatabaseInstance &db);
	static TableFunction GetFunction();
};

/**
 * Define all the required functions from the MultiFileFunction template class.
 *
 */
struct FastLanesMultiFileInfo {
	/* ---- MultiFileBind ---- */
	static unique_ptr<BaseFileReaderOptions> InitializeOptions(ClientContext &context,
	                                                           optional_ptr<TableFunctionInfo> info);
	static bool ParseOption(ClientContext &context, const string &key, const Value &val, MultiFileOptions &file_options,
	                        BaseFileReaderOptions &options);
	/* ---- MultiFileBindInternal ---- */
	/*!
	 * Save user-provided options (currently none) and allocate FastLanesReadBindData (TableFunctionData) object.
	 */
	static unique_ptr<TableFunctionData> InitializeBindData(MultiFileBindData &multi_file_data,
	                                                        unique_ptr<BaseFileReaderOptions> options);
	static void BindReader(ClientContext &context, vector<LogicalType> &return_types, vector<string> &names,
	                       MultiFileBindData &bind_data);
	static shared_ptr<BaseUnionData> GetUnionData(shared_ptr<BaseFileReader> scan_p, idx_t file_idx);

	static void FinalizeBindData(const MultiFileBindData &multi_file_data);
	/* ---- MultiFileGetBindInfo ---- */
	static void GetBindInfo(const TableFunctionData &bind_data, BindInfo &info);

	/* ---- MultiInitGlobal ---- */
	static unique_ptr<GlobalTableFunctionState>
	InitializeGlobalState(ClientContext &context, MultiFileBindData &bind_data, MultiFileGlobalState &global_state);
	static optional_idx MaxThreads(const MultiFileBindData &bind_data, const MultiFileGlobalState &global_state,
	                               FileExpandResult expand_result);
	/* ---- MultiInitLocal ---- */
	static unique_ptr<LocalTableFunctionState> InitializeLocalState(ExecutionContext &, GlobalTableFunctionState &);

	/* ---- MultiFileScan ---- */
	static void Scan(ClientContext &context, BaseFileReader &reader, GlobalTableFunctionState &global_state,
	                 LocalTableFunctionState &local_state, DataChunk &chunk);

	/* --- TryOpenNextFile ---- */
	static shared_ptr<BaseFileReader> CreateReader(ClientContext &context, GlobalTableFunctionState &gstate,
	                                               BaseUnionData &union_data, const MultiFileBindData &bind_data);
	static shared_ptr<BaseFileReader> CreateReader(ClientContext &context, GlobalTableFunctionState &gstate,
	                                               const OpenFileInfo &file, idx_t file_idx,
	                                               const MultiFileBindData &bind_data);
	static shared_ptr<BaseFileReader> CreateReader(ClientContext &context, const OpenFileInfo &file,
	                                               const BaseFileReaderOptions &options,
	                                               const MultiFileOptions &file_options);
	static void FinalizeReader(ClientContext &context, BaseFileReader &reader, GlobalTableFunctionState &);
	/* ---- TryInitializeNextBatch ---- */
	static void FinishReading(ClientContext &context, GlobalTableFunctionState &global_state,
	                          LocalTableFunctionState &local_state);
	static bool TryInitializeScan(ClientContext &context, shared_ptr<BaseFileReader> &reader,
	                              GlobalTableFunctionState &gstate, LocalTableFunctionState &lstate);
	static void FinishFile(ClientContext &context, GlobalTableFunctionState &global_state, BaseFileReader &reader);

	/* ---- MultiFileCardinality ---- */
	/*!
	 * Estimate the cardinality of the to-be-read files, the estimate is based on the first file.
	 */
	static unique_ptr<NodeStatistics> GetCardinality(const MultiFileBindData &bind_data_p, idx_t file_count);
	/* ---- MultiFileProgress ---- */
	static double GetProgressInFile(ClientContext &context, const BaseFileReader &reader);
	/* ---- MultiFileGetVirtualColumns ---- */
	static void GetVirtualColumns(ClientContext &context, MultiFileBindData &bind_data, virtual_column_map_t &result);
	static unique_ptr<BaseStatistics> GetStatistics(ClientContext &context, BaseFileReader &reader_p, const string &name);
};

} // namespace duckdb
