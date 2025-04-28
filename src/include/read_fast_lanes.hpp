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
	idx_t initial_file_n_vectors;
};

struct FastLanesReadLocalState : public LocalTableFunctionState {
	// OLD
	// fastlanes::Connection conn;
	// fastlanes::up<fastlanes::Reader> reader;
	// fastlanes::up<fastlanes::Rowgroup> row_group;
	// fastlanes::up<fastlanes::Materializer> materializer;
	// END OLD
	//! Vector for which the current thread is responsible.
	idx_t cur_vector;
	idx_t to_vector;
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

	//! Index of the vector within the current file that is staged for scanning.
	idx_t cur_vector;
	//! TODO: Index if the row group within the current file that is staged for scanning.
	idx_t cur_row_group;
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
	FastLanesReader(OpenFileInfo file_p);
	~FastLanesReader() override;

	// TODO: What does this do?
	string GetReaderType() const override {
		return "FastLanes";
	}

	idx_t GetNVectors() const;
	idx_t GetNRows() const;
	std::vector<std::shared_ptr<fastlanes::PhysicalExpr>> GetChunk(idx_t vec_idx) const;

private:
	fastlanes::Connection conn;
	unique_ptr<fastlanes::Reader> reader;
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

} // namespace duckdb
