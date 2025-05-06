#pragma once

#include "duckdb/common/multi_file/base_file_reader.hpp"
#include "duckdb/common/multi_file/multi_file_function.hpp"

#include <duckdb/main/database.hpp>
#include <fls/connection.hpp>
#include <fls/encoder/materializer.hpp>

namespace duckdb {

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
