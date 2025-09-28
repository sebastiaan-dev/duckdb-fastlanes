#pragma once

#include <duckdb/main/database.hpp>

namespace duckdb {

class ReadFastLanes {
public:
	/**
	 * @brief Register is responsible for registering functions available to the end user. It furthermore configures
	 * optional functionality associated with certain functions.
	 *
	 * @param db The currently running DuckDB database.
	 */
	static void Register(DatabaseInstance& db);
};
} // namespace duckdb
