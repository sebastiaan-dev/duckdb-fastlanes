#pragma once

#include <duckdb/main/database.hpp>

namespace duckdb {

class WriteFastLanes {
public:
	/**
	 * @brief Register is responsible for registering functions available to the end user.
	 * It registers the write_fls function that allows writing data to FastLanes format.
	 *
	 * @param db The currently running DuckDB database instance.
	 */
	static void Register(ExtensionLoader &loader);
};

} // namespace duckdb