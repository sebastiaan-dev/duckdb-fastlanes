#define DUCKDB_EXTENSION_MAIN

#include "fast_lanes_extension.hpp"
#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/main/extension_util.hpp"

#include <read_fast_lanes.hpp>
#include <duckdb/parser/parsed_data/create_scalar_function_info.hpp>


namespace duckdb {

static void LoadInternal(DatabaseInstance &instance) {
	ReadFastLanes::Register(instance);
}

void FastLanesExtension::Load(DuckDB &db) {
	LoadInternal(*db.instance);
}
std::string FastLanesExtension::Name() {
	return "fast_lanes";
}

std::string FastLanesExtension::Version() const {
#ifdef EXT_VERSION_FAST_LANES
	return EXT_VERSION_FAST_LANES;
#else
	return "";
#endif
}

} // namespace duckdb

extern "C" {

DUCKDB_EXTENSION_API void fast_lanes_init(duckdb::DatabaseInstance &db) {
    duckdb::DuckDB db_wrapper(db);
    db_wrapper.LoadExtension<duckdb::FastLanesExtension>();
}

DUCKDB_EXTENSION_API const char *fast_lanes_version() {
	return duckdb::DuckDB::LibraryVersion();
}
}

#ifndef DUCKDB_EXTENSION_MAIN
#error DUCKDB_EXTENSION_MAIN not defined
#endif
