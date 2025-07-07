#define DUCKDB_EXTENSION_MAIN

#include "fastlanes_extension.hpp"
#include "duckdb.hpp"
#include "duckdb/main/extension_util.hpp"
#include <read_fls.hpp>
#include <write_fls.hpp>

namespace duckdb {

static void LoadInternal(DatabaseInstance &instance) {
	ReadFastLanes::Register(instance);
	WriteFastLanes::Register(instance);
}

void FastlanesExtension::Load(DuckDB &db) {
	LoadInternal(*db.instance);
}
std::string FastlanesExtension::Name() {
	return "fastlanes";
}

std::string FastlanesExtension::Version() const {
#ifdef EXT_VERSION_FASTLANES
	return EXT_VERSION_FASTLANES;
#else
	return "";
#endif
}

} // namespace duckdb

extern "C" {

DUCKDB_EXTENSION_API void fastlanes_init(duckdb::DatabaseInstance &db) {
    duckdb::DuckDB db_wrapper(db);
    db_wrapper.LoadExtension<duckdb::FastlanesExtension>();
}

DUCKDB_EXTENSION_API const char *fastlanes_version() {
	return duckdb::DuckDB::LibraryVersion();
}
}

#ifndef DUCKDB_EXTENSION_MAIN
#error DUCKDB_EXTENSION_MAIN not defined
#endif
