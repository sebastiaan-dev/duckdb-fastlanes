#define DUCKDB_EXTENSION_MAIN

#include "fastlanes_extension.hpp"
#include "duckdb.hpp"
#include "read_fls.hpp"
#include "write_fls.hpp"

namespace duckdb {

static void LoadInternal(ExtensionLoader& loader) {
	ReadFastLanes::Register(loader);
	WriteFastLanes::Register(loader);
}

void FastlanesExtension::Load(ExtensionLoader& loader) {
	LoadInternal(loader);
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

DUCKDB_CPP_EXTENSION_ENTRY(fastlanes, loader) {
	duckdb::LoadInternal(loader);
}
}