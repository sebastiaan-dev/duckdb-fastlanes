# DuckDB 1.5.6 marks two deleted overloads dllexport when building its library.
# MSVC accepts that, but Clang rejects it. Deleted functions have no symbols to
# export, so removing the annotation preserves both overload resolution and ABI.
set(header "${DuckDB_SOURCE_DIR}/src/include/duckdb/storage/statistics/variant_stats.hpp")
file(READ "${header}" original)
set(patched "${original}")
foreach(declaration
        "static const BaseStatistics &GetTypedStats(const BaseStatistics &&stats) = delete;"
        "static optional_ptr<const BaseStatistics> GetUntypedStats(const BaseStatistics &&stats) = delete;")
    string(FIND "${patched}" "${declaration}" found)
    if (found EQUAL -1)
        message(FATAL_ERROR "DuckDB Clang compatibility patch does not match ${header}: ${declaration}")
    endif ()
    string(REPLACE "DUCKDB_API ${declaration}" "${declaration}" patched "${patched}")
endforeach()
if (NOT patched STREQUAL original)
    file(WRITE "${header}" "${patched}")
endif ()
