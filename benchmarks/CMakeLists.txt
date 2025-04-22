cmake_minimum_required(VERSION 3.5)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_POSITION_INDEPENDENT_CODE TRUE)

project(duckdb_benchmarks)

# Include DuckDB Headers
include_directories(${CMAKE_SOURCE_DIR}/duckdb/src/include)

# Link against DuckDB library
add_executable(benchmark benchmark.cpp generate_data.cpp bench_duckdb_vectorbuffer.cpp bench_fsst_interface.cpp)
#add_executable(generate generate_data.cpp)

# Ensure DuckDB is linked correctly
target_link_libraries(benchmark PRIVATE duckdb)
#target_link_libraries(generate PRIVATE duckdb)