include_directories(${CMAKE_SOURCE_DIR}/duckdb/src/include)

add_executable(benchmark benchmark.cpp generate_data.cpp bench_duckdb_vectorbuffer.cpp bench_fsst_interface.cpp)

target_link_libraries(benchmark PRIVATE duckdb FastLanes)
