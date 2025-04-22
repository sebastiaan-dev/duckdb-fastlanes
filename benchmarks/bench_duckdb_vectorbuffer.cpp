#include "include/bench_duckdb_vectorbuffer.hpp"


#include "duckdb.hpp"
#include "fls/cfg/cfg.hpp"

void BM_allocate_empty_string_12b_vector_buffer(const State &state) {
	auto buffer = duckdb::make_buffer<duckdb::VectorStringBuffer>();

	// DuckDB default vector size
	constexpr size_t n_entries = 2048;

	for (auto _: state.n_iterations) {
		for (auto i {0}; i < n_entries; ++i) {
			buffer->EmptyString(13);
		}
	}
}


void BM_allocate_empty_string_max_vector_buffer(const State &state) {
	auto buffer = duckdb::make_buffer<duckdb::VectorStringBuffer>();

	// DuckDB default vector size
	constexpr size_t n_entries = 2048;

	for (auto _: state.n_iterations) {
		for (auto i {0}; i < n_entries; ++i) {
			buffer->EmptyString(fastlanes::CFG::String::max_bytes_per_string);
		}
	}
}
