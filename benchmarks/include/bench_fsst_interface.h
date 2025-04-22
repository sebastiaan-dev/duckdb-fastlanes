#pragma once
#include "runner.hpp"
#include "duckdb.hpp"
#include "fls/connection.hpp"

#include <filesystem>

class BMContext {
public:
	BMContext() : conn(), db(nullptr), db_conn(db) {};

public:
	// FastLanes
	fastlanes::Connection conn;
	fastlanes::sp<fastlanes::dec_fsst_opr> opr;

	// DuckDB
	duckdb::DuckDB db;
	duckdb::Connection db_conn;
	duckdb::DataChunk output;
	duckdb::Vector *target_col;
};

struct str_inlined {
	uint32_t length;
	char inlined[12];
};

struct str_pointer {
	uint32_t length;
	char prefix[4];
	char *ptr;
};

/**
 * Decodes into a temporary buffer, then copies using the buffer API.
 */
void BM_buffer_tmp_copy(const State &state, const std::filesystem::path &path);
/**
 * Decodes into a temporary buffer, then directly manages the memory layout.
 */
void BM_mem_tmp_copy(const State &state, const std::filesystem::path &path);
/**
 * Directly decodes into memory owned by the extension, uses an extra value in the FastLanes file format storing the
 * total size required for the decoded values of the vector.
 */
void BM_mem_total_size(const State &state, const std::filesystem::path &path);
