#include "include/benchmark.hpp"

#include "duckdb.hpp"
#include <iostream>

static void generate_fls_data(const Benchmark &benchmark, fastlanes::OperatorToken schema, int32_t cols, int32_t rows) {
	const std::string &variant = Benchmark::token_to_variant.at(schema);
	const std::filesystem::path source_dir = benchmark.base_dir / "source" / (benchmark.name + "_" + variant + "_" + std::to_string(cols) + "_" + std::to_string(rows));
	const std::filesystem::path dest_dir = benchmark.base_dir / "fls" / (benchmark.name + "_" + variant + "_" + std::to_string(cols) + "_" + std::to_string(rows));

	if (!exists(dest_dir)) {
		create_directories(dest_dir);
	} else {
		return;
	}

	fastlanes::Connection conn;

	try {
		if (schema == fastlanes::OperatorToken::INVALID) {
			conn.reset().read(source_dir);
			std::cout << "Read without schema" << "\n";
		} else {
			// conn.set_sample_size(1);
			conn.reset().read(source_dir).force_schema({schema});
			std::cout << "Read with schema" << "\n";
		}
		// conn.set_sample_size(1);
		// conn.reset().read(source_dir);
		// std::cout << "Read without schema" << "\n";
	} catch (const std::exception &e) {
		std::cerr << "Failed to read from: " << source_dir << "\n";
		std::cerr << "Error: " << e.what() << "\n";
		return;
	}

	conn.to_fls(dest_dir);
	std::cout << "Wrote to FLS" << "\n";
}

static void generate_parquet_data(duckdb::Connection &conn, const Benchmark &benchmark, fastlanes::OperatorToken schema, int32_t cols, int32_t rows) {
	const std::string &variant = Benchmark::token_to_variant.at(schema);
	const std::filesystem::path source_dir = benchmark.base_dir / "source" / (benchmark.name + "_" + variant + "_" + std::to_string(cols) + "_" + std::to_string(rows));
	const std::filesystem::path dest_dir = benchmark.base_dir / "parquet" / (benchmark.name + "_" + variant + "_" + std::to_string(cols) + "_" + std::to_string(rows));

	if (!exists(dest_dir)) {
		create_directories(dest_dir);
	} else {
		return;
	}

	const auto csv_file = source_dir / "data.csv";
	const auto parquet_file = dest_dir / "data.parquet";

	const std::string query = "COPY (SELECT * FROM read_csv_auto('" + csv_file.string() + "')) TO '" +
	                          parquet_file.string() + "' (FORMAT 'parquet');";

	if (const auto result = conn.Query(query); result->HasError()) {
		std::cerr << "Error processing file " << csv_file << ": " << result->GetError() << '\n';
	} else {
		std::cout << "Successfully converted " << csv_file << " to " << parquet_file << '\n';
	}
}

void generate_data(const Benchmark &benchmark, const fastlanes::OperatorToken &variant) {
	duckdb::DuckDB db(nullptr); // In-memory instance
	duckdb::Connection conn(db);

	for (int cols = 1; cols <= 1; ++cols) {
		for (const int rows : {1024 * 1, 1024 * 32, 1024 * 64}) {
			generate_fls_data(benchmark, variant, cols, rows);
			generate_parquet_data(conn, benchmark, variant, cols, rows);
		}
	}
}
