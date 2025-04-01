// #include "duckdb.hpp"
// #include <iostream>
// #include <numeric>
// #include <fls/connection.hpp>
//
// void generate_fls_data(const std::filesystem::path &source_dir, const std::filesystem::path &dest_dir) {
// 	if (!exists(dest_dir)) {
// 		create_directories(dest_dir);
// 	} else {
// 		return;
// 	}
//
// 	fastlanes::Connection conn;
// 	conn.set_sample_size(1);
//
// 	conn.reset().read(source_dir).force_schema({
//
// 	});
// 	conn.to_fls(dest_dir);
// }
//
// void generate_parquet_data(duckdb::Connection &conn, const std::filesystem::path &source_dir,
// 						   const std::filesystem::path &dest_dir) {
// 	if (!exists(dest_dir)) {
// 		create_directories(dest_dir);
// 	} else {
// 		return;
// 	}
//
// 	const auto csv_file = source_dir / "data.csv";
// 	const auto parquet_file = dest_dir / "data.parquet";
//
// 	const std::string query = "COPY (SELECT * FROM read_csv_auto('" + csv_file.string() + "')) TO '" +
// 							  parquet_file.string() + "' (FORMAT 'parquet');";
//
// 	if (const auto result = conn.Query(query); result->HasError()) {
// 		std::cerr << "Error processing file " << csv_file << ": " << result->GetError() << '\n';
// 	} else {
// 		std::cout << "Successfully converted " << csv_file << " to " << parquet_file << '\n';
// 	}
// }
//
// int main() {
// 	duckdb::DuckDB db(nullptr); // In-memory instance
// 	duckdb::Connection conn(db);
// 	const std::filesystem::path base_dir =
// 			"/Users/sebastiaan/Documents/university/thesis/duckdb-fastlanes/benchmarks/data";
// 	std::array<std::string, 2> benchmarks = {"int_bool_rand_synth", "dbl_rand_synth"};
//
// 	for (const auto &benchmark : benchmarks) {
// 		generate_fls_data(base_dir / "source" / benchmark, base_dir / "fls" / benchmark);
// 		generate_parquet_data(conn, base_dir / "source" / benchmark, base_dir / "parquet" / benchmark);
// 	}
// };