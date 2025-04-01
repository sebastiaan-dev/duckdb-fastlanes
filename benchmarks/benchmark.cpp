#include "duckdb.hpp"
#include <iostream>
#include <numeric>
#include <fls/connection.hpp>
#include <unordered_map>

/**
 * Current Limitations
 *
 * - EXP_UNCOMPRESSED_U08 is not supported.
 * - Single column with NULL values cannot be read
 */
struct Benchmark {
	static const std::filesystem::path base_dir;
	static const std::unordered_map<fastlanes::OperatorToken, std::string> token_to_variant;

	std::string name;
	std::vector<fastlanes::OperatorToken> variants;
};

const std::filesystem::path Benchmark::base_dir =
    "/Users/sebastiaan/Documents/university/thesis/duckdb-fastlanes/benchmarks/data";
const std::unordered_map<fastlanes::OperatorToken, std::string> Benchmark::token_to_variant = {
    // Uncompressed encodings
    {fastlanes::OperatorToken::EXP_UNCOMPRESSED_U08, "uint8"},
    {fastlanes::OperatorToken::EXP_UNCOMPRESSED_I08, "int8"},
    {fastlanes::OperatorToken::EXP_UNCOMPRESSED_I16, "int16"},
    {fastlanes::OperatorToken::EXP_UNCOMPRESSED_I32, "int32"},
    {fastlanes::OperatorToken::EXP_UNCOMPRESSED_I64, "int64"},
    {fastlanes::OperatorToken::EXP_UNCOMPRESSED_DBL, "dbl"},
    {fastlanes::OperatorToken::EXP_UNCOMPRESSED_STR, "str"},

    // FOR encodings (unffor)
    {fastlanes::OperatorToken::EXP_FFOR_I08, "int8"},
    {fastlanes::OperatorToken::EXP_FFOR_I16, "int16"},
    {fastlanes::OperatorToken::EXP_FFOR_I32, "int32"},
    {fastlanes::OperatorToken::EXP_FFOR_I64, "int64"},

    // ALP encodings
    {fastlanes::OperatorToken::EXP_ALP_DBL, "dbl"},
    {fastlanes::OperatorToken::EXP_ALP_RD_DBL, "dbl"},

    // Constant encodings
    {fastlanes::OperatorToken::EXP_CONSTANT_I08, "int8"},
    {fastlanes::OperatorToken::EXP_CONSTANT_I16, "int16"},
    {fastlanes::OperatorToken::EXP_CONSTANT_I32, "int32"},
    {fastlanes::OperatorToken::EXP_CONSTANT_I64, "int64"},
    {fastlanes::OperatorToken::EXP_CONSTANT_DBL, "dbl"},
    {fastlanes::OperatorToken::EXP_CONSTANT_STR, "str"},

    // FSST encodings
    {fastlanes::OperatorToken::EXP_FSST_DELTA, "str"},
    {fastlanes::OperatorToken::EXP_FSST12_DELTA, "str"},

    // Null encodings
    {fastlanes::OperatorToken::EXP_NULL_I16, "int16"},
    {fastlanes::OperatorToken::EXP_NULL_I32, "int32"},
    {fastlanes::OperatorToken::EXP_NULL_DBL, "dbl"},

    // Frequency encodings
    {fastlanes::OperatorToken::EXP_FREQUENCY_I08, "int8"},
    {fastlanes::OperatorToken::EXP_FREQUENCY_I16, "int16"},
    {fastlanes::OperatorToken::EXP_FREQUENCY_I32, "int32"},
    {fastlanes::OperatorToken::EXP_FREQUENCY_I64, "int64"},
    {fastlanes::OperatorToken::EXP_FREQUENCY_DBL, "dbl"},
    {fastlanes::OperatorToken::EXP_FREQUENCY_STR, "str"},

    // Cross RLE encodings
    {fastlanes::OperatorToken::EXP_CROSS_RLE_I08, "int8"},
    {fastlanes::OperatorToken::EXP_CROSS_RLE_I16, "int16"},
    {fastlanes::OperatorToken::EXP_CROSS_RLE_I32, "int32"},
    {fastlanes::OperatorToken::EXP_CROSS_RLE_I64, "int64"},
    {fastlanes::OperatorToken::EXP_CROSS_RLE_DBL, "dbl"},
    {fastlanes::OperatorToken::EXP_CROSS_RLE_STR, "str"},

	{fastlanes::OperatorToken::INVALID, ""},
};

void create_table(duckdb::Connection &conn) {
	conn.Query("CREATE TABLE fls_table AS SELECT * FROM read_fls(\"/Users/sebastiaan/Downloads/equal_doubles\")");
}

void generate_fls_data(const Benchmark &benchmark, fastlanes::OperatorToken schema) {
	const std::string &variant = Benchmark::token_to_variant.at(schema);
	const std::filesystem::path source_dir = benchmark.base_dir / "source" / (benchmark.name + "_" + variant);
	const std::filesystem::path dest_dir = benchmark.base_dir / "fls" / (benchmark.name + "_" + variant);

	if (!exists(dest_dir)) {
		create_directories(dest_dir);
	} else {
		return;
	}

	fastlanes::Connection conn;

	if (schema == fastlanes::OperatorToken::INVALID) {
		conn.reset().read(source_dir);
	} else {
		conn.set_sample_size(1);
		conn.reset().read(source_dir).force_schema({schema});
	}

	conn.to_fls(dest_dir);
}

void generate_parquet_data(duckdb::Connection &conn, const Benchmark &benchmark, fastlanes::OperatorToken schema) {
	const std::string &variant = Benchmark::token_to_variant.at(schema);
	const std::filesystem::path source_dir = benchmark.base_dir / "source" / (benchmark.name + "_" + variant);
	const std::filesystem::path dest_dir = benchmark.base_dir / "parquet" / (benchmark.name + "_" + variant);

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

std::chrono::duration<double> profile_query(duckdb::Connection &conn, const std::string &query) {
	const auto start = std::chrono::high_resolution_clock::now();
	const auto result = conn.Query(query);
	const auto end = std::chrono::high_resolution_clock::now();

	if (result->HasError()) {
		std::cerr << "Query error: " << result->GetError() << '\n';
		exit(1);
	}

	std::chrono::duration<double> elapsed = end - start;

	return elapsed;
}

int main() {
	duckdb::DuckDB db(nullptr); // In-memory instance
	duckdb::Connection conn(db);

	const std::vector<Benchmark> benchmarks = {
	    Benchmark {"dec_uncompressed_opr",
	               {fastlanes::OperatorToken::EXP_UNCOMPRESSED_I08, fastlanes::OperatorToken::EXP_UNCOMPRESSED_I16,
	                fastlanes::OperatorToken::EXP_UNCOMPRESSED_I32, fastlanes::OperatorToken::EXP_UNCOMPRESSED_I64,
	                fastlanes::OperatorToken::EXP_UNCOMPRESSED_DBL, fastlanes::OperatorToken::EXP_UNCOMPRESSED_STR}},
	    Benchmark {"dec_unffor_opr",
	               {fastlanes::OperatorToken::EXP_FFOR_I08, fastlanes::OperatorToken::EXP_FFOR_I16,
	                fastlanes::OperatorToken::EXP_FFOR_I32, fastlanes::OperatorToken::EXP_FFOR_I64}},
	    Benchmark {"dec_alp_opr", {fastlanes::OperatorToken::EXP_ALP_DBL}},
	    Benchmark {"dec_alp_rd_opr", {fastlanes::OperatorToken::EXP_ALP_RD_DBL}},
	    Benchmark {"dec_constant_opr",
	               {fastlanes::OperatorToken::EXP_CONSTANT_I08, fastlanes::OperatorToken::EXP_CONSTANT_I16,
	                fastlanes::OperatorToken::EXP_CONSTANT_I32, fastlanes::OperatorToken::EXP_CONSTANT_I64,
	                fastlanes::OperatorToken::EXP_CONSTANT_DBL, fastlanes::OperatorToken::EXP_CONSTANT_STR}},
		// TODO: Does not seem to work with enforced schema.
	    Benchmark {"dec_fsst_opr", {fastlanes::OperatorToken::INVALID}},
	    // Benchmark {"dec_fsst12_opr", {fastlanes::OperatorToken::EXP_FSST12_DELTA}},
	    // Benchmark {"dec_null_opr",
	    //            {fastlanes::OperatorToken::EXP_NULL_I16, fastlanes::OperatorToken::EXP_NULL_I32,
	    //             fastlanes::OperatorToken::EXP_NULL_DBL}},
	    Benchmark {"dec_frequency_opr",
	               {fastlanes::OperatorToken::EXP_FREQUENCY_I08, fastlanes::OperatorToken::EXP_FREQUENCY_I16,
	                fastlanes::OperatorToken::EXP_FREQUENCY_I32, fastlanes::OperatorToken::EXP_FREQUENCY_I64,
	                fastlanes::OperatorToken::EXP_FREQUENCY_DBL, fastlanes::OperatorToken::EXP_FREQUENCY_STR}},
	    Benchmark {"dec_cross_rle_opr",
	               {fastlanes::OperatorToken::EXP_CROSS_RLE_I08, fastlanes::OperatorToken::EXP_CROSS_RLE_I16,
	                fastlanes::OperatorToken::EXP_CROSS_RLE_I32, fastlanes::OperatorToken::EXP_CROSS_RLE_I64,
	                fastlanes::OperatorToken::EXP_CROSS_RLE_DBL, fastlanes::OperatorToken::EXP_CROSS_RLE_STR}}};

	for (const auto &benchmark : benchmarks) {
		std::cout << benchmark.name << '\n';

		for (const auto &variant : benchmark.variants) {
			std::cout << Benchmark::token_to_variant.at(variant) << '\n';

			generate_fls_data(benchmark, variant);
			generate_parquet_data(conn, benchmark, variant);
		}
	}

	return 0;
}

// std::vector<double> fls_create;
// std::vector<double> parquet_create;
//
// std::vector<double> fls_agg;
// std::vector<double> parquet_agg;
//
// for (int i = 0; i < 10; i++) {
// 	duckdb::DuckDB bench_db(nullptr);
// 	duckdb::Connection bench_conn(bench_db);
//
// 	bench_conn.Query("SET threads=1;");
// 	// bench_conn.Query("PRAGMA force_compression='alp';");
//
// 	std::chrono::duration<double> elapsed_fls =
// 	    profile_query(bench_conn, "CREATE VIEW fls_table AS SELECT * FROM read_fls('" +
// 	                                  (base_dir / "fls" / benchmark).string() + "');");
// 	std::chrono::duration<double> elapsed_parquet =
// 	    profile_query(bench_conn, "CREATE VIEW parquet_table AS SELECT * FROM read_parquet('" +
// 	                                  (base_dir / "parquet" / benchmark / "data.parquet").string() + "');");
//
// 	fls_create.push_back(elapsed_fls.count());
// 	parquet_create.push_back(elapsed_parquet.count());
//
// 	std::chrono::duration<double> elapsed_agg_fls = profile_query(
// 	    bench_conn,
// 	    "SELECT AVG(COLUMN_0), AVG(COLUMN_1), AVG(COLUMN_2), AVG(COLUMN_3), AVG(COLUMN_4), "
// 	    "AVG(COLUMN_5), AVG(COLUMN_6), AVG(COLUMN_7), AVG(COLUMN_8), AVG(COLUMN_9) FROM fls_table;");
// 	std::chrono::duration<double> elapsed_agg_parquet = profile_query(
// 	    bench_conn,
// 	    "SELECT AVG(COLUMN_0), AVG(COLUMN_1), AVG(COLUMN_2), AVG(COLUMN_3), AVG(COLUMN_4), "
// 	    "AVG(COLUMN_5), AVG(COLUMN_6), AVG(COLUMN_7), AVG(COLUMN_8), AVG(COLUMN_9) FROM fls_table;");
//
// 	fls_agg.push_back(elapsed_agg_fls.count());
// 	parquet_agg.push_back(elapsed_agg_parquet.count());

// std::cout << "FLS Create Timings: [";
// for (const auto &f : fls_create) {
// 	std::cout << " " << f << " ";
// }
// std::cout << "]\n";
//
// std::cout << "Parquet Create Timings: [";
// for (const auto &pq : parquet_create) {
// 	std::cout << " " << pq << " ";
// }
// std::cout << "]\n";
//
// std::cout << "FLS Create Average:"
//           << std::accumulate(fls_create.begin(), fls_create.end(), 0.0) / fls_create.size() << '\n';
// std::cout << "Parquet Create Average:"
//           << std::accumulate(parquet_create.begin(), parquet_create.end(), 0.0) / parquet_create.size() << '\n';
//
// std::cout << "FLS Agg Timings: [";
// for (const auto &f : fls_agg) {
// 	std::cout << " " << f << " ";
// }
// std::cout << "]\n";
//
// std::cout << "Parquet Agg Timings: [";
// for (const auto &pq : parquet_agg) {
// 	std::cout << " " << pq << " ";
// }
// std::cout << "]\n";
//
// std::cout << "FLS Agg Average:" << std::accumulate(fls_agg.begin(), fls_agg.end(), 0.0) / fls_agg.size()
//           << '\n';
// std::cout << "Parquet Agg Average:"
//           << std::accumulate(parquet_agg.begin(), parquet_agg.end(), 0.0) / parquet_agg.size() << '\n';

// create_table(conn);
//
// auto result = conn.Query("SELECT * FROM fls_table");
// if (result->HasError()) {
// 	std::cerr << "Query Error: " << result->GetError() << std::endl;
// 	return 1;
// }
//
// for (size_t row = 0; row < result->RowCount(); row++) {
// 	std::cout << "Row " << row << ": " << result->GetValue(0, row).ToString()
// 			  << ", " << result->GetValue(1, row).ToString() << std::endl;
// }
