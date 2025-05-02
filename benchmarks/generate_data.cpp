#include <vector>
#include "include/benchmark.hpp"

#include "duckdb.hpp"
#include <iostream>
#include <regex>

static bool verify_fls_data(const std::filesystem::path &dest_dir) {
	std::cout << "Verifying: ";

	fastlanes::Connection conn;
	// Verify we can load the generated file
	fastlanes::TableReader &table_reader = conn.reset().read_fls(dest_dir);
	for (idx_t group = 0; group < table_reader.get_n_rowgroups(); group++) {
		auto row_reader = table_reader.get_rowgroup_reader(group);
		auto row_descriptor = row_reader->get_descriptor();

		for (idx_t vector = 0; vector < row_descriptor.GetNVectors(); vector++) {
			if (row_reader->get_chunk(vector).size() != row_descriptor.GetColumnDescriptors().size()) {
				return false;
			}
		}

		row_reader->materialize();
	}

	return true;
}

static void generate_fls_data(const std::filesystem::path &base_dir, const std::filesystem::path &file_dir,
                              const std::vector<fastlanes::OperatorToken> &schema) {
	fastlanes::Connection conn;
	std::filesystem::path source_dir = base_dir / "source" / file_dir;
	std::filesystem::path dest_dir = base_dir / "fls" / file_dir;

	if (exists(dest_dir)) {
		std::cout << "Directory already exists, skipping generation:" << file_dir << "\n";
		return;
	}
	std::cout << "Generating FLS file: " << file_dir << "\n";

	try {
		if (schema.empty()) {
			conn.reset().read_csv(source_dir);
		} else {
			conn.reset().read_csv(source_dir).force_schema(schema);
		}
	} catch (const std::exception &e) {
		std::cerr << "Failed to read from: " << source_dir << "\n";
		std::cerr << "Error: " << e.what() << "\n";
		return;
	}

	create_directories(dest_dir);
	conn.to_fls(dest_dir);

	std::cout << "\u2714 Written FastLanes file" << "\n";

	try {
		if (verify_fls_data(dest_dir)) {
			std::cout << "\u2714" << "\n";
		}
	} catch (const std::exception &e) {
		std::cerr << "\u2718" << "\n";
	}
}

static void generate_parquet_data(duckdb::Connection &conn, const std::filesystem::path &base_dir,
                                  const std::filesystem::path &file_dir) {
	const std::filesystem::path source_dir = base_dir / "source" / file_dir;
	const std::filesystem::path dest_dir = base_dir / "parquet" / file_dir;

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

void generate_data(const std::filesystem::path &base_dir) {
	duckdb::DuckDB db(nullptr);
	duckdb::Connection conn(db);

	// Directory containing source files
	std::filesystem::path source_dir = base_dir / "source";

	// Pattern: dec_*_opr_<type>_<n_cols>_<n_rows>_<n_occurrence>
	std::regex pattern(R"(dec_.*_opr_([^_]+)_(\d+)_(\d+)_(\d+))");

	for (auto &entry : std::filesystem::directory_iterator(source_dir)) {
		if (!entry.is_directory())
			continue;
		std::string dirname = entry.path().filename().string();
		std::smatch match;
		if (!std::regex_match(dirname, match, pattern)) {
			std::cout << "Skipping non‐matching directory: " << dirname << "\n";
			continue;
		}

		int cols = std::stoi(match[2].str());
		std::vector schema(cols, fastlanes::OperatorToken::EXP_FSST_DELTA);

		generate_fls_data(base_dir, entry.path().filename(), {});
		generate_parquet_data(conn, base_dir, entry.path().filename());
	}

	std::cout << "---- Finished generation ----" << '\n';
}
