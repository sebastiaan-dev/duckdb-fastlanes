#include "fastlanes.hpp"
#include <iostream>
#include <string>
#include <filesystem>

int main(int argc, char* argv[]) {
    if (argc < 3) {
        std::cerr << "Usage: " << argv[0] << " <input_parquet_file> <output_fls_file>" << std::endl;
        return 1;
    }

    std::string input_file = argv[1];
    std::string output_file = argv[2];
    
    // Create output directory if it doesn't exist
    std::filesystem::path output_path(output_file);
	auto parent = output_path.parent_path();
	if (!parent.empty()) {
		std::filesystem::create_directories(parent);
	}
    
    try {
        fastlanes::Connection conn;
        conn.read_csv(input_file).inline_footer().to_fls(output_file);
        std::cout << "Successfully converted " << input_file << " to " << output_file << std::endl;
        return 0;
    } catch (const std::exception& e) {
        std::cerr << "Error: " << e.what() << std::endl;
        return 1;
    }
}
