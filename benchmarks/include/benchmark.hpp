#pragma once

#include <filesystem>
#include <fls/connection.hpp>

struct Benchmark {
	static const std::filesystem::path base_dir;
	static const std::unordered_map<fastlanes::OperatorToken, std::string> token_to_variant;

	std::string name;
	std::vector<fastlanes::OperatorToken> variants;
};