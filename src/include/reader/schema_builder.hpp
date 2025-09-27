#pragma once

#include "duckdb/common/types.hpp"
#include <fls/footer/datatype_generated.h>
#include <fls/footer/table_descriptor_generated.h>
#include <string>
#include <vector>

namespace duckdb {

struct SchemaBuildResult {
	std::vector<std::string>         column_names;
	std::vector<fastlanes::DataType> promoted_types;
};

class SchemaBuilder {
public:
	SchemaBuilder(const fastlanes::TableDescriptorT& table_descriptor_p, const std::string& file_path_p);

	SchemaBuildResult Build() const;

private:
	const fastlanes::TableDescriptorT& table_descriptor;
	const std::string&                 file_path;
};

} // namespace duckdb
