#pragma once

namespace duckdb {
size_t fsst_decode(void* decoder, size_t length, const unsigned char* input, size_t size, unsigned char* output);
} // namespace duckdb