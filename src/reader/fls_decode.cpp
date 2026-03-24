#include "fsst.h"

namespace duckdb {
// We require this to be in a separate file to prevent conflicts with other definitions.
size_t fsst_decode(void* decoder, size_t length, const unsigned char* input, size_t size, unsigned char* output) {
	return duckdb_fsst_decompress(static_cast<duckdb_fsst_decoder_t*>(decoder), length, input, size, output);
}

} // namespace duckdb