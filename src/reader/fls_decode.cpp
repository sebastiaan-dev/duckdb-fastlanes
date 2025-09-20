#include "fsst.h"

namespace duckdb {
size_t test_fsst_decode(void *decoder, size_t length, const unsigned char *input, size_t size, unsigned char *output) {
	return duckdb_fsst_decompress(static_cast<duckdb_fsst_decoder_t *>(decoder), length, input, size, output);
}

} // namespace duckdb