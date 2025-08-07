#include "duckdb/common/types/string_type.hpp"
// #include "duckdb/common/types/vector.hpp"
// #include "fls/expression/fsst_dict_operator.hpp"
// #include "fls_gen/untranspose/untranspose.hpp"
#include "fsst.h"

namespace duckdb {
//
// template <typename INDEX_PT>
// void fls_decode_dict_fsst(string_t *target_ptr, Vector &target_col,
//                           const fastlanes::sp<fastlanes::dec_fsst_dict_opr<INDEX_PT>> &opr) {
// 	auto *in_byte_arr = reinterpret_cast<uint8_t *>(opr->fsst_bytes_segment_view.data);
//
// 	FLS_ASSERT_NOT_NULL_POINTER(in_byte_arr)
//
// 	for (fastlanes::n_t idx {0}; idx < fastlanes::CFG::VEC_SZ; ++idx) {
// 		const auto index = opr->Index()[idx];
//
// 		fastlanes::ofs_t offset = 0;
// 		fastlanes::len_t length = 0;
//
// 		const auto offset_bytes = reinterpret_cast<const uint8_t *>(opr->fsst_offset_segment_view.data_span.data());
//
// 		if (index == 0) {
// 			offset = 0;
// 			std::memcpy(&length, offset_bytes + index * sizeof(fastlanes::ofs_t), sizeof(fastlanes::ofs_t));
// 			// length = offsets[index];
// 		} else {
// 			std::memcpy(&offset, offset_bytes + (index - 1) * sizeof(fastlanes::ofs_t), sizeof(fastlanes::ofs_t));
// 			// offset = offsets[index - 1];
// 			// const auto offset_next = offsets[index];
// 			fastlanes::ofs_t offset_next = 0;
// 			std::memcpy(&offset_next, offset_bytes + index * sizeof(fastlanes::ofs_t), sizeof(fastlanes::ofs_t));
//
// 			length = offset_next - offset;
// 		}
// 		FLS_ASSERT_LE(length, fastlanes::CFG::String::max_bytes_per_string)
//
// 		alignas(8) std::array<unsigned char, fastlanes::CFG::String::max_bytes_per_string> tmp;
// 		const auto decoded_size = static_cast<fastlanes::ofs_t>(
// 		    duckdb_fsst_decompress(&opr->fsst_decoder, length, in_byte_arr + offset,
// 		                           fastlanes::CFG::String::max_bytes_per_string, tmp.data()));
//
// 		// FLS_ASSERT_L(decoded_size, opr->tmp_string.capacity())
//
// 		target_ptr[idx] = StringVector::AddString(target_col, reinterpret_cast<const char *>(tmp.data()), decoded_size);
// 	}
// }

size_t test_fsst_decode(void *decoder, size_t length, const unsigned char *input, size_t size, unsigned char *output) {
	return duckdb_fsst_decompress(static_cast<duckdb_fsst_decoder_t *>(decoder), length, input, size, output);
}

} // namespace duckdb