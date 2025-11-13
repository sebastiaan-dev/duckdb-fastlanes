#pragma once

#include "fls_gen/untranspose/untranspose.hpp"
#include "reader/materializer/context.hpp"
#include <duckdb/common/types/string_type.hpp>
#include <duckdb/common/types/vector.hpp>
#include <fls/expression/fsst_expression.hpp>

namespace duckdb::materializer {

template <>
struct KernelTraits<fastlanes::dec_fsst_opr> {
	static void Prepare(ColumnCtxHandle&         ctx,
	                    LogicalType&             type,
	                    fastlanes::dec_fsst_opr& opr,
	                    const std::vector<FastLanesScanFilter*>*) {
		auto& c = ctx.Emplace<FSSTColumnCtx>();
		// 	const fsst_decoder_t& dec_ref = opr.fsst_decoder;
		// 	c.decoder =
		// 	    buffer_ptr<void>(new fsst_decoder_t(dec_ref), [](void* p) { delete static_cast<fsst_decoder_t*>(p); });
		// 	c.aux_vec = make_uniq<Vector>(type, 2048);
		// 	c.aux_vec->SetVectorType(VectorType::FSST_VECTOR);
		// 	FSSTVector::RegisterDecoder(*c.aux_vec, c.decoder, fastlanes::CFG::String::max_bytes_per_string);
	}

	// static string_t DecompressValue(void *duckdb_fsst_decoder, VectorStringBuffer &str_buffer,
	// 								const char *compressed_string, const idx_t compressed_string_len) {
	// 	const auto max_uncompressed_length = compressed_string_len * 8;
	// 	const auto fsst_decoder = static_cast<duckdb_fsst_decoder_t *>(duckdb_fsst_decoder);
	// 	const auto compressed_string_ptr = (const unsigned char *)compressed_string; // NOLINT
	// 	const auto target_ptr = str_buffer.AllocateShrinkableBuffer(max_uncompressed_length);
	// 	const auto decompressed_string_size = duckdb_fsst_decompress(
	// 		fsst_decoder, compressed_string_len, compressed_string_ptr, max_uncompressed_length, target_ptr);
	// 	return str_buffer.FinalizeShrinkableBuffer(target_ptr, max_uncompressed_length, decompressed_string_size);
	// }

	template <Pass PASS>
	static void
	Decode(ColumnCtxHandle& ctx, Vector& target_col, idx_t, fastlanes::dec_fsst_opr& opr, fastlanes::DataType&) {
		auto& str_buffer  = StringVector::GetStringBuffer(target_col);
		auto target_ptr = GetDataPtr<PASS, string_t>(target_col);

		generated::untranspose::fallback::scalar::untranspose_i(opr.offset_arr, opr.untrasposed_offset);

		const auto* __restrict offs = opr.untrasposed_offset; // length VEC_SZ
		fastlanes::len_t sizes[fastlanes::CFG::VEC_SZ];
		{
			fastlanes::ofs_t prev = 0;
			for (idx_t i = 0; i < fastlanes::CFG::VEC_SZ; ++i) {
				const auto cur = offs[i];
				sizes[i]       = static_cast<fastlanes::len_t>(cur - prev);
				prev           = cur;
			}
		}

		const auto base = reinterpret_cast<uint8_t*>(opr.fsst_bytes_segment_view.data);
		uint8_t* __restrict in_ptrs[fastlanes::CFG::VEC_SZ];
		{
			fastlanes::ofs_t acc = 0;
			for (idx_t i = 0; i < fastlanes::CFG::VEC_SZ; ++i) {
				in_ptrs[i] = base + acc;
				acc += sizes[i];
			}
		}

		for (idx_t i = 0; i < fastlanes::CFG::VEC_SZ; ++i) {
			const fastlanes::len_t enc = sizes[i];
			const idx_t max_out = static_cast<idx_t>(enc) * 8;
			const data_ptr_t buf = str_buffer.AllocateShrinkableBuffer(max_out);

			const auto decoded =
			    static_cast<fastlanes::ofs_t>(fsst_decompress(&opr.fsst_decoder, enc, in_ptrs[i], max_out, buf));

			target_ptr[i] = str_buffer.FinalizeShrinkableBuffer(buf, max_out, decoded);
		}

		// for (auto i {0}; i < fastlanes::CFG::VEC_SZ; ++i) {
		//
		// 	fastlanes::len_t encoded_size {0};
		// 	fastlanes::ofs_t offset {0};
		//
		// 	if (i == 0) {
		// 		encoded_size = opr.untrasposed_offset[0];
		// 	} else {
		// 		offset                 = opr.untrasposed_offset[i - 1];
		// 		const auto offset_next = opr.untrasposed_offset[i];
		// 		encoded_size           = offset_next - offset;
		// 	}
		//
		// 	const auto max_uncompressed_length = encoded_size * 8;
		// 	const auto buf_ptr                 = str_buffer.AllocateShrinkableBuffer(max_uncompressed_length);
		//
		// 	const auto decoded_size = static_cast<fastlanes::ofs_t>(
		// 	    fsst_decompress(&opr.fsst_decoder, encoded_size, in_byte_arr, max_uncompressed_length, buf_ptr));
		//
		// 	target_ptr[i] = str_buffer.FinalizeShrinkableBuffer(buf_ptr, max_uncompressed_length, decoded_size);
		// 	in_byte_arr += encoded_size;
		// }

		// 		auto& c = ctx.Expect<FSSTColumnCtx>();
		// 		if constexpr (PASS == Pass::First) {
		// 			target_col.SetVectorType(VectorType::FSST_VECTOR);
		// 			target_col.Reinterpret(*c.aux_vec);
		// 			FSSTVector::SetCount(target_col, 1024);
		// 		} else {
		// 			FSSTVector::SetCount(target_col, 2048);
		// 		}
		//
		// 		auto  encoded     = opr.GetEncodedBytes();
		// 		auto& fsst_buffer = target_col.GetAuxiliary()->template Cast<VectorFSSTStringBuffer>();
		// 		if (encoded.owner) {
		// 			fsst_buffer.AddHeapReference(make_buffer<KeepAlive>(std::move(encoded.owner)));
		// 		}
		//
		// #if defined(FLS_NO_TRANSPOSE) && FLS_NO_TRANSPOSE
		// 		// fastlanes::copy(opr.offset_arr, opr.untrasposed_offset);
		// 		generated::untranspose::fallback::scalar::untranspose_i(opr.offset_arr, opr.untrasposed_offset);
		// #else
		// 		generated::untranspose::fallback::scalar::untranspose_i(opr.offset_arr, opr.untrasposed_offset);
		// #endif
		// 		auto*            bytes    = reinterpret_cast<const char*>(encoded.span.data());
		// 		auto*            out      = GetCompressedStringPtr<PASS>(target_col);
		// 		fastlanes::ofs_t prev_end = 0;
		// 		for (idx_t i = 0; i < 1024; ++i) {
		// 			const fastlanes::ofs_t end = opr.untrasposed_offset[i];
		// 			out[i]                     = string_t(bytes + prev_end, static_cast<uint32_t>(end - prev_end));
		// 			prev_end                   = end;
		// 		}
	}
};

// auto* in_byte_arr = reinterpret_cast<uint8_t*>(opr.fsst_bytes_segment_view.data);
// generated::untranspose::fallback::scalar::untranspose_i(opr.offset_arr, opr.untrasposed_offset);
//
// auto target_ptr = GetDataPtr<PASS, string_t>(target_col);
// for (auto i {0}; i < fastlanes::CFG::VEC_SZ; ++i) {
//
// 	fastlanes::len_t encoded_size {0};
// 	fastlanes::ofs_t offset {0};
//
// 	if (i == 0) {
// 		encoded_size = opr.untrasposed_offset[0];
// 	} else {
// 		offset                 = opr.untrasposed_offset[i - 1];
// 		const auto offset_next = opr.untrasposed_offset[i];
// 		encoded_size           = offset_next - offset;
// 	}
//
// 	const auto decoded_size =
// 	    static_cast<fastlanes::ofs_t>(fsst_decompress(&opr.fsst_decoder,
// 	                                                  encoded_size,
// 	                                                  in_byte_arr,
// 	                                                  fastlanes::CFG::String::max_bytes_per_string,
// 	                                                  opr.tmp_string.data()));
//
// 	target_ptr[i] =
// 	    StringVector::AddString(target_col, reinterpret_cast<const char*>(opr.tmp_string.data()), decoded_size);
//
// 	in_byte_arr += encoded_size;
// }

} // namespace duckdb::materializer
