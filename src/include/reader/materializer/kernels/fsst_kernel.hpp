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
		auto&                 c       = ctx.Emplace<FSSTColumnCtx>();
		const fsst_decoder_t& dec_ref = opr.fsst_decoder;
		c.decoder =
		    buffer_ptr<void>(new fsst_decoder_t(dec_ref), [](void* p) { delete static_cast<fsst_decoder_t*>(p); });
		c.aux_vec = make_uniq<Vector>(type, 2048);
		c.aux_vec->SetVectorType(VectorType::FSST_VECTOR);
		FSSTVector::RegisterDecoder(*c.aux_vec, c.decoder, fastlanes::CFG::String::max_bytes_per_string);
	}

	template <Pass PASS>
	static void
	Decode(ColumnCtxHandle& ctx, Vector& target_col, idx_t, fastlanes::dec_fsst_opr& opr, fastlanes::DataType&) {
		auto& c = ctx.Expect<FSSTColumnCtx>();
		if constexpr (PASS == Pass::First) {
			target_col.SetVectorType(VectorType::FSST_VECTOR);
			target_col.Reinterpret(*c.aux_vec);
			FSSTVector::SetCount(target_col, 1024);
		} else {
			FSSTVector::SetCount(target_col, 2048);
		}

		auto  encoded     = opr.GetEncodedBytes();
		auto& fsst_buffer = target_col.GetAuxiliary()->template Cast<VectorFSSTStringBuffer>();
		if (encoded.owner) {
			fsst_buffer.AddHeapReference(make_buffer<KeepAlive>(std::move(encoded.owner)));
		}

#if defined(FLS_NO_TRANSPOSE) && FLS_NO_TRANSPOSE
		// fastlanes::copy(opr.offset_arr, opr.untrasposed_offset);
		generated::untranspose::fallback::scalar::untranspose_i(opr.offset_arr, opr.untrasposed_offset);
#else
		generated::untranspose::fallback::scalar::untranspose_i(opr.offset_arr, opr.untrasposed_offset);
#endif
		auto*            bytes    = reinterpret_cast<const char*>(encoded.span.data());
		auto*            out      = GetCompressedStringPtr<PASS>(target_col);
		fastlanes::ofs_t prev_end = 0;
		for (idx_t i = 0; i < 1024; ++i) {
			const fastlanes::ofs_t end = opr.untrasposed_offset[i];
			out[i]                     = string_t(bytes + prev_end, static_cast<uint32_t>(end - prev_end));
			prev_end                   = end;
		}
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
