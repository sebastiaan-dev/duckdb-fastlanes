#pragma once

#include "reader/fls_decode.hpp"
#include "reader/materializer/context.hpp"
#include <duckdb/common/types/string_type.hpp>
#include <duckdb/common/types/vector.hpp>
#include <fls/expression/fsst_dict_operator.hpp>

namespace duckdb::materializer {

template <typename T>
T load_unaligned(const void* ptr) {
	T value;
	std::memcpy(&value, ptr, sizeof(T));
	return value;
}

template <typename INDEX_PT>
struct KernelTraits<fastlanes::dec_fsst_dict_opr<INDEX_PT>> {
	static void Prepare(ColumnCtxHandle&                         ctx,
	                    LogicalType&                             type,
	                    fastlanes::dec_fsst_dict_opr<INDEX_PT>&  op,
	                    const std::vector<FastLanesScanFilter*>* scan_filters) {
		auto& c = ctx.Emplace<FSSTDictColumnCtx>();

		c.dict_size = op.fsst_offset_segment_view.data_span.size() / sizeof(fastlanes::ofs_t);
		c.dict_vec  = make_uniq<Vector>(type, c.dict_size);
		c.sel_vec   = make_uniq<SelectionVector>(2048);

		auto& str_buffer = StringVector::GetStringBuffer(*c.dict_vec);

		const auto  child_ptr    = GetDataPtr<Pass::First, string_t>(*c.dict_vec);
		const auto  offset_bytes = reinterpret_cast<const uint8_t*>(op.fsst_offset_segment_view.data_span.data());
		const auto* in_byte_arr  = reinterpret_cast<uint8_t*>(op.fsst_bytes_segment_view.data);

		fastlanes::ofs_t prev_end = 0;
		for (fastlanes::n_t i {0}; i < c.dict_size; ++i) {
			const fastlanes::ofs_t end = load_unaligned<fastlanes::ofs_t>(offset_bytes + i * sizeof(fastlanes::ofs_t));
			const fastlanes::ofs_t enc_len = end - prev_end;

			const idx_t      max_out = static_cast<idx_t>(enc_len) * 8;
			const data_ptr_t buf     = str_buffer.AllocateShrinkableBuffer(max_out);

			const auto decoded_size = static_cast<fastlanes::ofs_t>(
			    fsst_decode(&op.fsst_decoder, enc_len, in_byte_arr + prev_end, max_out, buf));

			child_ptr[i] = str_buffer.FinalizeShrinkableBuffer(buf, max_out, decoded_size);

			prev_end = end;
		}
	}

	template <Pass PASS>
	static void
	Decode(ColumnCtxHandle& ctx, Vector& col, idx_t, fastlanes::dec_fsst_dict_opr<INDEX_PT>& op, fastlanes::DataType&) {
		const auto& c     = ctx.Expect<FSSTDictColumnCtx>();
		auto        index = op.Index();
		auto&       sel   = *c.sel_vec;

		// TODO: To make this fast we need to know if there will be 2 passes or only one.
		// Do we want to add a third function call to the Kernel API that does a potential finishing pass per vector?
		// (1) no second pass:
		//		Immediately construct the dictionary vector.
		// (2) second pass:
		//		Only construct the first part of the selection vector, then on the second pass construct the dictionary
		//		vector.
		if constexpr (PASS == Pass::First) {
			for (idx_t i = 0; i < 1024; ++i) {
				sel.set_index(i, static_cast<sel_t>(index[i]));
			}
			col.Dictionary(*c.dict_vec, c.dict_size, sel, 1024);
			DictionaryVector::SetDictionaryId(col, to_string(CastPointerToValue(c.dict_vec->GetBuffer().get())));
		} else {
			for (idx_t i = 0; i < 1024; ++i) {
				sel.set_index(i + 1024, static_cast<sel_t>(index[i]));
			}
			col.Dictionary(*c.dict_vec, c.dict_size, sel, 2048);
		}
	}
};

} // namespace duckdb::materializer
