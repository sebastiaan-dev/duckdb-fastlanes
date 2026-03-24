#pragma once

#include "fls/expression/cross_rle_operator.hpp"
#include "reader/materializer/fls_type_resolver.hpp"

namespace duckdb::materializer {

inline fastlanes::n_t
t_find_rle_segment(const fastlanes::len_t* rle_lengths, fastlanes::n_t size, fastlanes::n_t range_index) {
	fastlanes::n_t target_start = range_index * 1024;
	fastlanes::n_t current_pos  = 0;

	for (fastlanes::n_t i = 0; i < size; ++i) {
		if (current_pos + rle_lengths[i] > target_start) {
			return i;
		}
		current_pos += rle_lengths[i];
	}

	return size - 1;
}

template <typename PT>
void t_decode_rle_range(const fastlanes::len_t* rle_lengths,
                        const PT*               rle_values,
                        fastlanes::n_t          size,
                        fastlanes::n_t          range_index,
                        PT*                     decoded_arr) {
	fastlanes::n_t start_rle_index = t_find_rle_segment(rle_lengths, size, range_index);

	fastlanes::n_t needed        = 1024;
	fastlanes::n_t current_index = start_rle_index;
	fastlanes::n_t current_pos   = 0;

	for (fastlanes::n_t i = 0; i < start_rle_index; ++i)
		current_pos += rle_lengths[i];

	fastlanes::n_t offset      = range_index * 1024 - current_pos;
	fastlanes::n_t decoded_pos = 0; // Track the correct position in decoded_arr

	while (needed > 0 && current_index < size) {
		fastlanes::n_t available = rle_lengths[current_index] - offset;
		fastlanes::n_t to_copy   = std::min(available, needed);

		for (fastlanes::n_t i = 0; i < to_copy; ++i) {
			decoded_arr[decoded_pos++] = rle_values[current_index];
		}

		needed -= to_copy;
		offset = 0;
		++current_index;
	}
}

inline void t_decode_rle_range(const fastlanes::len_t* rle_lengths,
                               const uint8_t*          rle_value_bytes,
                               const fastlanes::ofs_t* rle_value_offsets,
                               fastlanes::n_t          size,
                               fastlanes::n_t          range_index,
                               Vector&                 target_col,
                               string_t*               target) {

	fastlanes::n_t start_rle_index = t_find_rle_segment(rle_lengths, size, range_index);

	fastlanes::n_t needed        = 1024;
	fastlanes::n_t current_index = start_rle_index;
	fastlanes::n_t current_pos   = 0;
	idx_t          entries       = 0;

	for (fastlanes::n_t i = 0; i < start_rle_index; ++i)
		current_pos += rle_lengths[i];

	fastlanes::n_t offset = range_index * 1024 - current_pos;

	while (needed > 0 && current_index < size) {
		fastlanes::ofs_t prev_offset = 0;
		if (current_index == 0) {
			prev_offset = 0;
		} else {
			prev_offset = rle_value_offsets[current_index - 1];
		}
		fastlanes::ofs_t cur_offset = rle_value_offsets[current_index];
		auto             length     = cur_offset - prev_offset;
		fastlanes::n_t   available  = rle_lengths[current_index] - offset;
		fastlanes::n_t   to_copy    = std::min(available, needed);

		for (fastlanes::n_t i = 0; i < to_copy; ++i) {
			target[entries] = StringVector::AddString(
			    target_col, reinterpret_cast<const char*>(rle_value_bytes + prev_offset), length);
			entries++;
		}

		needed -= to_copy;
		offset = 0;
		++current_index;
	}
}

template <typename PT>
struct KernelTraits<fastlanes::dec_cross_rle_opr<PT>> {
	static void Prepare(ColumnCtxHandle&,
	                    LogicalType&,
	                    fastlanes::dec_cross_rle_opr<PT>&,
	                    const std::vector<FastLanesScanFilter*>*) {
	}

	template <Pass PASS>
	static void
	Decode(ColumnCtxHandle&, Vector& col, idx_t vec_idx, fastlanes::dec_cross_rle_opr<PT>& op, fastlanes::DataType&) {
		const auto* length = reinterpret_cast<const fastlanes::len_t*>(op.lengths_segment.data);
		const auto* values = reinterpret_cast<const PT*>(op.values_segment.data);

		const auto size = op.lengths_segment.data_span.size() / sizeof(fastlanes::len_t);

		std::array<PT, fastlanes::CFG::VEC_SZ> decoded {};
		t_decode_rle_range(length, values, size, vec_idx, decoded.data());
		detail::NumericHelper<PASS>::template CopyVector<PT>(decoded.data(), col);
	}
};

template <>
struct KernelTraits<fastlanes::dec_cross_rle_opr<fastlanes::fls_string_t>> {
	static void Prepare(ColumnCtxHandle&,
	                    LogicalType&,
	                    fastlanes::dec_cross_rle_opr<fastlanes::fls_string_t>&,
	                    const std::vector<FastLanesScanFilter*>*) {
	}

	template <Pass PASS>
	static void Decode(ColumnCtxHandle&,
	                   Vector&                                                col,
	                   idx_t                                                  vec_idx,
	                   fastlanes::dec_cross_rle_opr<fastlanes::fls_string_t>& op,
	                   fastlanes::DataType&) {
		const auto  target       = GetDataPtr<PASS, string_t>(col);
		const auto* length       = reinterpret_cast<const fastlanes::len_t*>(op.lengths_segment.data);
		const auto* values_bytes = reinterpret_cast<const uint8_t*>(op.values_bytes_seg.data);
		const auto* offsets      = reinterpret_cast<const fastlanes::ofs_t*>(op.values_offset_seg.data);

		const auto size = op.lengths_segment.data_span.size() / sizeof(fastlanes::len_t);

		t_decode_rle_range(length, values_bytes, offsets, size, vec_idx, col, target);
	}
};

} // namespace duckdb::materializer
