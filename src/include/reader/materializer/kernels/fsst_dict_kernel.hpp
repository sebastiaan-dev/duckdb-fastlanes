#pragma once

#include "reader/fls_decode.hpp"
#include "reader/materializer/context.hpp"
#include <duckdb/common/types/string_type.hpp>
#include <duckdb/common/types/vector.hpp>
#include <duckdb/planner/filter/in_filter.hpp>
#include <fls/expression/fsst_dict_operator.hpp>

namespace duckdb::materializer {

template <typename T>
T load_unaligned(const void* ptr) {
	T value;
	std::memcpy(&value, ptr, sizeof(T));
	return value;
}

// inline void BuildAllowedIds(FastLanesScanFilter* scan_filter, const string_t* dict_ptr, idx_t dict_size) {
// 	// Basic guards
// 	if (!scan_filter || !dict_ptr || dict_size <= 0) {
// 		if (scan_filter)
// 			scan_filter->allowed_dict_mask.reset();
// 		return;
// 	}
//
// 	// 1) Collect needles ONCE and bucket by length (to prune memcmp)
// 	//    We keep ownership in `owned_needles` and reference via string_view in buckets.
// 	std::vector<std::string>                                  owned_needles;
// 	std::unordered_map<size_t, std::vector<std::string_view>> needles_by_len;
// 	auto                                                      add_needle = [&](std::string s) {
//         const size_t nlen = s.size();
//         owned_needles.emplace_back(std::move(s));
//         needles_by_len[nlen].emplace_back(owned_needles.back());
// 	};
//
// 	switch (scan_filter->filter.filter_type) {
// 	case TableFilterType::IN_FILTER: {
// 		const auto& in = scan_filter->filter.Cast<InFilter>();
// 		if (in.values.empty()) { // IN () -> always false
// 			auto m = std::make_shared<DictMask>();
// 			m->assign_zero(static_cast<size_t>(dict_size));
// 			scan_filter->allowed_dict_mask = std::move(m);
// 			return;
// 		}
// 		owned_needles.reserve(in.values.size());
// 		needles_by_len.reserve(in.values.size());
// 		for (const auto& v : in.values)
// 			add_needle(StringValue::Get(v));
// 		break;
// 	}
// 	case TableFilterType::CONSTANT_COMPARISON: {
// 		const auto& cf = scan_filter->filter.Cast<ConstantFilter>();
// 		if (cf.comparison_type != ExpressionType::COMPARE_EQUAL) {
// 			scan_filter->allowed_dict_mask.reset();
// 			return; // unsupported for direct path
// 		}
// 		add_needle(StringValue::Get(cf.constant));
// 		break;
// 	}
// 	default:
// 		scan_filter->allowed_dict_mask.reset();
// 		return; // unsupported
// 	}
//
// 	if (needles_by_len.empty()) { // all needles were filtered out somehow
// 		scan_filter->allowed_dict_mask.reset();
// 		return;
// 	}
//
// 	// 2) Allocate mask (all zeros initially)
// 	auto mask = std::make_shared<DictMask>();
// 	mask->assign_zero(static_cast<size_t>(dict_size));
//
// 	// 3) Single-needle fast path (very common for '=')
// 	if (needles_by_len.size() == 1) {
// 		const auto&  bucket = *needles_by_len.begin();
// 		const size_t nlen   = bucket.first;
// 		const auto&  list   = bucket.second; // vector<string_view>
// 		if (list.size() == 1) {
// 			const std::string_view needle = list[0];
// 			const char*            ndata  = needle.data();
//
// 			for (idx_t i = 0; i < dict_size; ++i) {
// 				const size_t len = dict_ptr[i].GetSize();
// 				if (len != nlen)
// 					continue;
// 				const char* data = dict_ptr[i].GetDataUnsafe();
// 				if (std::memcmp(data, ndata, nlen) == 0) {
// 					mask->set(i);
// 				}
// 			}
// 			scan_filter->allowed_dict_mask = std::move(mask);
// 			return;
// 		}
// 	}
//
// 	// 4) General case: check only candidates with matching length
// 	//    (buckets are typically small; linear memcmp wins over hashing)
// 	for (idx_t i = 0; i < dict_size; ++i) {
// 		const size_t len = dict_ptr[i].GetSize();
// 		auto         it  = needles_by_len.find(len);
// 		if (it == needles_by_len.end())
// 			continue;
//
// 		const char* data       = dict_ptr[i].GetDataUnsafe();
// 		const auto& candidates = it->second; // vector<string_view>
//
// 		// Typically few candidates per length; stop on first match
// 		for (const auto sv : candidates) {
// 			// lengths equal by construction
// 			if (std::memcmp(data, sv.data(), len) == 0) {
// 				mask->set(i);
// 				break;
// 			}
// 		}
// 	}
//
// 	scan_filter->allowed_dict_mask = std::move(mask);
// }

// inline void BuildAllowedIds(FastLanesScanFilter* scan_filter, const string_t* dict_ptr, idx_t dict_size) {
// 	if (!scan_filter || !dict_ptr || dict_size <= 0) {
// 		if (scan_filter)
// 			scan_filter->allowed_dict_mask.reset();
// 		return;
// 	}
//
// 	// 1) Collect needles ONCE and bucket by length (prunes memcmp)
// 	std::unordered_map<size_t, std::vector<std::string>> needles_by_len;
// 	auto                                                 add_needle = [&](std::string s) {
//         needles_by_len[s.size()].emplace_back(std::move(s));
// 	};
//
// 	switch (scan_filter->filter.filter_type) {
// 	case TableFilterType::IN_FILTER: {
// 		const auto& in = scan_filter->filter.Cast<InFilter>();
// 		if (in.values.empty()) { // IN () -> always false
// 			auto m = std::make_shared<DictMask>();
// 			m->assign_zero(dict_size);
// 			scan_filter->allowed_dict_mask = std::move(m);
// 			return;
// 		}
// 		needles_by_len.reserve(in.values.size());
// 		for (const auto& v : in.values)
// 			add_needle(StringValue::Get(v));
// 		break;
// 	}
// 	case TableFilterType::CONSTANT_COMPARISON: {
// 		const auto& cf = scan_filter->filter.Cast<ConstantFilter>();
// 		if (cf.comparison_type != ExpressionType::COMPARE_EQUAL) {
// 			scan_filter->allowed_dict_mask.reset();
// 			return; // unsupported for direct path
// 		}
// 		add_needle(StringValue::Get(cf.constant));
// 		break;
// 	}
// 	default:
// 		scan_filter->allowed_dict_mask.reset();
// 		return; // unsupported
// 	}
//
// 	if (needles_by_len.empty()) {
// 		scan_filter->allowed_dict_mask.reset();
// 		return;
// 	}
//
// 	// 2) Allocate bitmap
// 	auto mask = std::make_shared<DictMask>();
// 	mask->assign_zero(dict_size);
//
// 	// 3) Single-needle fast path (very common for '=')
// 	if (needles_by_len.size() == 1) {
// 		const auto&  bucket = *needles_by_len.begin();
// 		const size_t nlen   = bucket.first;
// 		const auto&  list   = bucket.second;
// 		if (list.size() == 1) {
// 			const std::string& needle = list[0];
// 			const char*        ndata  = needle.data();
//
// 			for (idx_t i = 0; i < dict_size; ++i) {
// 				const size_t len = dict_ptr[i].GetSize();
// 				if (len != nlen)
// 					continue;
// 				const char* data = dict_ptr[i].GetDataUnsafe();
// 				if (std::memcmp(data, ndata, nlen) == 0) {
// 					mask->set(i);
// 				}
// 			}
// 			scan_filter->allowed_dict_mask = std::move(mask);
// 			return;
// 		}
// 	}
//
// 	// 4) General case: by length, then memcmp candidates
// 	for (idx_t i = 0; i < dict_size; ++i) {
// 		const size_t len = dict_ptr[i].GetSize();
// 		auto         it  = needles_by_len.find(len);
// 		if (it == needles_by_len.end())
// 			continue;
//
// 		const char* data       = dict_ptr[i].GetDataUnsafe();
// 		const auto& candidates = it->second;
//
// 		// Small candidate set per length → fast loop
// 		for (const auto& s : candidates) {
// 			if (std::memcmp(data, s.data(), len) == 0) {
// 				mask->set(i);
// 				break;
// 			}
// 		}
// 	}
//
// 	scan_filter->allowed_dict_mask = std::move(mask);
// }

// inline void BuildAllowedIds(FastLanesScanFilter* scan_filter, const string_t* ptr, size_t dict_size) {
// 	scan_filter->allowed_dict_ids.clear();
//
// 	auto build_membership = [&](auto&& needles) {
// 		scan_filter->allowed_dict_ids.assign(static_cast<size_t>(dict_size), false);
//
// 		for (idx_t i = 0; i < dict_size; ++i) {
// 			const auto s = ptr[i].GetString();
// 			if (needles.find(s) != needles.end()) {
// 				scan_filter->allowed_dict_ids[i] = true;
// 			}
// 		}
// 	};
//
// 	switch (scan_filter->filter.filter_type) {
// 	case TableFilterType::IN_FILTER: {
// 		const auto&                     in = scan_filter->filter.Cast<InFilter>();
// 		std::unordered_set<std::string> needles;
// 		needles.reserve(in.values.size());
// 		for (const auto& v : in.values) {
// 			needles.insert(StringValue::Get(v));
// 		}
// 		build_membership(needles);
// 		break;
// 	}
// 	case TableFilterType::CONSTANT_COMPARISON: {
// 		const auto& cf = scan_filter->filter.Cast<ConstantFilter>();
// 		if (cf.comparison_type == ExpressionType::COMPARE_EQUAL) {
// 			std::unordered_set<std::string> needles;
// 			needles.insert(StringValue::Get(cf.constant));
// 			build_membership(needles);
// 		}
// 		break;
// 	}
// 	default:
// 		break;
// 	}
// }

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
			    test_fsst_decode(&op.fsst_decoder, enc_len, in_byte_arr + prev_end, max_out, buf));

			child_ptr[i] = str_buffer.FinalizeShrinkableBuffer(buf, max_out, decoded_size);

			prev_end = end;
		}

		// if (scan_filters && !scan_filters->empty()) {
		// 	for (auto* sf : *scan_filters) {
		//		First do some precondition checks before setting this, otherwise the ApplyFilter will use the direct
		//		filter on incompatible types.
		// 		if (auto* existing = sf->ctx.Maybe<FSSTDictFilterCtx, DirectFilterKind::DictMask>()) {
		// 			// BuildAllowedIds(f, child_ptr, c.dict_size);
		// 		} else {
		// 			auto& sf_ctx = sf->ctx.Emplace<FSSTDictFilterCtx>();
		// 		}
		// 	}
		// }
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
