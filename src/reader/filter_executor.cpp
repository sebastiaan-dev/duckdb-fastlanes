#include "reader/filter_executor.hpp"
#include "duckdb/storage/table/column_segment.hpp"
#include "reader/fls_multi_file_info.hpp"
#include <duckdb/planner/table_filter_state.hpp>

namespace duckdb {

struct FLSFilterStats {
	idx_t chunks = 0;

	// Cumulative across this thread
	idx_t total_rows_scanned = 0; // rows entering Apply()
	idx_t total_rows_output  = 0; // rows after all filters

	// How many rows were removed by each mechanism
	idx_t removed_by_direct   = 0; // TryDirectFilter wins
	idx_t removed_by_fallback = 0; // ColumnSegment::FilterSelection

	// Fast path usage
	idx_t direct_calls = 0;
	idx_t direct_hits  = 0; // returned true

	// Optional: per-chunk last values (handy for logging)
	idx_t last_in_rows  = 0;
	idx_t last_out_rows = 0;
};

inline thread_local FLSFilterStats fls_filter_stats;

inline void FLSLogChunkStats(const FLSFilterStats& s) {
	const auto   total_in      = s.total_rows_scanned;
	const auto   total_out     = s.total_rows_output;
	const auto   total_removed = (total_in >= total_out) ? (total_in - total_out) : 0;
	const double removal_pct   = total_in ? (100.0 * total_removed / double(total_in)) : 0.0;

	std::fprintf(stderr,
	             "[FLS] chunks=%llu in=%llu out=%llu removed=%llu (%.2f%%) "
	             "direct{calls=%llu hits=%llu removed=%llu} fallback{removed=%llu}\n",
	             (unsigned long long)s.chunks,
	             (unsigned long long)total_in,
	             (unsigned long long)total_out,
	             (unsigned long long)total_removed,
	             removal_pct,
	             (unsigned long long)s.direct_calls,
	             (unsigned long long)s.direct_hits,
	             (unsigned long long)s.removed_by_direct,
	             (unsigned long long)s.removed_by_fallback);
}

FastLanesScanFilter::FastLanesScanFilter(ClientContext& context, idx_t filter_idx_p, TableFilter& filter_p)
    : filter_idx(filter_idx_p)
    , filter(filter_p)
    , filter_state(TableFilterState::Initialize(context, filter_p)) {
}

FastLanesScanFilter::~FastLanesScanFilter() = default;

// bool TryDirectFilter(const FastLanesScanFilter& sf, Vector& vec, SelectionVector& sel, idx_t& count) {
// 	if (vec.GetVectorType() != VectorType::DICTIONARY_VECTOR) {
// 		return false;
// 	}
//
// 	switch (sf.filter.filter_type) {
// 	case TableFilterType::IN_FILTER:
// 		break;
// 	case TableFilterType::CONSTANT_COMPARISON: {
// 		const auto& cf = sf.filter.Cast<ConstantFilter>();
// 		if (cf.comparison_type != ExpressionType::COMPARE_EQUAL)
// 			return false;
// 		break;
// 	}
// 	default:
// 		return false;
// 	}
//
// 	const auto mask = sf.allowed_dict_mask;
// 	if (!mask || mask->empty() || count == 0)
// 		return false;
//
// 	// Must return a pointer to a 0/1 byte per dictionary id.
// 	// Replace `as_byte_mask()` with your actual accessor.
// 	const uint8_t* mask8 = mask->as_byte_mask();
// 	if (!mask8)
// 		return false;
//
// 	auto&        dict_sv  = DictionaryVector::SelVector(vec);
// 	const sel_t* dict_idx = dict_sv.data();
// 	sel_t*       out_sel  = sel.data();
// 	const sel_t* in_sel   = sel.data();
//
// 	// Detect identity selection cheaply
// 	bool identity = true;
// 	for (idx_t k = 0, e = std::min<idx_t>(count, 4); k < e; ++k) {
// 		if (in_sel[k] != (sel_t)k) {
// 			identity = false;
// 			break;
// 		}
// 	}
//
// 	idx_t out = 0;
//
// 	if (identity) {
// 		// Branchless: write candidate, then advance 'out' by keep (0/1)
// 		for (idx_t i = 0; i < count; ++i) {
// 			const sel_t    dst  = (sel_t)i;
// 			const uint32_t keep = mask8[dict_idx[i]]; // 0 or 1
// 			out_sel[out]        = dst;                // harmless overwrite when keep==0
// 			out += keep;                              // advances only when kept
// 		}
// 	} else {
// 		for (idx_t i = 0; i < count; ++i) {
// 			const sel_t    r    = in_sel[i];
// 			const uint32_t keep = mask8[dict_idx[r]]; // 0 or 1
// 			out_sel[out]        = r;                  // harmless overwrite when keep==0
// 			out += keep;                              // advances only when kept
// 		}
// 	}
//
// 	count = out;
// 	return true;
// }

// bool TryDirectFilter(const FastLanesScanFilter& sf, Vector& vec, SelectionVector& sel, idx_t& count) {
// 	if (vec.GetVectorType() != VectorType::DICTIONARY_VECTOR) {
// 		return false;
// 	}
//
// 	switch (sf.filter.filter_type) {
// 	case TableFilterType::IN_FILTER:
// 		break;
// 	case TableFilterType::CONSTANT_COMPARISON: {
// 		const auto& cf = sf.filter.Cast<ConstantFilter>();
// 		if (cf.comparison_type != ExpressionType::COMPARE_EQUAL)
// 			return false;
// 		break;
// 	}
// 	default:
// 		return false;
// 	}
//
// 	const auto mask = sf.allowed_dict_mask;
// 	if (!mask || mask->empty() || count == 0)
// 		return false;
//
// 	auto&        dict_sv  = DictionaryVector::SelVector(vec);
// 	const sel_t* dict_idx = dict_sv.data();
// 	sel_t*       out_sel  = sel.data();
// 	const sel_t* in_sel   = sel.data();
//
// 	bool identity = true;
// 	for (idx_t k = 0, e = std::min<idx_t>(count, 4); k < e; ++k) {
// 		if (in_sel[k] != (sel_t)k) {
// 			identity = false;
// 			break;
// 		}
// 	}
//
// 	idx_t out = 0;
// 	if (identity) {
// 		idx_t i = 0;
// 		for (; i + 4 <= count; i += 4) {
// 			const idx_t id0 = dict_idx[i + 0];
// 			const idx_t id1 = dict_idx[i + 1];
// 			const idx_t id2 = dict_idx[i + 2];
// 			const idx_t id3 = dict_idx[i + 3];
// 			if (mask->test(id0))
// 				out_sel[out++] = (sel_t)(i + 0);
// 			if (mask->test(id1))
// 				out_sel[out++] = (sel_t)(i + 1);
// 			if (mask->test(id2))
// 				out_sel[out++] = (sel_t)(i + 2);
// 			if (mask->test(id3))
// 				out_sel[out++] = (sel_t)(i + 3);
// 		}
// 		for (; i < count; ++i) {
// 			const idx_t id = dict_idx[i];
// 			if (mask->test(id))
// 				out_sel[out++] = (sel_t)i;
// 		}
// 	} else {
// 		idx_t i = 0;
// 		for (; i + 4 <= count; i += 4) {
// 			const sel_t r0 = in_sel[i + 0];
// 			const sel_t r1 = in_sel[i + 1];
// 			const sel_t r2 = in_sel[i + 2];
// 			const sel_t r3 = in_sel[i + 3];
// 			if (mask->test(dict_idx[r0]))
// 				out_sel[out++] = r0;
// 			if (mask->test(dict_idx[r1]))
// 				out_sel[out++] = r1;
// 			if (mask->test(dict_idx[r2]))
// 				out_sel[out++] = r2;
// 			if (mask->test(dict_idx[r3]))
// 				out_sel[out++] = r3;
// 		}
// 		for (; i < count; ++i) {
// 			const sel_t r = in_sel[i];
// 			if (mask->test(dict_idx[r]))
// 				out_sel[out++] = r;
// 		}
// 	}
//
// 	count = out;
// 	return true;
// }
// bool TryDirectFilter(const FastLanesScanFilter& sf, Vector& vec, SelectionVector& sel, idx_t& count) {
// 	if (vec.GetVectorType() != VectorType::DICTIONARY_VECTOR)
// 		return false;
//
// 	switch (sf.filter.filter_type) {
// 	case TableFilterType::IN_FILTER:
// 		break;
// 	case TableFilterType::CONSTANT_COMPARISON: {
// 		if (const auto& constant_filter = sf.filter.Cast<ConstantFilter>();
// 		    constant_filter.comparison_type == ExpressionType::COMPARE_EQUAL) {
// 			break;
// 		}
// 		return false;
// 	}
// 	default:
// 		return false;
// 	}
//
// 	if (sf.allowed_dict_ids.empty()) {
// 		return false;
// 	}
//
// 	auto& dict_sel = DictionaryVector::SelVector(vec);
//
// 	idx_t out = 0;
// 	for (idx_t i = 0; i < count; ++i) {
// 		const auto row = sel.get_index(i);
// 		const auto id  = dict_sel.get_index(row); // dict id (0..dict_size-1)
// 		// Defensive bound check in case of mismatch
// 		if (static_cast<size_t>(id) < sf.allowed_dict_ids.size() && sf.allowed_dict_ids[id]) {
// 			sel.set_index(out++, row);
// 		}
// 	}
// 	count = out;
// 	return true;
// }

bool FsstDictDirectFilter(
    const FastLanesScanFilter& sf, const FSSTDictFilterCtx&, Vector& vec, SelectionVector& sel, idx_t& count) {
	switch (sf.filter.filter_type) {
	case TableFilterType::IN_FILTER:
		break;
	case TableFilterType::CONSTANT_COMPARISON: {
		const auto& cf = sf.filter.Cast<ConstantFilter>();
		if (cf.comparison_type != ExpressionType::COMPARE_EQUAL)
			return false;
		break;
	}
	default:
		return false;
	}

	return true;
}

bool TryDirectFilter(const FastLanesScanFilter& sf, Vector& vec, SelectionVector& sel, idx_t& count) {
	switch (vec.GetVectorType()) {
	case VectorType::DICTIONARY_VECTOR: {
		if (const auto* d = sf.ctx.Maybe<FSSTDictFilterCtx, DirectFilterKind::DictMask>()) {
			return FsstDictDirectFilter(sf, *d, vec, sel, count);
		}
		break;
	}
	default:
		return false;
	}

	return false;
}

void FilterExecutor::Apply(DataChunk&                              chunk,
                           const unique_ptr<AdaptiveFilter>&       adaptive_filter,
                           const std::vector<FastLanesScanFilter>& scan_filters,
                           const TableFilterSet*                   filters) {
	if (!filters || filters->filters.empty()) {
		return;
	}

	const idx_t scan_count = chunk.size();
	if (scan_count == 0) {
		return;
	}

	// duckdb::fls_filter_stats.chunks++;
	// duckdb::fls_filter_stats.total_rows_scanned += scan_count;
	// duckdb::fls_filter_stats.last_in_rows = scan_count;

	SelectionVector sel(STANDARD_VECTOR_SIZE);
	for (idx_t i = 0; i < scan_count; ++i) {
		sel.set_index(i, static_cast<sel_t>(i));
	}
	idx_t active_count = scan_count;

	const auto filter_state = adaptive_filter->BeginFilter();
	for (idx_t filter_idx = 0; filter_idx < scan_filters.size(); filter_idx++) {
		auto& scan_filter   = scan_filters[adaptive_filter->permutation[filter_idx]];
		auto& result_vector = chunk.data[scan_filter.filter_idx];

		// {
		// 	idx_t direct_count = active_count;
		// 	if (TryDirectFilter(scan_filter, result_vector, sel, direct_count)) {
		// 		active_count = direct_count;
		// 		if (active_count == 0) {
		// 			break;
		// 		}
		// 		continue;
		// 	}
		// }

		UnifiedVectorFormat unified;
		result_vector.ToUnifiedFormat(scan_count, unified);

		idx_t filter_count = active_count;
		ColumnSegment::FilterSelection(
		    sel, result_vector, unified, scan_filter.filter, *scan_filter.filter_state, scan_count, filter_count);

		// if (filter_count < active_count) {
		// 	duckdb::fls_filter_stats.removed_by_fallback += (active_count - filter_count);
		// }

		active_count = filter_count;
		if (active_count == 0) {
			break;
		}
	}
	adaptive_filter->EndFilter(filter_state);

	if (active_count != scan_count) {
		chunk.Slice(sel, active_count);
	}

	// duckdb::fls_filter_stats.total_rows_output += active_count;
	// duckdb::fls_filter_stats.last_out_rows = active_count;

	// Per-chunk logging (comment out if too verbose)
	// const idx_t  in  = duckdb::fls_filter_stats.last_in_rows;
	// const idx_t  out = duckdb::fls_filter_stats.last_out_rows;
	// const double pct = in ? (100.0 * (in - out) / double(in)) : 0.0;
	// std::fprintf(stderr,
	//              "[FLS][chunk] in=%llu out=%llu removed=%llu (%.2f%%)\n",
	//              (unsigned long long)in,
	//              (unsigned long long)out,
	//              (unsigned long long)(in - out),
	//              pct);

	// Optional: print running totals every N chunks
	// if ((duckdb::fls_filter_stats.chunks % 64) == 0) {
	// 	duckdb::FLSLogChunkStats(duckdb::fls_filter_stats);
	// }
}

} // namespace duckdb
