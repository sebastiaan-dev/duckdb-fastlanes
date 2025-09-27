#pragma once

#include "duckdb/execution/adaptive_filter.hpp"
#include "duckdb/storage/table/column_segment.hpp"
#include <vector>

namespace duckdb {
class TableFilterSet;

// Compact, branch-light bitmap for membership tests
// struct DictMask {
// 	std::unique_ptr<uint64_t[]> words;
// 	idx_t                       nbits  = 0;
// 	idx_t                       nwords = 0;
//
// 	void assign_zero(idx_t bits) {
// 		nbits  = bits;
// 		nwords = (bits + 63) >> 6;
// 		words  = std::unique_ptr<uint64_t[]>(new uint64_t[nwords]());
// 	}
// 	inline void set(idx_t id) {
// 		words[id >> 6] |= (1ULL << (id & 63));
// 	}
// 	inline bool test(idx_t id) const {
// 		return (words[id >> 6] >> (id & 63)) & 1ULL;
// 	}
// 	inline bool empty() const {
// 		return nbits == 0;
// 	}
// };

struct DictMask {
	// original storage (bitset, vector<bool>, or similar)
	std::vector<uint64_t> bits;

	// expanded mask (1 byte per entry), built once when mask is created/modified
	std::vector<uint8_t> byte_mask;

	DictMask() = default;

	DictMask(size_t nbits) {
		bits.resize((nbits + 63) / 64, 0);
		byte_mask.resize(nbits, 0);
	}

	void assign_zero(size_t nbits) {
		bits.assign((nbits + 63) / 64, 0);
		byte_mask.assign(nbits, 0);
	}

	inline void set(idx_t id) {
		bits[id >> 6] |= (1ULL << (id & 63));
		byte_mask[id] = 1;
	}

	inline void clear(idx_t id) {
		bits[id >> 6] &= ~(1ULL << (id & 63));
		byte_mask[id] = 0;
	}

	inline bool test(idx_t id) const {
		return byte_mask[id] != 0; // fast path
	}

	inline const uint8_t* as_byte_mask() const {
		return byte_mask.data();
	}

	bool empty() const {
		return std::all_of(byte_mask.begin(), byte_mask.end(), [](uint8_t v) { return v == 0; });
	}
};

enum class DirectFilterKind : uint8_t {
	DictMask,
};

struct FilterCtxBase {
	explicit FilterCtxBase(DirectFilterKind k)
	    : kind(k) {
	}
	virtual ~FilterCtxBase() = default;
	DirectFilterKind kind;
};

struct FSSTDictFilterCtx final : FilterCtxBase {
	FSSTDictFilterCtx()
	    : FilterCtxBase(DirectFilterKind::DictMask) {
	}
	std::shared_ptr<DictMask> allowed_dict_mask;
};

template <typename T, DirectFilterKind K>
T* AsKind(FilterCtxBase* b) {
	return (b && b->kind == K) ? static_cast<T*>(b) : nullptr;
}

struct FilterCtxHandle {
	template <typename T, typename... Args>
	T& Emplace(Args&&... args) {
		ctx = std::make_unique<T>(std::forward<Args>(args)...);
		return *static_cast<T*>(ctx.get());
	}

	template <typename T, DirectFilterKind K>
	T* Maybe() {
		return AsKind<T, K>(ctx.get());
	}

	template <typename T, DirectFilterKind K>
	const T* Maybe() const {
		return AsKind<T, K>(ctx.get());
	}

	void Reset() noexcept {
		ctx.reset();
	}

private:
	std::unique_ptr<FilterCtxBase> ctx;
};

struct FastLanesScanFilter {
	FastLanesScanFilter(ClientContext& context, idx_t filter_idx, TableFilter& filter);
	~FastLanesScanFilter();
	FastLanesScanFilter(FastLanesScanFilter&&) = default;

	idx_t                        filter_idx;
	TableFilter&                 filter;
	unique_ptr<TableFilterState> filter_state;

	FilterCtxHandle ctx;
};

class FilterExecutor {
public:
	static void Apply(DataChunk&                              chunk,
	                  const unique_ptr<AdaptiveFilter>&       adaptive_filter,
	                  const std::vector<FastLanesScanFilter>& scan_filters,
	                  const TableFilterSet*                   filters);
};

} // namespace duckdb
