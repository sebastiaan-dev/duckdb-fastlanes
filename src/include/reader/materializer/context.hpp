#pragma once

#include "duckdb/common/types/vector.hpp"
#include "duckdb/common/types/vector_buffer.hpp"
#include <memory>

namespace duckdb::materializer {

template <Pass PASS, typename T>
static T* GetDataPtr(Vector& col) {
	if constexpr (PASS == Pass::First) {
		return FlatVector::GetData<T>(col);
	}

	D_ASSERT(col.GetVectorType() == VectorType::FLAT_VECTOR);
	return FlatVector::GetData<T>(col) + fastlanes::CFG::VEC_SZ;
}

template <Pass PASS>
static string_t* GetCompressedStringPtr(Vector& col) {
	if constexpr (PASS == Pass::First) {
		return FSSTVector::GetCompressedData<string_t>(col);
	}

	D_ASSERT(col.GetVectorType() == VectorType::FSST_VECTOR);
	return FSSTVector::GetCompressedData<string_t>(col) + fastlanes::CFG::VEC_SZ;
}

struct KeepAlive : VectorBuffer {
	std::shared_ptr<void> owner;
	explicit KeepAlive(std::shared_ptr<void> owner_p)
	    : VectorBuffer(VectorBufferType::MANAGED_BUFFER)
	    , owner(std::move(owner_p)) {
	}
};

struct ColumnCtxBase {
	virtual ~ColumnCtxBase() = default;

public:
	fastlanes::DataType data_type;
};

template <typename PT>
struct DictColumnCtx final : ColumnCtxBase {
	std::vector<PT> keys;
};

struct FSSTColumnCtx final : ColumnCtxBase {
	unique_ptr<Vector> aux_vec;
	buffer_ptr<void>   decoder;
};

struct FSSTDictColumnCtx final : ColumnCtxBase {
	size_t                      dict_size;
	unique_ptr<Vector>          dict_vec;
	unique_ptr<SelectionVector> sel_vec;
	buffer_ptr<void>            decoder;
};

struct ColumnCtxHandle {
	template <typename T, typename... Args>
	T& Emplace(Args&&... args) {
		static_assert(std::is_base_of<ColumnCtxBase, T>::value, "T must derive from ColumnCtxBase");
		ctx = std::make_unique<T>(std::forward<Args>(args)...);
		return *static_cast<T*>(ctx.get());
	}

	template <typename T>
	T& Expect() {
		static_assert(std::is_base_of<ColumnCtxBase, T>::value, "T must derive from ColumnCtxBase");
		return *static_cast<T*>(ctx.get());
	}

	void Reset() noexcept {
		ctx.reset();
	}

private:
	std::unique_ptr<ColumnCtxBase> ctx;
};

} // namespace duckdb::materializer
