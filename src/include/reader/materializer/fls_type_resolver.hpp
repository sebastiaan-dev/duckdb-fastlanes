#pragma once

#include "duckdb/common/enum_util.hpp"
#include "duckdb/common/exception/conversion_exception.hpp"
#include "duckdb/common/operator/cast_operators.hpp"
#include "duckdb/common/type_util.hpp"
#include "duckdb/common/types/vector.hpp"
#include "fls/primitive/copy/fls_copy.hpp"
#include "fls_gen/untranspose/untranspose.hpp"
#include <array>
#include <cstring>
#include <iostream>
#include <type_traits>

namespace duckdb::materializer {

enum class Pass : bool { First = false, Second = true };

namespace detail {

template <typename SRC, typename DEST>
void CastNumeric(const SRC* __restrict src, DEST* __restrict dest, uint64_t count) {
	// if constexpr (std::is_same_v<std::make_unsigned_t<SRC>, std::make_unsigned_t<DEST>> &&
	//               sizeof(SRC) == sizeof(DEST)) {
	// 	std::memcpy(dest, src, count * sizeof(SRC));
	// 	return;
	// }

#pragma clang loop vectorize(enable) interleave(enable)
	for (uint64_t i = 0; i < count; ++i) {
		dest[i] = static_cast<DEST>(src[i]);
	}
}

template <typename T>
inline T LoadValue(const void* ptr) {
	T value;
	std::memcpy(&value, ptr, sizeof(T));
	return value;
}

template <typename VISITOR>
static void DispatchNumericPhysicalType(PhysicalType physical_type, VISITOR&& visitor) {
	switch (physical_type) {
	case PhysicalType::INT8:
		visitor.template operator()<int8_t>();
		break;
	case PhysicalType::INT16:
		visitor.template operator()<int16_t>();
		break;
	case PhysicalType::INT32:
		visitor.template operator()<int32_t>();
		break;
	case PhysicalType::INT64:
		visitor.template operator()<int64_t>();
		break;
	case PhysicalType::UINT8:
		visitor.template operator()<uint8_t>();
		break;
	case PhysicalType::UINT16:
		visitor.template operator()<uint16_t>();
		break;
	case PhysicalType::UINT32:
		visitor.template operator()<uint32_t>();
		break;
	case PhysicalType::UINT64:
		visitor.template operator()<uint64_t>();
		break;
	case PhysicalType::FLOAT:
		visitor.template operator()<float>();
		break;
	case PhysicalType::DOUBLE:
		visitor.template operator()<double>();
		break;
	case PhysicalType::INTERVAL:
	case PhysicalType::BOOL:
	case PhysicalType::INT128:
	case PhysicalType::UINT128:
	default:
		throw ConversionException("Materializer does not support casting to physical type " +
		                          EnumUtil::ToString(physical_type));
	}
}

template <Pass PASS, typename SRC>
struct CopyVisitor;

template <Pass PASS, typename SRC>
struct AssignVisitor;

template <Pass PASS, typename SRC>
struct UntransposeVisitor;

template <Pass PASS>
struct NumericHelper {
	template <typename T>
	static T* GetDataPtr(Vector& col) {
		if constexpr (PASS == Pass::First) {
			return FlatVector::GetData<T>(col);
		}
		D_ASSERT(col.GetVectorType() == VectorType::FLAT_VECTOR);
		return FlatVector::GetData<T>(col) + fastlanes::CFG::VEC_SZ;
	}

	template <typename SRC, typename DEST>
	static void CastAndStore(const SRC* src, DEST* dest) {
		CastNumeric(src, dest, fastlanes::CFG::VEC_SZ);

		// for (idx_t i = 0; i < fastlanes::CFG::VEC_SZ; ++i) {
		// 	DEST cast_value;
		// 	if (!TryCast::Operation<SRC, DEST>(src[i], cast_value, true)) {
		// 		throw ConversionException("Materializer cannot safely cast from physical type " +
		// 		                          EnumUtil::ToString(GetTypeId<SRC>()) + " to " +
		// 		                          EnumUtil::ToString(GetTypeId<DEST>()));
		// 	}
		// 	dest[i] = cast_value;
		// }
	}

	template <typename SRC>
	static void CopyVector(const SRC* src, Vector& col) {
		const auto physical_type   = col.GetType().InternalType();
		const auto source_physical = GetTypeId<SRC>();
		if (physical_type == source_physical) {
			auto dest = GetDataPtr<SRC>(col);
			fastlanes::copy<SRC>(src, dest);
			return;
		}

		DispatchNumericPhysicalType(physical_type, CopyVisitor<PASS, SRC> {src, col});
	}

	template <typename SRC>
	static void AssignValue(const SRC& value, Vector& col, idx_t index) {
		const auto physical_type   = col.GetType().InternalType();
		const auto source_physical = GetTypeId<SRC>();
		if (physical_type == source_physical) {
			auto dest   = GetDataPtr<SRC>(col);
			dest[index] = value;
			return;
		}

		DispatchNumericPhysicalType(physical_type, AssignVisitor<PASS, SRC> {value, col, index});
	}

	template <typename SRC>
	static void UntransposeBlock(SRC* transposed, Vector& col) {
		const auto physical_type   = col.GetType().InternalType();
		const auto source_physical = GetTypeId<SRC>();
		if (physical_type == source_physical) {
			auto dest = GetDataPtr<SRC>(col);
#if defined(FLS_NO_TRANSPOSE) && FLS_NO_TRANSPOSE
			fastlanes::copy<SRC>(transposed, dest);
#else
			generated::untranspose::fallback::scalar::untranspose_i(transposed, dest);
#endif
			return;
		}

#if defined(FLS_NO_TRANSPOSE) && FLS_NO_TRANSPOSE
		DispatchNumericPhysicalType(physical_type, CopyVisitor<PASS, SRC> {transposed, col});
#else
		DispatchNumericPhysicalType(physical_type, UntransposeVisitor<PASS, SRC> {transposed, col});
#endif
	}
};

template <Pass PASS, typename SRC>
struct CopyVisitor {
	const SRC* src;
	Vector&    col;

	template <typename DEST>
	void operator()() const {
		if constexpr (std::is_unsigned_v<SRC> && std::is_signed_v<DEST> && sizeof(SRC) == sizeof(DEST)) {
			auto dest = NumericHelper<PASS>::template GetDataPtr<DEST>(col);
			fastlanes::copy<SRC>(src, dest);
			return;
		}

		auto dest = NumericHelper<PASS>::template GetDataPtr<DEST>(col);
		NumericHelper<PASS>::template CastAndStore<SRC, DEST>(src, dest);
	}
};

template <Pass PASS, typename SRC>
struct AssignVisitor {
	const SRC& value;
	Vector&    col;
	idx_t      index;

	template <typename DEST>
	void operator()() const {
		DEST cast_value;
		if (!TryCast::Operation<SRC, DEST>(value, cast_value, true)) {
			throw ConversionException("Materializer cannot safely cast from physical type " +
			                          EnumUtil::ToString(GetTypeId<SRC>()) + " to " +
			                          EnumUtil::ToString(GetTypeId<DEST>()));
		}

		auto dest_ptr   = NumericHelper<PASS>::template GetDataPtr<DEST>(col);
		dest_ptr[index] = cast_value;
	}
};

template <Pass PASS, typename SRC>
struct UntransposeVisitor {
	SRC*    transposed;
	Vector& col;

	template <typename DEST>
	void operator()() const {
		auto out = NumericHelper<PASS>::template GetDataPtr<DEST>(col);
		if constexpr (std::is_same_v<SRC, DEST>) {
			generated::untranspose::fallback::scalar::untranspose_i(transposed, out);
		} else {
			std::array<SRC, fastlanes::CFG::VEC_SZ> temp_buffer;
			generated::untranspose::fallback::scalar::untranspose_i(transposed, temp_buffer.data());
			NumericHelper<PASS>::template CastAndStore<SRC, DEST>(temp_buffer.data(), out);
		}
	}
};

} // namespace detail

} // namespace duckdb::materializer
