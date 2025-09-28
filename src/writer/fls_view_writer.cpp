#include "writer/fls_view_writer.hpp"
#include "duckdb/common/enum_util.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/types/decimal.hpp"
#include "duckdb/common/types/hugeint.hpp"
#include "fmt/core.h"
#include <algorithm>
#include <iostream>

namespace duckdb {

Int128ViewWriterFactory::Int128ViewWriterFactory() {
	buf.resize(DEFAULT_STANDARD_VECTOR_SIZE);
	for (auto& e : buf) {
		// Hugeint::HUGEINT_MINIMUM_STRING
		e.reserve(40);
	}
}

unique_ptr<fastlanes::ColumnWriteView> Int128ViewWriterFactory::Build(Vector& src, idx_t count) {
	const auto data_ptr = FlatVector::GetData<hugeint_t>(src);
	for (idx_t i = 0; i < count; i++) {
		auto& s   = data_ptr[i];
		auto  str = s.ToString();
		buf[i].assign(str.data(), str.size());
	}
	return make_uniq<fastlanes::StringWriteView>(std::span<std::string> {buf.data(), count});
}

Uint128ViewWriterFactory::Uint128ViewWriterFactory() {
	buf.resize(DEFAULT_STANDARD_VECTOR_SIZE);
	for (auto& e : buf) {
		// Hugeint::HUGEINT_MINIMUM_STRING
		e.reserve(39);
	}
}

unique_ptr<fastlanes::ColumnWriteView> Uint128ViewWriterFactory::Build(Vector& src, idx_t count) {
	const auto data_ptr = FlatVector::GetData<uhugeint_t>(src);
	for (idx_t i = 0; i < count; i++) {
		auto& s   = data_ptr[i];
		auto  str = s.ToString();
		buf[i].assign(str.data(), str.size());
	}
	return make_uniq<fastlanes::StringWriteView>(std::span<std::string> {buf.data(), count});
}

StringViewWriterFactory::StringViewWriterFactory() {
	buf.resize(DEFAULT_STANDARD_VECTOR_SIZE);
}

unique_ptr<fastlanes::ColumnWriteView> StringViewWriterFactory::Build(Vector& src, const idx_t count) {
	const auto data_ptr = FlatVector::GetData<string_t>(src);
	for (idx_t i = 0; i < count; i++) {
		auto& s = data_ptr[i];
		buf[i].assign(s.GetData(), s.GetSize());
	}
	return make_uniq<fastlanes::StringWriteView>(std::span<std::string> {buf.data(), count});
}

unique_ptr<ViewWriterFactoryBase> MakeViewWriterFactory(const PhysicalType& type) {
	switch (type) {
	case PhysicalType::BOOL:
		return make_uniq<PrimitiveViewWriterFactory<uint8_t>>();
	case PhysicalType::UINT8:
		return make_uniq<PrimitiveViewWriterFactory<uint8_t>>();
	case PhysicalType::INT8:
		return make_uniq<PrimitiveViewWriterFactory<int8_t>>();
	case PhysicalType::UINT16:
		return make_uniq<PrimitiveViewWriterFactory<uint16_t>>();
	case PhysicalType::INT16:
		return make_uniq<PrimitiveViewWriterFactory<int16_t>>();
	case PhysicalType::UINT32:
		return make_uniq<PrimitiveViewWriterFactory<uint32_t>>();
	case PhysicalType::INT32:
		return make_uniq<PrimitiveViewWriterFactory<int32_t>>();
	case PhysicalType::UINT64:
		return make_uniq<PrimitiveViewWriterFactory<uint64_t>>();
	case PhysicalType::INT64:
		return make_uniq<PrimitiveViewWriterFactory<int64_t>>();
	case PhysicalType::INT128:
		return make_uniq<Int128ViewWriterFactory>();
	case PhysicalType::UINT128:
		return make_uniq<Uint128ViewWriterFactory>();
	case PhysicalType::FLOAT:
		return make_uniq<PrimitiveViewWriterFactory<float>>();
	case PhysicalType::DOUBLE:
		return make_uniq<PrimitiveViewWriterFactory<double>>();
	case PhysicalType::VARCHAR:
		return make_uniq<StringViewWriterFactory>();
	default:
		throw NotImplementedException("PhysicalType %s is not supported by FastLanes writer", EnumUtil::ToString(phys));
	}
}

} // namespace duckdb
