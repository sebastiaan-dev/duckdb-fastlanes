#include "writer/fls_view_writer.hpp"

#include "duckdb/common/enum_util.hpp"

namespace duckdb {

StringViewWriterFactory::StringViewWriterFactory() {
	buf.resize(DEFAULT_STANDARD_VECTOR_SIZE);
}

unique_ptr<fastlanes::ColumnWriteView> StringViewWriterFactory::Build(Vector &src, const idx_t count) {
	const auto data_ptr = FlatVector::GetData<string_t>(src);
	for (idx_t i = 0; i < count; i++) {
		auto &s = data_ptr[i];
		buf[i].assign(s.GetData(), s.GetSize());
	}
	return make_uniq<fastlanes::StringWriteView>(std::span<std::string> {buf.data(), count});
}

unique_ptr<ViewWriterFactoryBase> MakeViewWriterFactory(const PhysicalType phys) {
	switch (phys) {
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