#pragma once

#include "duckdb/common/types/vector.hpp"
#include "fls/writer/ColumnView.hpp"

namespace duckdb {

struct ViewWriterFactoryBase {
	virtual ~ViewWriterFactoryBase() = default;
	virtual unique_ptr<fastlanes::ColumnWriteView> Build(Vector &src, idx_t count) = 0;
};

template <typename PT>
struct PrimitiveViewWriterFactory final : ViewWriterFactoryBase {
public:
	unique_ptr<fastlanes::ColumnWriteView> Build(Vector &src, idx_t count) override {
		const auto data_ptr = FlatVector::GetData<PT>(src);
		return make_uniq<fastlanes::PrimitiveWriteView<PT>>(std::span<PT> {data_ptr, count});
	}
};

struct StringViewWriterFactory final : ViewWriterFactoryBase {
public:
	explicit StringViewWriterFactory();
	unique_ptr<fastlanes::ColumnWriteView> Build(Vector &src, idx_t count) override;

private:
	vector<string> buf;
};

unique_ptr<ViewWriterFactoryBase> MakeViewWriterFactory(PhysicalType phys);

} // namespace duckdb