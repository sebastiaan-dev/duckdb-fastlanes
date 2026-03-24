#pragma once

#include "duckdb/common/types/vector.hpp"
#include "fls/writer/column_write_view.hpp"

namespace duckdb {

struct ViewWriterFactoryBase {
	virtual ~ViewWriterFactoryBase()                                               = default;
	virtual unique_ptr<fastlanes::ColumnWriteView> Build(Vector& src, idx_t count) = 0;
};

template <typename PT>
struct PrimitiveViewWriterFactory final : ViewWriterFactoryBase {
public:
	unique_ptr<fastlanes::ColumnWriteView> Build(Vector& src, idx_t count) override {
		// Ownership of the data is not needed if we use the span before the chunk (and its associated vectors) becomes
		// invalid.
		const auto data_ptr = FlatVector::GetData<PT>(src);
		return make_uniq<fastlanes::PrimitiveWriteView<PT>>(std::span<const PT> {data_ptr, count});
	}
};

struct Int128ViewWriterFactory final : ViewWriterFactoryBase {
public:
	explicit Int128ViewWriterFactory();
	unique_ptr<fastlanes::ColumnWriteView> Build(Vector& src, idx_t count) override;

private:
	vector<string> buf;
};

struct Uint128ViewWriterFactory final : ViewWriterFactoryBase {
public:
	explicit Uint128ViewWriterFactory();
	unique_ptr<fastlanes::ColumnWriteView> Build(Vector& src, idx_t count) override;

private:
	vector<string> buf;
};

struct StringViewWriterFactory final : ViewWriterFactoryBase {
public:
	explicit StringViewWriterFactory();
	unique_ptr<fastlanes::ColumnWriteView> Build(Vector& src, idx_t count) override;

private:
	vector<string> buf;
};

unique_ptr<ViewWriterFactoryBase> MakeViewWriterFactory(const PhysicalType& type);

} // namespace duckdb
