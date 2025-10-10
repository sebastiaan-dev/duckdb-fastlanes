#pragma once

#include "reader/materializer/fls_type_resolver.hpp"
#include <duckdb/common/types/string_type.hpp>
#include <duckdb/common/types/vector.hpp>
#include <fls/common/alias.hpp>
#include <fls/expression/dict_expression.hpp>

namespace duckdb::materializer {

template <typename KEY_PT, typename INDEX_PT>
struct KernelTraits<fastlanes::dec_dict_opr<KEY_PT, INDEX_PT>> {
	static void Prepare(ColumnCtxHandle&                           ctx,
	                    LogicalType&                               type,
	                    fastlanes::dec_dict_opr<KEY_PT, INDEX_PT>& op,
	                    const std::vector<FastLanesScanFilter*>*) {
		const auto size = op.key_segment_view.data_span.size();

		const auto* bytes   = op.key_segment_view.data_span.data();
		const auto* index_p = op.Index();

		switch (type.InternalType()) {
		case PhysicalType::INT8: {
			auto& c = ctx.Emplace<DictColumnCtx<int8_t>>();
			c.keys.resize(size);
			for (size_t i = 0; i < size; ++i) {
				auto val  = detail::LoadValue<KEY_PT>(bytes + i * sizeof(KEY_PT));
				c.keys[i] = static_cast<int8_t>(val);
			}
			break;
		}
		case PhysicalType::INT16: {
			auto& c = ctx.Emplace<DictColumnCtx<int16_t>>();
			c.keys.resize(size);
			for (size_t i = 0; i < size; ++i) {
				auto val  = detail::LoadValue<KEY_PT>(bytes + i * sizeof(KEY_PT));
				c.keys[i] = static_cast<int16_t>(val);
			}
			break;
		}
		case PhysicalType::INT32: {
			auto& c = ctx.Emplace<DictColumnCtx<int32_t>>();
			c.keys.resize(size);
			for (size_t i = 0; i < size; ++i) {
				auto val  = detail::LoadValue<KEY_PT>(bytes + i * sizeof(KEY_PT));
				c.keys[i] = static_cast<int32_t>(val);
			}
			break;
		}
		case PhysicalType::INT64: {
			auto& c = ctx.Emplace<DictColumnCtx<int64_t>>();
			c.keys.resize(size);
			for (size_t i = 0; i < size; ++i) {
				auto val  = detail::LoadValue<KEY_PT>(bytes + i * sizeof(KEY_PT));
				c.keys[i] = static_cast<int64_t>(val);
			}
			break;
		}
		default:
			throw std::runtime_error("Unsupported type");
		}
	}

	template <Pass PASS>
	static void Decode(ColumnCtxHandle& ctx,
	                   Vector&          col,
	                   idx_t,
	                   fastlanes::dec_dict_opr<KEY_PT, INDEX_PT>& op,
	                   fastlanes::DataType&                       src_type) {
		// const auto* bytes   = op.key_segment_view.data_span.data();
		const auto* index_p = op.Index();
		// const auto  size    = op.key_segment_view.data_span.size();
		// std::cout << "DuckDB type: " << EnumUtil::ToString(col.GetType().InternalType()) << "\n";
		// std::cout << "Descriptor type: " << fastlanes::ToStr(src_type) << "\n";
		// print_type<KEY_PT>();

		// auto target_ptr = GetDataPtr<PASS, KEY_PT>(col);
		// for (fastlanes::n_t i = 0; i < fastlanes::CFG::VEC_SZ; ++i) {
		// 	const size_t off = static_cast<size_t>(index_p[i]) * sizeof(KEY_PT);
		// 	auto         val = detail::LoadValue<KEY_PT>(bytes + off);
		// 	target_ptr[i]    = val;
		// }

		const auto phys = col.GetType().InternalType();
		switch (phys) {
		case PhysicalType::INT8: {
			auto& c          = ctx.Expect<DictColumnCtx<int8_t>>();
			auto  target_ptr = GetDataPtr<PASS, int8_t>(col);
			for (fastlanes::n_t i = 0; i < fastlanes::CFG::VEC_SZ; ++i) {
				target_ptr[i] = c.keys[index_p[i]];
			}
			break;
		}
		case PhysicalType::INT16: {
			auto& c          = ctx.Expect<DictColumnCtx<int16_t>>();
			auto  target_ptr = GetDataPtr<PASS, int16_t>(col);
			for (fastlanes::n_t i = 0; i < fastlanes::CFG::VEC_SZ; ++i) {
				target_ptr[i] = c.keys[index_p[i]];
			}
			break;
		}
		case PhysicalType::INT32: {
			auto& c          = ctx.Expect<DictColumnCtx<int32_t>>();
			auto  target_ptr = GetDataPtr<PASS, int32_t>(col);
			for (fastlanes::n_t i = 0; i < fastlanes::CFG::VEC_SZ; ++i) {
				target_ptr[i] = c.keys[index_p[i]];
			}
			break;
		}
		case PhysicalType::INT64: {
			auto& c          = ctx.Expect<DictColumnCtx<int64_t>>();
			auto  target_ptr = GetDataPtr<PASS, int64_t>(col);
			for (fastlanes::n_t i = 0; i < fastlanes::CFG::VEC_SZ; ++i) {
				target_ptr[i] = c.keys[index_p[i]];
			}
			break;
		}
		default:
			throw std::runtime_error("Unsupported type");
		}

		// const auto phys = col.GetType().InternalType();
		// switch (phys) {
		// case PhysicalType::INT16: {
		// 	auto dst = FlatVector::GetData<int16_t>(col);
		// 	for (fastlanes::n_t i = 0; i < fastlanes::CFG::VEC_SZ; ++i) {
		// 		const size_t off = static_cast<size_t>(index_p[i]) * sizeof(KEY_PT);
		// 		auto         val = detail::LoadValue<KEY_PT>(bytes + off);
		// 		dst[i]           = static_cast<int16_t>(val);
		// 	}
		// 	break;
		// }
		// case PhysicalType::INT32: {
		// 	auto dst = FlatVector::GetData<int32_t>(col);
		// 	for (fastlanes::n_t i = 0; i < fastlanes::CFG::VEC_SZ; ++i) {
		// 		const size_t off = static_cast<size_t>(index_p[i]) * sizeof(KEY_PT);
		// 		auto         val = detail::LoadValue<KEY_PT>(bytes + off);
		// 		dst[i]           = static_cast<int32_t>(val);
		// 	}
		// 	break;
		// }
		// case PhysicalType::INT64: {
		// 	auto dst = FlatVector::GetData<int64_t>(col);
		// 	for (fastlanes::n_t i = 0; i < fastlanes::CFG::VEC_SZ; ++i) {
		// 		const size_t off = static_cast<size_t>(index_p[i]) * sizeof(KEY_PT);
		// 		auto         val = detail::LoadValue<KEY_PT>(bytes + off);
		// 		dst[i]           = static_cast<int64_t>(val); // widening sign-extend
		// 	}
		// 	break;
		// }
		// default:
		// 	throw std::runtime_error("Unsupported DECIMAL physical type");
		// }

		// for (fastlanes::n_t i = 0; i < fastlanes::CFG::VEC_SZ; ++i) {
		// 	const size_t off = static_cast<size_t>(index_p[i]) * sizeof(KEY_PT);
		// 	auto         val = detail::LoadValue<KEY_PT>(bytes + off);
		// 	detail::NumericHelper<PASS>::template AssignValue<KEY_PT>(val, col, i);
		// }
	}
};

template <typename INDEX_PT>
struct KernelTraits<fastlanes::dec_dict_opr<fastlanes::fls_string_t, INDEX_PT>> {
	static void Prepare(ColumnCtxHandle&,
	                    LogicalType&,
	                    fastlanes::dec_dict_opr<fastlanes::fls_string_t, INDEX_PT>&,
	                    const std::vector<FastLanesScanFilter*>*) {
	}

	template <Pass PASS>
	static void Decode(ColumnCtxHandle&,
	                   Vector& col,
	                   idx_t,
	                   fastlanes::dec_dict_opr<fastlanes::fls_string_t, INDEX_PT>& op,
	                   fastlanes::DataType&) {
		col.SetVectorType(VectorType::FLAT_VECTOR);
		auto        target_ptr = GetDataPtr<PASS, string_t>(col);
		const auto* offsets    = op.dict_offsets_segment.data_span.data();
		const auto* idx_arr    = op.Index();
		const auto* bytes      = op.Bytes();
		for (fastlanes::n_t i = 0; i < fastlanes::CFG::VEC_SZ; ++i) {
			const INDEX_PT k   = idx_arr[i];
			const auto     cur = detail::LoadValue<fastlanes::ofs_t>(offsets + k * sizeof(fastlanes::ofs_t));
			const auto     prev =
                (k == 0) ? 0 : detail::LoadValue<fastlanes::ofs_t>(offsets + (k - 1) * sizeof(fastlanes::ofs_t));
			target_ptr[i] = StringVector::AddString(col, reinterpret_cast<const char*>(bytes + prev), cur - prev);
		}
	}
};

} // namespace duckdb::materializer
