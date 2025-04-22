#include <memory>
#include "include/bench_fsst_interface.h"

#include "duckdb.hpp"
#include "fls/connection.hpp"
#include "fls/cfg/cfg.hpp"
#include "fls/cor/prm/fsst/fsst.h"
#include "fls/expression/fsst_expression.hpp"
#include "fls_gen/untranspose/untranspose.hpp"

#include <iostream>

std::unique_ptr<BMContext> MakeBMContext(const std::filesystem::path &path) {
	auto ctx = std::make_unique<BMContext>();

	// --- FastLanes ---
	auto &fls_reader = ctx->conn.read_fls(path);
	const auto &expressions = fls_reader.get_chunk(0);
	const auto expr = expressions[0];
	expr->PointTo(0);
	ctx->opr = std::get<fastlanes::sp<fastlanes::dec_fsst_opr>>(expr->operators.back());

	// --- DuckDB ---
	ctx->output.Initialize(*ctx->db_conn.context.get(), {duckdb::LogicalType::VARCHAR});
	ctx->output.SetCapacity(1024);
	ctx->output.SetCardinality(1024);

	// store pointer into the first column
	ctx->target_col = &ctx->output.data[0];

	return ctx;
}

void BM_buffer_tmp_copy(const State &state, const std::filesystem::path &path) {
	// Setup
	auto ctx = MakeBMContext(path);
	auto &opr = ctx->opr;
	auto &target_col = *ctx->target_col;

	// Bench
	for (auto _ : state.n_iterations) {
		const auto target_ptr = duckdb::FlatVector::GetData<duckdb::string_t>(target_col);
		auto buffer = duckdb::make_buffer<duckdb::VectorStringBuffer>();
		auto *in_byte_arr = reinterpret_cast<uint8_t *>(opr->fsst_bytes_segment_view.data);

		for (auto i {0}; i < fastlanes::CFG::VEC_SZ; ++i) {
			generated::untranspose::fallback::scalar::untranspose_i(opr->offset_arr, opr->untrasposed_offset);

			fastlanes::len_t encoded_size {0};
			fastlanes::ofs_t offset {0};

			if (i == 0) {
				encoded_size = opr->untrasposed_offset[0];
			} else {
				offset = opr->untrasposed_offset[i - 1];
				const auto offset_next = opr->untrasposed_offset[i];
				encoded_size = offset_next - offset;
			}

			const auto decoded_size = static_cast<fastlanes::ofs_t>(
			    fsst_decompress(&opr->fsst_decoder, encoded_size, in_byte_arr,
			                    fastlanes::CFG::String::max_bytes_per_string, opr->tmp_string.data()));
			in_byte_arr += encoded_size;

			duckdb::string_t tmp = buffer->EmptyString(decoded_size);
			auto data_ptr = tmp.GetDataWriteable();
			memcpy(data_ptr, opr->tmp_string.data(), decoded_size);

			target_ptr[i] = tmp;
		}

		target_col.SetAuxiliary(buffer);
	}
}

void BM_mem_tmp_copy(const State &state, const std::filesystem::path &path) {
	// Setup
	auto ctx = MakeBMContext(path);
	auto &opr = ctx->opr;
	auto &target_col = *ctx->target_col;

	// Bench
	for (auto _ : state.n_iterations) {
		const auto target_ptr = duckdb::FlatVector::GetData<str_pointer>(target_col);

		duckdb::vector<u_int8_t> byte_arr_vec;
		auto *in_byte_arr = reinterpret_cast<uint8_t *>(opr->fsst_bytes_segment_view.data);

		for (auto i {0}; i < fastlanes::CFG::VEC_SZ; ++i) {
			generated::untranspose::fallback::scalar::untranspose_i(opr->offset_arr, opr->untrasposed_offset);

			fastlanes::len_t encoded_size {0};
			fastlanes::ofs_t offset {0};

			if (i == 0) {
				encoded_size = opr->untrasposed_offset[0];
			} else {
				offset = opr->untrasposed_offset[i - 1];
				const auto offset_next = opr->untrasposed_offset[i];
				encoded_size = offset_next - offset;
			}

			const auto decoded_size = static_cast<fastlanes::ofs_t>(
			    fsst_decompress(&opr->fsst_decoder, encoded_size, in_byte_arr,
			                    fastlanes::CFG::String::max_bytes_per_string, opr->tmp_string.data()));
			in_byte_arr += encoded_size;

			if (byte_arr_vec.capacity() - byte_arr_vec.size() < fastlanes::CFG::String::max_bytes_per_string) {
				byte_arr_vec.reserve(byte_arr_vec.size() + 1024 * fastlanes::CFG::String::max_bytes_per_string);
			}
			byte_arr_vec.insert(byte_arr_vec.end(), opr->tmp_string.begin(), opr->tmp_string.begin() + decoded_size);

			size_t start_offset = byte_arr_vec.size() - decoded_size;

			target_ptr[i].length = decoded_size;
			memcpy(target_ptr[i].prefix, &byte_arr_vec[start_offset], duckdb::string_t::PREFIX_LENGTH);
			target_ptr[i].ptr = reinterpret_cast<char *>(&byte_arr_vec[start_offset]);
		}
	}
}

/**
 * Simulate FastLanes storing the total size by decoding all entries and then summing the decoded sizes.
 */
uint64_t compute_total_size(fastlanes::sp<fastlanes::dec_fsst_opr> &opr) {
	auto *in_byte_arr = reinterpret_cast<uint8_t *>(opr->fsst_bytes_segment_view.data);

	auto total_size = 0;

	for (auto i {0}; i < fastlanes::CFG::VEC_SZ; ++i) {
		generated::untranspose::fallback::scalar::untranspose_i(opr->offset_arr, opr->untrasposed_offset);

		fastlanes::len_t encoded_size {0};
		fastlanes::ofs_t offset {0};

		if (i == 0) {
			encoded_size = opr->untrasposed_offset[0];
		} else {
			offset = opr->untrasposed_offset[i - 1];
			const auto offset_next = opr->untrasposed_offset[i];
			encoded_size = offset_next - offset;
		}

		const auto decoded_size = static_cast<fastlanes::ofs_t>(
		    fsst_decompress(&opr->fsst_decoder, encoded_size, in_byte_arr, fastlanes::CFG::String::max_bytes_per_string,
		                    opr->tmp_string.data()));
		in_byte_arr += encoded_size;
		total_size += decoded_size;
	}

	return total_size;
}

void BM_mem_total_size(const State &state, const std::filesystem::path &path) {
	// Setup
	auto ctx = MakeBMContext(path);
	auto &opr = ctx->opr;
	auto &target_col = *ctx->target_col;

	// Simulate the presence of a value being present in FastLanes which contains the total size
	auto total_size = compute_total_size(opr);

	// Bench
	for (auto _ : state.n_iterations) {
		const auto target_ptr = duckdb::FlatVector::GetData<str_pointer>(target_col);
		auto *in_byte_arr = reinterpret_cast<uint8_t *>(opr->fsst_bytes_segment_view.data);

		duckdb::vector<u_int8_t> byte_arr_vec;
		byte_arr_vec.resize(total_size);

		size_t current_offset = 0;
		auto *out_ptr = byte_arr_vec.data();

		for (auto i {0}; i < fastlanes::CFG::VEC_SZ; ++i) {
			generated::untranspose::fallback::scalar::untranspose_i(opr->offset_arr, opr->untrasposed_offset);

			fastlanes::len_t encoded_size {0};
			fastlanes::ofs_t offset {0};

			if (i == 0) {
				encoded_size = opr->untrasposed_offset[0];
			} else {
				offset = opr->untrasposed_offset[i - 1];
				const auto offset_next = opr->untrasposed_offset[i];
				encoded_size = offset_next - offset;
			}

			const auto decoded_size = static_cast<fastlanes::ofs_t>(
			    fsst_decompress(&opr->fsst_decoder, encoded_size, in_byte_arr,
			                    fastlanes::CFG::String::max_bytes_per_string, out_ptr + current_offset));
			in_byte_arr += encoded_size;

			target_ptr[i].length = decoded_size;
			memcpy(target_ptr[i].prefix, out_ptr + current_offset, duckdb::string_t::PREFIX_LENGTH);
			target_ptr[i].ptr = reinterpret_cast<char *>(out_ptr + current_offset);

			current_offset += decoded_size;
		}
	}
}