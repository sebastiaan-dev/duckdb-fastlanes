PROJ_DIR := $(dir $(abspath $(lastword $(MAKEFILE_LIST))))

TPCH_SCALE_FACTORS ?= 1

# Configuration of extension
EXT_NAME=fastlanes
EXT_CONFIG=${PROJ_DIR}extension_config.cmake

ifeq (${BUILD_BENCHMARK}, 1)
	TOOLCHAIN_FLAGS:=${TOOLCHAIN_FLAGS} -DBUILD_BENCHMARKS=1
endif

# Include the Makefile from extension-ci-tools
include extension-ci-tools/makefiles/duckdb_extension.Makefile

include benchmark/Makefile

build-fls-generator:
	cmake --build build/release --target generate_fls -j 8

generate-data: build-fls-generator
	python3 -m pip install duckdb==v1.3.2
	python3 scripts/data_generator/generate_test_data.py --scale-factors $(TPCH_SCALE_FACTORS)
	python3 scripts/data_generator/generate_tpch_benchmarks.py --scale-factors $(TPCH_SCALE_FACTORS)

generate-vis:
	python3 scripts/plot_query_timings.py --scale-factor $(TPCH_SCALE_FACTORS)
