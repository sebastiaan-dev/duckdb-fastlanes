PROJ_DIR := $(dir $(abspath $(lastword $(MAKEFILE_LIST))))

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
	cmake --build /Users/sebastiaan/repos/duckdb-fastlanes/build/release --target generate_fls -j 8

generate-data: build-fls-generator
	python3 -m pip install duckdb
	python3 scripts/data_generator/generate_test_data.py
	python3 scripts/data_generator/fls/generate_fls_files.py