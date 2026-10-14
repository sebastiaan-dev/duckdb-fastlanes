.PHONY: clean
PROJ_DIR := $(dir $(abspath $(lastword $(MAKEFILE_LIST))))

# Configuration of extension
EXT_NAME=fastlanes
EXT_CONFIG=${PROJ_DIR}extension_config.cmake

ifeq (${BUILD_BENCHMARK}, 1)
	TOOLCHAIN_FLAGS:=${TOOLCHAIN_FLAGS} -DBUILD_BENCHMARKS=1
endif

# Optional local FastLanes checkout (otherwise CMake fetches the pinned patch stack).
ifneq ($(FASTLANES_SOURCE_DIR),)
	EXT_FLAGS += -DFETCHCONTENT_SOURCE_DIR_FLS_REPO="$(abspath $(FASTLANES_SOURCE_DIR))"
endif

# Include the Makefile from extension-ci-tools
include extension-ci-tools/makefiles/duckdb_extension.Makefile
