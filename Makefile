PROJ_DIR := $(dir $(abspath $(lastword $(MAKEFILE_LIST))))

# Configuration of extension
EXT_NAME=quack
EXT_CONFIG=${PROJ_DIR}extension_config.cmake

# Core extensions that we need for testing
CORE_EXTENSIONS='tpcds;tpch;httpfs'

# Include the Makefile from the benchmarks directory
include benchmark/Makefile

# Include the Makefile from extension-ci-tools
include extension-ci-tools/makefiles/duckdb_extension.Makefile