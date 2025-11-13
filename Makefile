.PHONY: ramdisk remove-ramdisk clean generate-data generate-vis build-fls-generator vortex-extension vortex-clean vortex-fetch
PROJ_DIR := $(dir $(abspath $(lastword $(MAKEFILE_LIST))))

TPCH_SCALE_FACTORS ?= 1 10 30

VORTEX_REPO_URL := https://github.com/vortex-data/duckdb-vortex.git
VORTEX_COMMIT := 9ea698117199440f62a7cf1673afb647dc6437c7
VORTEX_SRC_DIR := ${PROJ_DIR}build/duckdb-vortex
VORTEX_EXTENSION_BUILD := ${VORTEX_SRC_DIR}/build/release/extension/vortex/vortex.duckdb_extension
VORTEX_EXTENSION_TARGET := ${PROJ_DIR}build/release/extension/vortex/vortex.duckdb_extension

# Configuration of extension
EXT_NAME=fastlanes
EXT_CONFIG=${PROJ_DIR}extension_config.cmake

ifeq (${BUILD_BENCHMARK}, 1)
	TOOLCHAIN_FLAGS:=${TOOLCHAIN_FLAGS} -DBUILD_BENCHMARKS=1
endif

# Include the Makefile from extension-ci-tools
include extension-ci-tools/makefiles/duckdb_extension.Makefile

include benchmark/Makefile

#
# Benchmark configuration
#

# WARNING: This should not be equal to or exceed the RAM capacity of the device.
RAMDISK_SIZE ?= 1024 * 13
RAMDISK_LABEL := fls-ramdisk
UNAME_S := $(shell uname -s)

ramdisk:
ifeq ($(UNAME_S),Darwin)
	@TOTAL_BYTES=$$(sysctl -n hw.memsize); \
	TOTAL_MB=$$(( TOTAL_BYTES / 1024 / 1024 )); \
	if [ $$(( TOTAL_MB - $(RAMDISK_SIZE) )) -lt 4096 ]; then \
	  echo "Error: RAMDISK_SIZE=$(RAMDISK_SIZE)MiB leaves less than 4096MiB free (total=$$TOTAL_MB MiB)."; \
	  exit 1; \
	fi;

	@echo "[MacOS] Creating $(RAMDISK_SIZE)MiB RAM disk: $(RAMDISK_LABEL)"
	@diskutil erasevolume EXFAT "$(RAMDISK_LABEL)" $$(hdiutil attach -nomount ram://$$(( $(RAMDISK_SIZE) * 2048 )))
	@echo "[MacOS] Mounted at /Volumes/$(RAMDISK_LABEL)"
else ifeq ($(UNAME_S),Linux)
	@TOTAL_KB=$$(awk '/MemTotal:/ {print $$2}' /proc/meminfo); \
	TOTAL_MB=$$(( TOTAL_KB / 1024 )); \
	if [ $$(( TOTAL_MB - $(RAMDISK_SIZE) )) -lt 4096 ]; then \
	  echo "Error: RAMDISK_SIZE=$(RAMDISK_SIZE)MiB leaves less than 4096MiB free (total=$$TOTAL_MB MiB)."; \
	  exit 1; \
	fi;

	@echo "[Linux] Creating RAM disk: $(RAMDISK_SIZE) MB at /dev/ram$(RD_INDEX)"
	@sudo modprobe brd rd_size=$(shell echo $$(( $(RAMDISK_SIZE) * 1024 ))) max_part=1 rd_nr=1
	@sudo mkfs.exfat /dev/ram0
	@sudo mkdir /mnt/$(RAMDISK_LABEL)
	@sudo mount /dev/ram0 /mnt/$(RAMDISK_LABEL)
	@echo "[Linux] Mounted at /mnt/$(RAMDISK_LABEL)"
else
	$(error Unsupported OS: $(UNAME_S))
endif

remove-ramdisk:
ifeq ($(UNAME_S),Darwin)
	@echo "[MacOS] Cleaning RAM disk"
	@diskutil eject   "/Volumes/$(RAMDISK_LABEL)" >/dev/null 2>&1 || true
else ifeq ($(UNAME_S),Linux)
	@echo "[Linux] Cleaning RAM disk"
	@sudo umount /mnt/$(RAMDISK_LABEL) >/dev/null 2>&1 || true
	@rmdir /mnt/$(RAMDISK_LABEL) >/dev/null 2>&1 || true
	@sudo rmmod brd >/dev/null 2>&1 || true
else
	$(error Unsupported OS: $(UNAME_S))
endif

bench-all: 
	$(MAKE) bench-per-iteration
	$(MAKE) bench-per-shared

bench-per-iteration: ramdisk
	python3 scripts/benchmark/run_query_benchmarks.py \
		--mean-relative-error 0.025 \
		--min-iterations 10 \
		--max-iterations 100 \
		--threads 1 2 4 8 \
		--ram-disk true \
		--target-dir /Volumes/fls-ramdisk \
		--object-cache true \
		--external-file-cache true \
		--benchmarks tpch volumetric \
		--connection-mode per-execution
	@$(MAKE) remove-ramdisk

bench-per-shared: ramdisk
	python3 scripts/benchmark/run_query_benchmarks.py \
		--mean-relative-error 0.025 \
		--min-iterations 10 \
		--max-iterations 100 \
		--threads 1 2 4 8 \
		--ram-disk true \
		--target-dir /Volumes/fls-ramdisk \
		--object-cache true \
		--external-file-cache true \
		--benchmarks tpch volumetric \
		--connection-mode group \
		--metadata benchmark/datav2/metadata-persist.duckdb
		--profile-dir benchmark/datav2/profiles-persist
	@$(MAKE) remove-ramdisk

# Might need: cmake -G Ninja -S . -B build
generate-data-v2: vortex-extension
	@./scripts/benchmark/generate.sh

generate-data:
	python3 -m pip install duckdb==v1.4.0
	python3 scripts/data_generator/generate_test_data.py --scale-factors $(TPCH_SCALE_FACTORS) --row-group-vectors 64
	python3 scripts/data_generator/generate_tpch_benchmarks.py --scale-factors $(TPCH_SCALE_FACTORS)

generate-vis:
	python3 scripts/plot_query_timings.py --scale-factor $(TPCH_SCALE_FACTORS)

vortex-fetch:
	@echo "Ensuring duckdb-vortex sources at ${VORTEX_COMMIT}"
	@if [ ! -d $(VORTEX_SRC_DIR)/.git ]; then \
		rm -rf $(VORTEX_SRC_DIR); \
		git clone --recurse-submodules $(VORTEX_REPO_URL) $(VORTEX_SRC_DIR); \
	else \
		cd $(VORTEX_SRC_DIR) && git fetch origin; \
	fi
	@cd $(VORTEX_SRC_DIR) && git checkout $(VORTEX_COMMIT)
	@cd $(VORTEX_SRC_DIR) && git submodule update --init --recursive

$(VORTEX_EXTENSION_BUILD): vortex-fetch
	@echo "Building duckdb-vortex extension"
	@$(MAKE) -C $(VORTEX_SRC_DIR) GEN=ninja release

$(VORTEX_EXTENSION_TARGET): $(VORTEX_EXTENSION_BUILD)
	@mkdir -p $(dir $(VORTEX_EXTENSION_TARGET))
	@cp $(VORTEX_EXTENSION_BUILD) $(VORTEX_EXTENSION_TARGET)

vortex-extension: $(VORTEX_EXTENSION_TARGET)

vortex-clean:
	@rm -rf $(VORTEX_SRC_DIR)
	@rm -f $(VORTEX_EXTENSION_TARGET)
