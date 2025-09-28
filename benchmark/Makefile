.PHONY: bench-output-dir clean_benchmark

BENCHMARK_PATTERN ?= .*

bench-output-dir:
	mkdir -p benchmark/results

clean_benchmark:
	rm -rf benchmark/results

############### BENCHMARK TARGETS ###############

TPCH_SCALE_FACTORS ?= 0.01 1
TPCH_FORMATS ?= fls parquet duckdb

define tpch_sf_slug
$(strip $(subst .,_, $(1)))
endef

define tpch_target_template
.PHONY: bench-run-tpch-sf$(1)-$(2)
bench-run-tpch-sf$(1)-$(2): bench-output-dir
	./build/release/benchmark/benchmark_runner --root-dir './' 'benchmark/queries/tpch/sf$(1)/$(2)/$(BENCHMARK_PATTERN)' 2>&1 | tee benchmark/results/tpch-sf$(1)-$(2).csv
endef

define tpch_aggregate_template
.PHONY: bench-run-tpch-sf$(1)
bench-run-tpch-sf$(1): $(foreach fmt,$(TPCH_FORMATS),bench-run-tpch-sf$(1)-$(fmt))
endef

$(foreach sf,$(TPCH_SCALE_FACTORS),$(foreach fmt,$(TPCH_FORMATS),$(eval $(call tpch_target_template,$(call tpch_sf_slug,$(sf)),$(fmt)))))
$(foreach sf,$(TPCH_SCALE_FACTORS),$(eval $(call tpch_aggregate_template,$(call tpch_sf_slug,$(sf)))))

DEFAULT_TPCH_SF := $(word 1,$(TPCH_SCALE_FACTORS))

.PHONY: bench-run-tpch
bench-run-tpch: bench-run-tpch-sf$(call tpch_sf_slug,$(DEFAULT_TPCH_SF))
