.PHONY: bench-output-dir clean_benchmark

ifeq ("$(BENCHMARK_PATTERN)a", "a")
	BENCHMARK_PATTERN:=.*
endif

bench-output-dir:
	mkdir -p benchmark_results

clean_benchmark:
	rm -rf benchmark_results

############### BENCHMARK TARGETS ###############

TPCH_SCALE_FACTORS ?= 0.01 1
TPCH_FORMATS ?= fls parquet duckdb

define tpch_sf_slug
$(subst .,_, $(1))
endef

define tpch_target_template
.PHONY: bench-run-tpch-sf$(call tpch_sf_slug,$(1))-$(2)
bench-run-tpch-sf$(call tpch_sf_slug,$(1))-$(2): bench-output-dir
	./build/release/benchmark/benchmark_runner --root-dir './' 'benchmark/tpch/sf$(call tpch_sf_slug,$(1))/$(2)/$(BENCHMARK_PATTERN)' 2>&1 | tee benchmark_results/tpch-sf$(call tpch_sf_slug,$(1))-$(2).csv
endef

define tpch_aggregate_template
.PHONY: bench-run-tpch-sf$(call tpch_sf_slug,$(1))
bench-run-tpch-sf$(call tpch_sf_slug,$(1)): $(foreach fmt,$(TPCH_FORMATS),bench-run-tpch-sf$(call tpch_sf_slug,$(1))-$(fmt))
endef

$(foreach sf,$(TPCH_SCALE_FACTORS),$(foreach fmt,$(TPCH_FORMATS),$(eval $(call tpch_target_template,$(sf),$(fmt)))))
$(foreach sf,$(TPCH_SCALE_FACTORS),$(eval $(call tpch_aggregate_template,$(sf))))

DEFAULT_TPCH_SF := $(word 1,$(TPCH_SCALE_FACTORS))

.PHONY: bench-run-tpch
bench-run-tpch: bench-run-tpch-sf$(call tpch_sf_slug,$(DEFAULT_TPCH_SF))
