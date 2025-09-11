.PHONY: bench-output-dir clean_benchmark

ifeq ("$(BENCHMARK_PATTERN)a", "a")
    BENCHMARK_PATTERN:=.*
endif

bench-output-dir:
	mkdir -p benchmark_results

clean_benchmark:
	rm -rf benchmark_results

############### BENCHMARK TARGETS ###############

###
# TPCH
###

bench-run-tpch-sf1-fls: bench-output-dir
	./build/release/benchmark/benchmark_runner --root-dir './' 'benchmark/tpch/sf1/fls/$(BENCHMARK_PATTERN)' 2>&1 | tee benchmark_results/tpch-sf1-fls.csv

bench-run-tpch-sf1-parquet: bench-output-dir
	./build/release/benchmark/benchmark_runner --root-dir './' 'benchmark/tpch/sf1/parquet/$(BENCHMARK_PATTERN)' 2>&1 | tee benchmark_results/tpch-sf1-parquet.csv

bench-run-tpch-sf1-duckdb: bench-output-dir
	./build/release/benchmark/benchmark_runner --root-dir './' 'benchmark/tpch/sf1/duckdb/$(BENCHMARK_PATTERN)' 2>&1 | tee benchmark_results/tpch-sf1-duckdb.csv

bench-run-tpch: bench-output-dir
	./build/release/benchmark/benchmark_runner --root-dir './' 'benchmark/tpch/q03.benchmark' 2>&1 | tee benchmark_results/tpch-sf1-delta.csv