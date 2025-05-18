.PHONY: bench-output-dir clean_benchmark

bench-output-dir:
	mkdir -p benchmark_results

clean_benchmark:
	rm -rf benchmark_results

bench-run-tpch: bench-output-dir
	./build/release/benchmark/benchmark_runner --root-dir './' 'benchmark/tpch/q03.benchmark' 2>&1 | tee benchmark_results/tpch-sf1-delta.csv