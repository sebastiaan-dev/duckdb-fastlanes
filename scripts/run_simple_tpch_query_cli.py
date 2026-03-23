from __future__ import annotations

import argparse
import re
import statistics
import subprocess
import sys
from pathlib import Path
from typing import List, Optional, Sequence


from scripts.common.duckdb_utils import sql_literal
from scripts.common.paths import (
    BUILD_ROOT,
    DATA_ROOT as BENCHMARK_DATA_ROOT,
    FASTLANES_EXTENSION,
    VORTEX_EXTENSION,
)
from scripts.common.tpch_utils import ScaleInfo, parse_scale_arg

DATA_ROOT = BENCHMARK_DATA_ROOT / "tpch"
DUCKDB_BINARY = BUILD_ROOT / "duckdb"
DEFAULT_TABLE = "lineitem"
DEFAULT_COLUMN = "l_extendedprice"
WARMUP_RUNS = 2
ROW_GROUP_SIZE = 65536
DUCKDB_CLI_FLAGS: Sequence[str] = ("-unsigned", "-csv", "-noheader", "-bail")


def resolve_duckdb_binary() -> Path:
    if not DUCKDB_BINARY.exists():
        raise FileNotFoundError(
            f"DuckDB binary not found at {DUCKDB_BINARY}. Build the project with 'make' first."
        )
    if not DUCKDB_BINARY.is_file():
        raise FileNotFoundError(
            f"Expected DuckDB binary at {DUCKDB_BINARY}, but found something else."
        )
    return DUCKDB_BINARY


def run_duckdb_script(
    statements: Sequence[str], database: Optional[Path]
) -> subprocess.CompletedProcess:
    binary = resolve_duckdb_binary()
    args: List[str] = [str(binary)]
    if database is not None:
        if not database.exists():
            raise FileNotFoundError(f"DuckDB database not found at {database}")
        args.append(str(database))
        args.extend(DUCKDB_CLI_FLAGS)
        args.append("-readonly")
    else:
        args.append(":memory:")
        args.extend(DUCKDB_CLI_FLAGS)
    script = "\n".join(statements)
    try:
        return subprocess.run(
            args,
            input=script,
            text=True,
            capture_output=True,
            check=True,
        )
    except subprocess.CalledProcessError as exc:
        stdout = exc.stdout.strip()
        stderr = exc.stderr.strip()
        details = stderr or stdout or "unknown error"
        raise RuntimeError(f"DuckDB command failed: {details}") from exc


def extract_result_and_runtime(output: str) -> tuple[str, float]:
    result: str | None = None
    runtime: float | None = None
    for line in output.splitlines():
        stripped = line.strip()
        if not stripped:
            continue
        if stripped.startswith("Run Time (s):"):
            match = re.search(r"Run Time \(s\):\s*real\s+([0-9.]+)", stripped)
            if not match:
                raise RuntimeError(f"Failed to parse runtime from: {stripped}")
            runtime = float(match.group(1))
        else:
            result = stripped
    if result is None:
        raise RuntimeError("DuckDB did not return a result value")
    if runtime is None:
        raise RuntimeError("DuckDB did not report a runtime (enable .timer?)")
    return result, runtime


def fastlanes_load_statement() -> str:
    return f"LOAD {sql_literal(FASTLANES_EXTENSION)};"


def vortex_load_statement() -> str:
    return f"LOAD {sql_literal(VORTEX_EXTENSION)};"


def base_session_statements(threads: int) -> List[str]:
    return [
        f"PRAGMA threads={threads};",
        "PRAGMA disable_object_cache;",
        "SET enable_external_file_cache = FALSE;",
    ]


def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Run a simple SUM aggregate against TPCH data from duckdb, parquet, or FastLanes files."
        )
    )
    parser.add_argument(
        "--format",
        choices=("duckdb", "parquet", "fls", "vortex"),
        required=True,
        help="Storage format to query",
    )
    parser.add_argument(
        "--scale",
        default="1",
        type=lambda value: parse_scale_arg(value, allow_zero=False),
        help="TPCH scale factor (e.g. 0.01, 1, 10). Defaults to 1.",
    )
    parser.add_argument(
        "--table",
        default=DEFAULT_TABLE,
        help=f"Table name to query (default: {DEFAULT_TABLE})",
    )
    parser.add_argument(
        "--column",
        default=DEFAULT_COLUMN,
        help=f"Column to aggregate (default: {DEFAULT_COLUMN})",
    )
    parser.add_argument(
        "--runs",
        type=int,
        default=5,
        help="Number of times to run the query (default: 5)",
    )
    parser.add_argument(
        "--operator",
        choices=("sum", "count", "max", "min", "avg", "distinct"),
        default="count",
        help="Aggregation operator to use (default: count)",
    )
    parser.add_argument(
        "--threads",
        type=int,
        default=1,
        help="Number of threads to use (default: 1)",
    )
    return parser.parse_args(argv)


def validate_identifier(name: str, kind: str) -> str:
    if (
        not name
        or not name.replace("_", "").replace(".", "").isalnum()
        or name[0].isdigit()
    ):
        raise ValueError(f"Invalid {kind} identifier '{name}'")
    if '"' in name:
        raise ValueError(f"{kind.capitalize()} identifier cannot contain double quotes")
    return name


def run_duckdb_query(
    table: str, column: str, scale: ScaleInfo, runs: int, operator: str, threads: int
) -> None:
    db_path = DATA_ROOT / "duckdb" / f"{scale.dataset_name(ROW_GROUP_SIZE)}.duckdb"
    if not db_path.exists():
        raise FileNotFoundError(f"DuckDB database not found at {db_path}")
    table_name = validate_identifier(table, "table")
    column_name = validate_identifier(column, "column")
    if operator == "distinct":
        select_sql = f"SELECT COUNT(DISTINCT {column_name}) FROM {table_name};"
    else:
        select_sql = f"SELECT {operator.upper()}({column_name}) FROM {table_name};"
    execute_runs(
        select_sql, runs=runs, database=db_path, extra_statements=(), threads=threads
    )


def run_parquet_query(
    table: str, column: str, scale: ScaleInfo, runs: int, operator: str, threads: int
) -> None:
    parquet_path = (
        DATA_ROOT / "parquet" / scale.dataset_name(ROW_GROUP_SIZE) / f"{table}.parquet"
    )
    if not parquet_path.exists():
        raise FileNotFoundError(f"Parquet file not found at {parquet_path}")
    column_name = validate_identifier(column, "column")
    select_sql = f"SELECT {operator.upper()}({column_name}) FROM read_parquet({sql_literal(parquet_path)});"
    execute_runs(
        select_sql, runs=runs, database=None, extra_statements=(), threads=threads
    )


def run_fls_query(
    table: str, column: str, scale: ScaleInfo, runs: int, operator: str, threads: int
) -> None:
    fls_path = DATA_ROOT / "fls" / scale.dataset_name(ROW_GROUP_SIZE) / f"{table}.fls"
    if not fls_path.exists():
        raise FileNotFoundError(f"FastLanes file not found at {fls_path}")
    column_name = validate_identifier(column, "column")
    select_sql = f"SELECT {operator.upper()}({column_name}) FROM read_fls({sql_literal(fls_path)});"
    execute_runs(
        select_sql,
        runs=runs,
        database=None,
        extra_statements=(fastlanes_load_statement(),),
        threads=threads,
    )


def run_vortex_query(
    table: str, column: str, scale: ScaleInfo, runs: int, operator: str, threads: int
) -> None:
    vortex_path = (
        DATA_ROOT / "vortex" / scale.dataset_name(ROW_GROUP_SIZE) / f"{table}.vortex"
    )
    if not vortex_path.exists():
        raise FileNotFoundError(f"Vortex file not found at {vortex_path}")
    column_name = validate_identifier(column, "column")
    select_sql = f"SELECT {operator.upper()}({column_name}) FROM read_vortex({sql_literal(vortex_path)});"
    execute_runs(
        select_sql,
        runs=runs,
        database=None,
        extra_statements=(vortex_load_statement(),),
        threads=threads,
    )


def execute_runs(
    select_sql: str,
    runs: int,
    database: Optional[Path],
    extra_statements: Sequence[str],
    threads: int,
) -> None:
    if runs <= 0:
        raise ValueError("Number of runs must be positive")

    statements = list(base_session_statements(threads))
    statements.extend(extra_statements)
    warmup_script = [*statements, select_sql]
    timed_script = [*statements, ".timer on", select_sql, ".timer off"]

    # Warm up caches to reduce first-run overhead.
    for _ in range(WARMUP_RUNS):
        run_duckdb_script(warmup_script, database)

    durations: List[float] = []
    last_result = None
    for run_idx in range(1, runs + 1):
        process = run_duckdb_script(timed_script, database)
        last_result, elapsed = extract_result_and_runtime(process.stdout)
        durations.append(elapsed)
        print(f"Run {run_idx}: result={last_result} elapsed={elapsed:.4f}s")

    print(
        f"Completed {runs} runs. "
        f"mean={statistics.mean(durations):.4f}s "
        f"median={statistics.median(durations):.4f}s "
        f"min={min(durations):.4f}s "
        f"max={max(durations):.4f}s "
        f"last_result={last_result if last_result is not None else 'N/A'}"
    )


def main(argv: Optional[Sequence[str]] = None) -> None:
    args = parse_args(argv)
    runs = args.runs
    table = args.table
    column = args.column
    operator = args.operator
    scale: ScaleInfo = args.scale
    threads = args.threads

    if args.format == "duckdb":
        run_duckdb_query(table, column, scale, runs, operator, threads)
    elif args.format == "parquet":
        run_parquet_query(table, column, scale, runs, operator, threads)
    elif args.format == "fls":
        run_fls_query(table, column, scale, runs, operator, threads)
    elif args.format == "vortex":
        run_vortex_query(table, column, scale, runs, operator, threads)
    else:
        raise ValueError(f"Unsupported format '{args.format}'")


if __name__ == "__main__":
    try:
        main()
    except (
        RuntimeError,
        FileNotFoundError,
        ValueError,
        argparse.ArgumentTypeError,
    ) as err:
        print(f"Error: {err}", file=sys.stderr)
        sys.exit(1)
