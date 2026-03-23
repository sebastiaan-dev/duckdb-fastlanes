#!/usr/bin/env python3
from __future__ import annotations

import argparse
import statistics
import sys
import time
from pathlib import Path
from typing import Iterable, List, Optional, Sequence

import duckdb

from scripts.common.duckdb_utils import ensure_extension, open_connection
from scripts.common.paths import DATA_ROOT as BENCHMARK_DATA_ROOT
from scripts.common.tpch_utils import ScaleInfo, parse_scale_arg

DATA_ROOT = BENCHMARK_DATA_ROOT / "tpch"
DEFAULT_TABLE = "lineitem"
DEFAULT_COLUMN = "l_extendedprice"
WARMUP_RUNS = 2
ROW_GROUP_SIZE = 65536


def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Run a simple SUM aggregate against TPCH data from duckdb, parquet, or FastLanes files."
        )
    )
    parser.add_argument(
        "--format",
        choices=("duckdb", "parquet", "fls"),
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
    return parser.parse_args(argv)


def connect(database_path: Optional[Path] = None) -> duckdb.DuckDBPyConnection:
    conn = open_connection(database_path)
    conn.execute("PRAGMA threads=1;")
    # conn.execute("PRAGMA enable_object_cache")
    # conn.execute("SET enable_external_file_cache = TRUE")
    conn.execute("PRAGMA disable_object_cache")
    conn.execute("SET enable_external_file_cache = FALSE")

    return conn


def ensure_fastlanes(conn: duckdb.DuckDBPyConnection) -> None:
    try:
        ensure_extension(conn, "fastlanes")
    except duckdb.Error as exc:
        raise RuntimeError(
            "Failed to load FastLanes extension. Build the project (`make`) "
            "or ensure the extension is available to DuckDB."
        ) from exc


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
    table: str, column: str, scale: ScaleInfo, runs: int, operator: str
) -> None:
    db_path = DATA_ROOT / "duckdb" / f"{scale.dataset_name(ROW_GROUP_SIZE)}.duckdb"
    if not db_path.exists():
        raise FileNotFoundError(f"DuckDB database not found at {db_path}")
    conn = connect(db_path)
    try:
        table_name = validate_identifier(table, "table")
        column_name = validate_identifier(column, "column")
        if operator == "distinct":
            select_sql = f"SELECT COUNT(DISTINCT {column_name}) FROM {table_name};"
        else:
            select_sql = f"SELECT {operator.upper()}({column_name}) FROM {table_name};"
        execute_runs(conn, select_sql, (), runs)
    finally:
        conn.close()


def run_parquet_query(
    table: str, column: str, scale: ScaleInfo, runs: int, operator: str
) -> None:
    parquet_path = (
        DATA_ROOT / "parquet" / scale.dataset_name(ROW_GROUP_SIZE) / f"{table}.parquet"
    )
    if not parquet_path.exists():
        raise FileNotFoundError(f"Parquet file not found at {parquet_path}")
    conn = connect()
    try:
        column_name = validate_identifier(column, "column")
        if operator == "distinct":
            select_sql = f"SELECT COUNT(DISTINCT {column_name}) FROM read_parquet(?);"
        else:
            select_sql = (
                f"SELECT {operator.upper()}({column_name}) FROM read_parquet(?);"
            )
        execute_runs(conn, select_sql, (str(parquet_path),), runs)
    finally:
        conn.close()


def run_fls_query(
    table: str, column: str, scale: ScaleInfo, runs: int, operator: str
) -> None:
    fls_path = DATA_ROOT / "fls" / scale.dataset_name(ROW_GROUP_SIZE) / f"{table}.fls"
    if not fls_path.exists():
        raise FileNotFoundError(f"FastLanes file not found at {fls_path}")
    conn = connect()
    try:
        ensure_fastlanes(conn)
        column_name = validate_identifier(column, "column")
        if operator == "distinct":
            select_sql = f"SELECT COUNT(DISTINCT {column_name}) FROM read_fls(?);"
        else:
            select_sql = f"SELECT {operator.upper()}({column_name}) FROM read_fls(?);"
        execute_runs(conn, select_sql, (str(fls_path),), runs)
    finally:
        conn.close()


def execute_runs(
    conn: duckdb.DuckDBPyConnection,
    query: str,
    parameters: Iterable[object],
    runs: int,
) -> None:
    if runs <= 0:
        raise ValueError("Number of runs must be positive")

    # Warm up caches to reduce first-run overhead.
    for _ in range(WARMUP_RUNS):
        conn.execute(query, parameters).fetchone()

    durations: List[float] = []
    last_result = None
    for run_idx in range(1, runs + 1):
        start = time.perf_counter()
        last_result = conn.execute(query, parameters).fetchone()
        elapsed = time.perf_counter() - start
        durations.append(elapsed)
        print(f"Run {run_idx}: result={last_result[0]} elapsed={elapsed:.4f}s")
    print(
        f"Completed {runs} runs. "
        f"mean={statistics.mean(durations):.4f}s "
        f"median={statistics.median(durations):.4f}s "
        f"min={min(durations):.4f}s "
        f"max={max(durations):.4f}s "
        f"last_result={last_result[0] if last_result else 'N/A'}"
    )


def main(argv: Optional[Sequence[str]] = None) -> None:
    args = parse_args(argv)
    runs = args.runs
    operator = args.operator
    table = args.table
    column = args.column
    scale: ScaleInfo = args.scale

    if args.format == "duckdb":
        run_duckdb_query(table, column, scale, runs, operator)
    elif args.format == "parquet":
        run_parquet_query(table, column, scale, runs, operator)
    elif args.format == "fls":
        run_fls_query(table, column, scale, runs, operator)
    else:
        raise ValueError(f"Unsupported format '{args.format}'")


if __name__ == "__main__":
    try:
        main()
    except (
        duckdb.Error,
        RuntimeError,
        FileNotFoundError,
        ValueError,
        argparse.ArgumentTypeError,
    ) as err:
        print(f"Error: {err}", file=sys.stderr)
        sys.exit(1)
