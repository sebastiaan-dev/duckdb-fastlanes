#!/usr/bin/env python3
from __future__ import annotations

import argparse
import statistics
import sys
import time
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Iterable, List, Optional, Sequence

import duckdb


REPO_ROOT = Path(__file__).resolve().parents[1]
DATA_ROOT = REPO_ROOT / "benchmark" / "data" / "tpch"
DEFAULT_TABLE = "lineitem"
DEFAULT_COLUMN = "l_extendedprice"
FASTLANES_EXTENSION_CANDIDATES: Sequence[Path] = (
    REPO_ROOT / "build" / "release" / "extension" / "fastlanes" / "fastlanes.duckdb_extension",
    REPO_ROOT / "build" / "debug" / "extension" / "fastlanes" / "fastlanes.duckdb_extension",
)
WARMUP_RUNS = 2


@dataclass(frozen=True)
class ScaleInfo:
    original: str
    normalized: str
    slug: str

    @property
    def dataset_name(self) -> str:
        return f"tpch_sf{self.slug}"


def parse_scale(value: str) -> ScaleInfo:
    raw = value.replace("_", ".")
    try:
        numeric = Decimal(raw)
    except InvalidOperation as exc:
        raise argparse.ArgumentTypeError(f"Invalid scale factor '{value}'") from exc
    if numeric <= 0:
        raise argparse.ArgumentTypeError("Scale factor must be positive")
    normalized = format(numeric.normalize(), "f")
    slug = normalized.replace(".", "_")
    return ScaleInfo(original=value, normalized=normalized, slug=slug)


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
        type=parse_scale,
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
    return parser.parse_args(argv)


def connect(database_path: Optional[Path] = None) -> duckdb.DuckDBPyConnection:
    if database_path is not None:
        conn = duckdb.connect(str(database_path), config={"allow_unsigned_extensions": "true"})
    else:
        conn = duckdb.connect(config={"allow_unsigned_extensions": "true"})
    conn.execute("PRAGMA threads=1;")
    conn.execute("PRAGMA disable_object_cache")
    conn.execute("SET enable_external_file_cache = FALSE")

    return conn


def ensure_fastlanes(conn: duckdb.DuckDBPyConnection) -> None:
    for candidate in FASTLANES_EXTENSION_CANDIDATES:
        if candidate.exists():
            conn.load_extension(str(candidate))
            return
    try:
        conn.execute("LOAD fastlanes")
        return
    except duckdb.Error as exc:
        raise RuntimeError(
            "Failed to load FastLanes extension. Build the project (`make`) "
            "or ensure the extension is available to DuckDB."
        ) from exc


def validate_identifier(name: str, kind: str) -> str:
    if not name or not name.replace("_", "").replace(".", "").isalnum() or name[0].isdigit():
        raise ValueError(f"Invalid {kind} identifier '{name}'")
    if '"' in name:
        raise ValueError(f"{kind.capitalize()} identifier cannot contain double quotes")
    return name


def run_duckdb_query(table: str, column: str, scale: ScaleInfo, runs: int) -> None:
    db_path = DATA_ROOT / "duckdb" / f"{scale.dataset_name}.duckdb"
    if not db_path.exists():
        raise FileNotFoundError(f"DuckDB database not found at {db_path}")
    conn = connect(db_path)
    try:
        table_name = validate_identifier(table, "table")
        column_name = validate_identifier(column, "column")
        query = f"SELECT SUM({column_name}) FROM {table_name};"
        execute_runs(conn, query, (), runs)
    finally:
        conn.close()


def run_parquet_query(table: str, column: str, scale: ScaleInfo, runs: int) -> None:
    parquet_path = DATA_ROOT / "parquet" / scale.dataset_name / f"{table}.parquet"
    if not parquet_path.exists():
        raise FileNotFoundError(f"Parquet file not found at {parquet_path}")
    conn = connect()
    try:
        column_name = validate_identifier(column, "column")
        query = f"SELECT SUM({column_name}) FROM read_parquet(?);"
        execute_runs(conn, query, (str(parquet_path),), runs)
    finally:
        conn.close()


def run_fls_query(table: str, column: str, scale: ScaleInfo, runs: int) -> None:
    fls_path = DATA_ROOT / "fls" / scale.dataset_name / f"{table}.fls"
    if not fls_path.exists():
        raise FileNotFoundError(f"FastLanes file not found at {fls_path}")
    conn = connect()
    try:
        ensure_fastlanes(conn)
        column_name = validate_identifier(column, "column")
        # query = f"SELECT SUM({column_name}) FROM read_fls(?);"
        query = f"SELECT SUM({column_name}) FROM read_fls(?);"
        execute_runs(conn, query, (str(fls_path),), runs)
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
    table = args.table
    column = args.column
    scale: ScaleInfo = args.scale

    if args.format == "duckdb":
        run_duckdb_query(table, column, scale, runs)
    elif args.format == "parquet":
        run_parquet_query(table, column, scale, runs)
    elif args.format == "fls":
        run_fls_query(table, column, scale, runs)
    else:
        raise ValueError(f"Unsupported format '{args.format}'")


if __name__ == "__main__":
    try:
        main()
    except (duckdb.Error, RuntimeError, FileNotFoundError, ValueError, argparse.ArgumentTypeError) as err:
        print(f"Error: {err}", file=sys.stderr)
        sys.exit(1)
