#!/usr/bin/env python
"""
Volumetric scan benchmark runner for DuckDB datasets.

This script scans the datasets stored under benchmark/data/<topic>/<format>/...
For every table it generates a set of volumetric aggregation queries based on the
column data types and executes each query multiple times while collecting
profiling output per run in JSON format.
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

import duckdb

# Supported file suffixes per format when the dataset is directory-based.
FORMAT_SUFFIXES: Dict[str, Tuple[str, ...]] = {
    "csv": (".csv",),
    "parquet": (".parquet",),
    "fls": (".fls",),
}

NUMERIC_BASE_TYPES = {
    "TINYINT",
    "SMALLINT",
    "INT",
    "INTEGER",
    "BIGINT",
    "HUGEINT",
    "UTINYINT",
    "USMALLINT",
    "UINTEGER",
    "UBIGINT",
    "UHUGEINT",
    "FLOAT",
    "REAL",
    "DOUBLE",
    "DECIMAL",
}

BOOLEAN_BASE_TYPES = {"BOOLEAN"}
DATE_BASE_TYPES = {"DATE"}
DATETIME_BASE_TYPES = {
    "TIMESTAMP",
    "TIMESTAMP_S",
    "TIMESTAMP_MS",
    "TIMESTAMP_NS",
    "TIMESTAMP_TZ",
}
STRING_BASE_TYPES = {"VARCHAR", "CHAR", "BPCHAR", "TEXT", "STRING"}
BLOB_BASE_TYPES = {"BLOB", "VARBINARY", "BYTEA"}

TABLE_OPERATOR_TYPES = {"TABLE_SCAN", "SEQ_SCAN", "TABLE_FUNCTION", "INDEX_SCAN"}
TABLE_OPERATOR_NAMES = {
    "READ_CSV",
    "READ_CSV_AUTO",
    "READ_FLS",
    "READ_PARQUET",
    "PARQUET_SCAN",
    "FLS_SCAN",
    "SEQ_SCAN",
}
TABLE_OPERATOR_PREFIXES = ("READ_", "PARQUET_", "FLS_")

STATUS_LINE_WIDTH = 100

REPO_ROOT = Path(__file__).resolve().parent.parent
FASTLANES_EXTENSION_PATH = (
        REPO_ROOT
        / "build"
        / "release"
        / "extension"
        / "fastlanes"
        / "fastlanes.duckdb_extension"
)


@dataclass
class DatasetSpec:
    topic: str
    file_format: str
    path: Path
    dataset_name: str


@dataclass
class TableRef:
    schema: Optional[str]
    name: str

    @property
    def qualified_name(self) -> str:
        if self.schema:
            return f"{quote_identifier(self.schema)}.{quote_identifier(self.name)}"
        return quote_identifier(self.name)

    @property
    def display_name(self) -> str:
        if self.schema and self.schema.lower() != "main":
            return f"{self.schema}.{self.name}"
        return self.name


@dataclass
class ColumnInfo:
    name: str
    type: str


@dataclass
class BenchmarkResult:
    table: str
    column: str
    column_type: str
    operation: str
    query: str
    runs: List[Dict[str, object]]


class BenchmarkError(Exception):
    pass


def quote_identifier(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'


def escape_literal(value: str) -> str:
    return value.replace("'", "''")


def serialise_value(value: Any) -> Any:
    if value is None or isinstance(value, (int, float, str, bool)):
        return value
    return str(value)


def serialise_rows(rows: Sequence[Sequence[Any]]) -> List[List[Any]]:
    return [[serialise_value(item) for item in row] for row in rows]


def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run volumetric scans against benchmark datasets using DuckDB."
    )
    parser.add_argument(
        "--data-root",
        default="benchmark/data",
        help="Root directory containing benchmark datasets",
    )
    parser.add_argument(
        "--results-root",
        default="benchmark/results/volumetric",
        help="Directory where benchmark results and profiles will be stored",
    )
    parser.add_argument(
        "--topics",
        nargs="*",
        help="Optional list of topics to include (defaults to all topics)",
    )
    parser.add_argument(
        "--formats",
        nargs="*",
        help="Optional list of file formats to include (defaults to all formats)",
    )
    parser.add_argument(
        "--datasets",
        nargs="*",
        help="Optional list of dataset names (e.g. tpch_sf1) to include",
    )
    parser.add_argument(
        "--tables",
        nargs="*",
        help="Optional list of table names to include (applied after loading)",
    )
    parser.add_argument(
        "--iterations",
        type=int,
        default=3,
        help="Number of measured executions per benchmark",
    )
    parser.add_argument(
        "--threads",
        type=int,
        help="Value to set for DuckDB's threads setting",
    )
    parser.add_argument(
        "--warmup",
        action="store_true",
        help="Execute an unmeasured warmup run before each benchmark",
    )
    group_object_cache = parser.add_mutually_exclusive_group()
    group_object_cache.add_argument(
        "--enable-object-cache",
        action="store_true",
        help="Enable DuckDB's object cache (PRAGMA enable_object_cache)",
    )
    group_object_cache.add_argument(
        "--disable-object-cache",
        action="store_true",
        help="Disable DuckDB's object cache (PRAGMA disable_object_cache)",
    )
    group_external_cache = parser.add_mutually_exclusive_group()
    group_external_cache.add_argument(
        "--enable-external-file-cache",
        action="store_true",
        help="Set enable_external_file_cache = TRUE",
    )
    group_external_cache.add_argument(
        "--disable-external-file-cache",
        action="store_true",
        help="Set enable_external_file_cache = FALSE",
    )
    parser.add_argument(
        "--load-extension",
        action="append",
        default=[],
        metavar="EXT",
        help="DuckDB extension(s) to LOAD before running benchmarks",
    )
    parser.add_argument(
        "--profile-subdir",
        default="profiles",
        help="Subdirectory under each dataset result directory to store profiles",
    )
    parser.add_argument(
        "--parquet-row-group-size",
        type=int,
        help="Override row group size when reading Parquet files",
    )
    parser.add_argument(
        "--fastlanes-row-group-size",
        type=int,
        help="Override row group size when reading FastLanes files",
    )
    args = parser.parse_args(argv)
    if args.iterations < 1:
        parser.error("--iterations must be at least 1")
    return args


def discover_datasets(
    data_root: Path,
    topics: Optional[Sequence[str]] = None,
    formats: Optional[Sequence[str]] = None,
    dataset_names: Optional[Sequence[str]] = None,
) -> List[DatasetSpec]:
    datasets: List[DatasetSpec] = []
    if not data_root.exists():
        raise BenchmarkError(f"Data root '{data_root}' does not exist")

    for topic_dir in sorted(p for p in data_root.iterdir() if p.is_dir()):
        if topic_dir.name.startswith('.'):
            continue
        if topics and topic_dir.name not in topics:
            continue
        for format_dir in sorted(p for p in topic_dir.iterdir() if p.is_dir()):
            if format_dir.name.startswith('.'):
                continue
            if formats and format_dir.name not in formats:
                continue
            for entry in sorted(format_dir.iterdir()):
                if entry.name.startswith('.'):
                    continue
                dataset_name = entry.stem if entry.is_file() else entry.name
                if dataset_names and dataset_name not in dataset_names:
                    continue
                datasets.append(
                    DatasetSpec(
                        topic=topic_dir.name,
                        file_format=format_dir.name,
                        path=entry,
                        dataset_name=dataset_name,
                    )
                )
    return datasets


def maybe_load_extensions(conn: duckdb.DuckDBPyConnection, extensions: Iterable[str]) -> None:
    for ext in extensions:
        ext = ext.strip()
        if not ext:
            continue
        candidate_path = Path(ext)
        if candidate_path.suffix == ".duckdb_extension" and candidate_path.exists():
            conn.load_extension(str(candidate_path))
            continue

        if ext.lower() == "fastlanes" and FASTLANES_EXTENSION_PATH.exists():
            conn.load_extension(str(FASTLANES_EXTENSION_PATH))
            continue

        conn.execute(f"LOAD {ext}")


def configure_connection(
    conn: duckdb.DuckDBPyConnection,
    args: argparse.Namespace,
) -> None:
    if args.threads is not None:
        conn.execute(f"SET threads = {args.threads}")
    if args.enable_object_cache:
        conn.execute("PRAGMA enable_object_cache")
    elif args.disable_object_cache:
        conn.execute("PRAGMA disable_object_cache")
    if args.enable_external_file_cache:
        conn.execute("SET enable_external_file_cache = TRUE")
    elif args.disable_external_file_cache:
        conn.execute("SET enable_external_file_cache = FALSE")

    conn.execute("SET enable_profiling = 'json'")
    conn.execute("SET profiling_mode = 'detailed'")
    conn.execute("PRAGMA disable_profiling")


def ensure_format_loaded(
    conn: duckdb.DuckDBPyConnection,
    file_format: str,
    args: argparse.Namespace,
) -> None:
    requested_extensions = list(args.load_extension)
    lower_requested = {ext.lower() for ext in requested_extensions}
    if file_format == "fls" and "fastlanes" not in lower_requested:
        if FASTLANES_EXTENSION_PATH.exists():
            requested_extensions.insert(0, str(FASTLANES_EXTENSION_PATH))
        else:
            requested_extensions.insert(0, "fastlanes")
    if requested_extensions:
        maybe_load_extensions(conn, requested_extensions)


def register_tables(
    conn: duckdb.DuckDBPyConnection,
    dataset: DatasetSpec,
    args: argparse.Namespace,
) -> List[TableRef]:
    file_format = dataset.file_format.lower()
    path = dataset.path
    tables: List[TableRef] = []

    if file_format == "duckdb":
        rows = conn.execute(
            """
            SELECT table_schema, table_name
            FROM information_schema.tables
            WHERE table_schema NOT IN ('information_schema', 'pg_catalog')
            ORDER BY table_schema, table_name
            """
        ).fetchall()
        tables = [TableRef(schema=row[0], name=row[1]) for row in rows]
        if not tables:
            raise BenchmarkError(
                f"No tables found in duckdb file '{path}'."  # pragma: no cover - defensive
            )
        return tables

    if not path.is_dir():
        raise BenchmarkError(
            f"Expected directory for dataset '{dataset.dataset_name}' in format '{file_format}'."
        )

    suffixes = FORMAT_SUFFIXES.get(file_format)
    if not suffixes:
        raise BenchmarkError(f"Unsupported file format '{dataset.file_format}'.")

    for item in sorted(path.iterdir()):
        if not item.is_file() or item.suffix.lower() not in suffixes:
            continue

        table_name = item.stem
        literal_path = escape_literal(str(item))

        if file_format == "csv":
            conn.execute(
                f"CREATE OR REPLACE VIEW {quote_identifier(table_name)} AS "
                f"SELECT * FROM read_csv_auto('{literal_path}')"
            )
        elif file_format == "parquet":
            options = []
            if args.parquet_row_group_size:
                options.append(f"row_group_size={args.parquet_row_group_size}")
            option_clause = ", " + ", ".join(options) if options else ""
            conn.execute(
                f"CREATE OR REPLACE VIEW {quote_identifier(table_name)} AS "
                f"SELECT * FROM read_parquet('{literal_path}'{option_clause})"
            )
        elif file_format == "fls":
            options = []
            if args.fastlanes_row_group_size:
                options.append(f"row_group_size={args.fastlanes_row_group_size}")
            option_clause = ", " + ", ".join(options) if options else ""
            conn.execute(
                f"CREATE OR REPLACE VIEW {quote_identifier(table_name)} AS "
                f"SELECT * FROM read_fls('{literal_path}'{option_clause})"
            )
        else:  # pragma: no cover - defensive fallback
            raise BenchmarkError(f"Unsupported format '{file_format}'")

        tables.append(TableRef(schema=None, name=table_name))

    if not tables:
        raise BenchmarkError(
            f"No data files with suffix {suffixes} found in '{path}'."
        )

    return tables


def fetch_columns(
    conn: duckdb.DuckDBPyConnection,
    table: TableRef,
) -> List[ColumnInfo]:
    pragma_target = table.qualified_name
    rows = conn.execute(f"PRAGMA table_info({pragma_target})").fetchall()
    columns = [ColumnInfo(name=row[1], type=row[2]) for row in rows]
    if not columns:
        raise BenchmarkError(f"Table {table.display_name} has no columns.")
    return columns


def normalize_type(type_name: str) -> str:
    base = type_name.strip().upper()
    if not base:
        return base
    if '(' in base:
        base = base.split('(', 1)[0]
    if ' ' in base:
        base = base.split(' ', 1)[0]
    return base


def benchmarks_for_type(base_type: str) -> List[Tuple[str, str]]:
    if base_type in NUMERIC_BASE_TYPES:
        return [
            ("sum", "SELECT SUM({col}) FROM {table}"),
            ("avg", "SELECT AVG({col}) FROM {table}"),
            ("min", "SELECT MIN({col}) FROM {table}"),
            ("max", "SELECT MAX({col}) FROM {table}"),
        ]
    if base_type in BOOLEAN_BASE_TYPES:
        return [
            (
                "count_true",
                "SELECT SUM(CASE WHEN {col} THEN 1 ELSE 0 END) FROM {table}",
            ),
            ("count", "SELECT COUNT({col}) FROM {table}"),
        ]
    if base_type in DATE_BASE_TYPES or base_type in DATETIME_BASE_TYPES:
        return [
            ("min", "SELECT MIN({col}) FROM {table}"),
            ("max", "SELECT MAX({col}) FROM {table}"),
            ("count", "SELECT COUNT({col}) FROM {table}"),
        ]
    if base_type in STRING_BASE_TYPES:
        return [
            ("count", "SELECT COUNT({col}) FROM {table}"),
            ("count_distinct", "SELECT COUNT(DISTINCT {col}) FROM {table}"),
        ]
    if base_type in BLOB_BASE_TYPES:
        return [("count", "SELECT COUNT({col}) FROM {table}")]
    return []


def run_query_with_profiling(
    conn: duckdb.DuckDBPyConnection,
    query: str,
    profile_path: Path,
) -> Tuple[float, float, List[Tuple], Path]:
    profile_path.parent.mkdir(parents=True, exist_ok=True)
    target_path = profile_path.resolve()
    conn.execute(f"SET profiling_output = '{escape_literal(str(target_path))}'")
    conn.execute("PRAGMA enable_profiling")
    try:
        relation = conn.execute(query)
        rows = relation.fetchall()
    finally:
        conn.execute("PRAGMA disable_profiling")

    latency = extract_latency(target_path)
    table_time = extract_table_scan_time(target_path)
    return latency, table_time, rows, target_path


def extract_latency(profile_path: Path) -> float:
    try:
        with profile_path.open("r", encoding="utf-8") as fh:
            profile = json.load(fh)
        latency = profile.get("latency")
        if latency is None:
            return 0.0
        return float(latency)
    except Exception:  # pragma: no cover - defensive fallback
        return 0.0


def extract_table_scan_time(profile_path: Path) -> float:
    try:
        with profile_path.open("r", encoding="utf-8") as fh:
            profile = json.load(fh)
    except Exception:  # pragma: no cover - defensive fallback
        return 0.0
    return collect_table_operator_time(profile.get("children") or [])


def collect_table_operator_time(nodes: Sequence[Dict[str, Any]]) -> float:
    total = 0.0
    for node in nodes:
        if not isinstance(node, dict):
            continue
        total += get_operator_time(node)
        children = node.get("children") or []
        if isinstance(children, list):
            total += collect_table_operator_time(children)  # type: ignore[arg-type]
    return total


def get_operator_time(node: Dict[str, Any]) -> float:
    operator_type = str(node.get("operator_type", "")).upper()
    operator_name = str(node.get("operator_name", "")).strip().upper()
    extra = node.get("extra_info") or {}
    function_name = str(extra.get("Function", "")).strip().upper()

    is_table = False
    if operator_type in TABLE_OPERATOR_TYPES:
        is_table = True
    elif operator_name in TABLE_OPERATOR_NAMES:
        is_table = True
    elif any(operator_name.startswith(prefix) for prefix in TABLE_OPERATOR_PREFIXES):
        is_table = True
    elif function_name and (
        function_name in TABLE_OPERATOR_NAMES
        or any(function_name.startswith(prefix) for prefix in TABLE_OPERATOR_PREFIXES)
    ):
        is_table = True

    if not is_table:
        return 0.0
    timing = node.get("operator_timing")
    if timing is None:
        return 0.0
    try:
        return float(timing)
    except (TypeError, ValueError):  # pragma: no cover - defensive fallback
        return 0.0


def maybe_warmup(conn: duckdb.DuckDBPyConnection, query: str) -> None:
    conn.execute("PRAGMA disable_profiling")
    conn.execute(query).fetchall()


def run_benchmarks_for_table(
    conn: duckdb.DuckDBPyConnection,
    table: TableRef,
    columns: List[ColumnInfo],
    args: argparse.Namespace,
    results_dir: Path,
    profile_subdir: Path,
    include_tables: Optional[Sequence[str]] = None,
) -> List[BenchmarkResult]:
    results: List[BenchmarkResult] = []
    if include_tables:
        aliases = {
            table.name,
            table.display_name,
            table.qualified_name,
        }
        if not any(alias in include_tables for alias in aliases):
            return results

    for column in columns:
        base_type = normalize_type(column.type)
        benchmark_specs = benchmarks_for_type(base_type)
        if not benchmark_specs:
            continue
        col_identifier = quote_identifier(column.name)
        table_identifier = table.qualified_name

        for op_name, template in benchmark_specs:
            query = template.format(table=table_identifier, col=col_identifier)
            runs: List[Dict[str, object]] = []

            if args.warmup:
                try:
                    maybe_warmup(conn, query)
                except duckdb.Error as exc:
                    print(" " * STATUS_LINE_WIDTH, end="\r", flush=True)
                    raise BenchmarkError(
                        f"DuckDB failed during warmup for {table.display_name}.{column.name} [{op_name}]: {exc}"
                    ) from exc
                except Exception as exc:
                    print(" " * STATUS_LINE_WIDTH, end="\r", flush=True)
                    raise BenchmarkError(
                        f"Unexpected error during warmup for {table.display_name}.{column.name} [{op_name}]: {exc}"
                    ) from exc

            for run_index in range(1, args.iterations + 1):
                status_message = (
                    f"  -> {table.display_name}.{column.name} [{op_name}] run {run_index}/{args.iterations}"
                )
                print(status_message.ljust(STATUS_LINE_WIDTH), end="\r", flush=True)
                profile_path = (
                    profile_subdir
                    / f"{table.display_name}__{column.name}__{op_name}__run{run_index}.json"
                )
                try:
                    latency, table_time, rows, profile_file = run_query_with_profiling(
                        conn, query, profile_path
                    )
                except duckdb.Error as exc:
                    print(" " * STATUS_LINE_WIDTH, end="\r", flush=True)
                    raise BenchmarkError(
                        f"DuckDB failed while running {table.display_name}.{column.name} [{op_name}]: {exc}"
                    ) from exc
                except Exception as exc:
                    print(" " * STATUS_LINE_WIDTH, end="\r", flush=True)
                    raise BenchmarkError(
                        f"Unexpected error while running {table.display_name}.{column.name} [{op_name}]: {exc}"
                    ) from exc
                runs.append(
                    {
                        "run": run_index,
                        "duration_s": latency,
                        "table_function_time": table_time,
                        "rows": serialise_rows(rows),
                        "profiling_output": os.path.relpath(profile_file, results_dir),
                    }
                )

            results.append(
                BenchmarkResult(
                    table=table.display_name,
                    column=column.name,
                    column_type=column.type,
                    operation=op_name,
                    query=query,
                    runs=runs,
                )
            )

    print(" " * STATUS_LINE_WIDTH, end="\r", flush=True)

    return results


def build_config_tag(args: argparse.Namespace) -> str:
    parts = [
        f"threads-{args.threads if args.threads is not None else 'auto'}",
        f"objcache-{'on' if args.enable_object_cache else ('off' if args.disable_object_cache else 'default')}",
        f"extcache-{'on' if args.enable_external_file_cache else ('off' if args.disable_external_file_cache else 'default')}",
        f"iters-{args.iterations}",
        f"pqrg-{args.parquet_row_group_size if args.parquet_row_group_size else 'default'}",
        f"flsrg-{args.fastlanes_row_group_size if args.fastlanes_row_group_size else 'default'}",
    ]
    if args.warmup:
        parts.append("warmup")
    return "__".join(parts)


def run_dataset_benchmarks(
    dataset: DatasetSpec,
    args: argparse.Namespace,
    results_root: Path,
) -> Optional[Path]:
    dataset_dir = results_root / dataset.topic / dataset.file_format / dataset.dataset_name
    dataset_dir.mkdir(parents=True, exist_ok=True)
    config_tag = build_config_tag(args)
    profile_dir = dataset_dir / args.profile_subdir / config_tag
    profile_dir.mkdir(parents=True, exist_ok=True)

    conn: Optional[duckdb.DuckDBPyConnection] = None
    try:
        if dataset.file_format.lower() == "duckdb":
            conn = duckdb.connect(str(dataset.path), read_only=True, config={"allow_unsigned_extensions": "true"})
        else:
            conn = duckdb.connect(config={"allow_unsigned_extensions": "true"})

        ensure_format_loaded(conn, dataset.file_format.lower(), args)
        configure_connection(conn, args)
        tables = register_tables(conn, dataset, args)

        all_results: List[BenchmarkResult] = []
        for table in tables:
            columns = fetch_columns(conn, table)
            table_results = run_benchmarks_for_table(
                conn,
                table,
                columns,
                args,
                dataset_dir,
                profile_dir,
                include_tables=args.tables,
            )
            all_results.extend(table_results)

        if not all_results:
            print(
                f"No matching benchmarks for dataset {dataset.topic}/{dataset.file_format}/{dataset.dataset_name}",
                file=sys.stderr,
            )
            return None

        summary_path = dataset_dir / f"summary__{config_tag}.json"
        summary = {
            "topic": dataset.topic,
            "file_format": dataset.file_format,
            "dataset": dataset.dataset_name,
            "data_path": str(dataset.path),
            "config": {
                "threads": args.threads,
                "object_cache": "enable"
                if args.enable_object_cache
                else "disable"
                if args.disable_object_cache
                else "default",
                "external_file_cache": "enable"
                if args.enable_external_file_cache
                else "disable"
                if args.disable_external_file_cache
                else "default",
                "iterations": args.iterations,
                "warmup": args.warmup,
                "parquet_row_group_size": args.parquet_row_group_size,
                "fastlanes_row_group_size": args.fastlanes_row_group_size,
            },
            "benchmarks": [
                {
                    "table": result.table,
                    "column": result.column,
                    "column_type": result.column_type,
                    "operation": result.operation,
                    "query": result.query,
                    "runs": result.runs,
                }
                for result in all_results
            ],
        }
        with summary_path.open("w", encoding="utf-8") as fh:
            json.dump(summary, fh, indent=2)

        return summary_path
    finally:
        if conn is not None:
            conn.close()


def main(argv: Optional[Sequence[str]] = None) -> int:
    try:
        args = parse_args(argv)
        data_root = Path(args.data_root)
        results_root = Path(args.results_root)
        results_root.mkdir(parents=True, exist_ok=True)

        datasets = discover_datasets(
            data_root,
            topics=args.topics,
            formats=args.formats,
            dataset_names=args.datasets,
        )
        if not datasets:
            raise BenchmarkError("No datasets matched the provided filters.")

        processed = 0
        for dataset in datasets:
            print(
                f"Running volumetric benchmarks for {dataset.topic}/{dataset.file_format}/{dataset.dataset_name}..."
            )
            summary_path = run_dataset_benchmarks(dataset, args, results_root)
            if summary_path:
                print(f"  -> Results written to {summary_path}")
                processed += 1

        if processed == 0:
            raise BenchmarkError("Benchmark finished without generating any results.")

        return 0
    except BenchmarkError as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
