#!/usr/bin/env python3
"""
Iterate over COPY outputs in temp/copy_to_tests/... and record COPY latency metadata.

For every <benchmark>/sf<sf>/rg<rowgroup>/<table>.<ext> entry, the script:
  * loads the corresponding dataset table from metadata.duckdb
  * re-executes COPY ... TO for the requested target format/path
  * measures end-to-end latency and verifies the output
  * inserts a row into metadata.duckdb copy_results capturing timing + metadata
"""

from __future__ import annotations

import argparse
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence, Tuple
from uuid import UUID, uuid4

import duckdb

from scripts.common.duckdb_utils import (
    ensure_extensions,
    quote_identifier,
    sql_literal,
)
from scripts.common.paths import DATA_ROOT, PROJECT_ROOT

DEFAULT_METADATA = DATA_ROOT / "metadata.duckdb"
DEFAULT_BASE_DIR = PROJECT_ROOT / "temp" / "copy_to_tests"
DEFAULT_THREADS = (4,)

TARGET_SUFFIXES = {
    "parquet": ".parquet",
    "fls": ".fls",
    "vortex": ".vortex",
    "duckdb": ".duckdb",
}
SUFFIX_TO_FORMAT = {suffix: fmt for fmt, suffix in TARGET_SUFFIXES.items()}
SOURCE_FORMAT_DEFAULTS = ("duckdb", "fls", "vortex", "parquet", "csv")


@dataclass(frozen=True)
class FileEntry:
    id: UUID
    benchmark: str
    sf: Optional[str]
    rowgroup_size: Optional[int]
    table_name: str
    file_format: str
    path: Path


@dataclass
class CopyTask:
    benchmark: str
    scale_factor: Optional[str]
    row_group_size: Optional[int]
    table_name: str
    target_format: str
    target_path: Path


@dataclass
class CopyResult:
    task: CopyTask
    target_path: Path
    iteration: int
    execution_time_ms: Optional[float]
    success: bool
    error_message: Optional[str]
    row_count: Optional[int]
    file_size_bytes: Optional[int]
    threads: int


def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Record COPY TO latencies for existing copy_to_tests outputs.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--metadata",
        type=Path,
        default=DEFAULT_METADATA,
        help="Path to benchmark metadata.duckdb catalog.",
    )
    parser.add_argument(
        "--base-dir",
        type=Path,
        default=DEFAULT_BASE_DIR,
        help="Root directory containing benchmark/sf/rg/table.ext outputs.",
    )
    parser.add_argument(
        "--benchmarks",
        nargs="+",
        help="Optional subset of benchmark categories to include.",
    )
    parser.add_argument(
        "--scale-factors",
        nargs="+",
        help="Optional subset of scale factors to include (match strings after 'sf').",
    )
    parser.add_argument(
        "--row-group-size",
        type=int,
        dest="dataset_rowgroup_size",
        help="Only include entries with this row group size (based on rg<value> directory).",
    )
    parser.add_argument(
        "--tables",
        nargs="+",
        help="Optional subset of table names to include.",
    )
    parser.add_argument(
        "--formats",
        nargs="+",
        help="Optional subset of target formats to include (parquet/fls/vortex/duckdb).",
    )
    parser.add_argument(
        "--source-formats",
        nargs="+",
        default=list(SOURCE_FORMAT_DEFAULTS),
        help="Preference order for source dataset formats in metadata.",
    )
    parser.add_argument(
        "--threads",
        type=int,
        nargs="+",
        default=list(DEFAULT_THREADS),
        help="One or more thread counts set on the DuckDB connection.",
    )
    parser.add_argument(
        "--object-cache",
        action="store_true",
        help="Enable DuckDB object cache before running COPY.",
    )
    parser.add_argument(
        "--iterations",
        type=int,
        default=1,
        help="Number of times to execute COPY for each target.",
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Overwrite existing target files instead of failing when they exist.",
    )
    parser.add_argument(
        "--verify",
        action="store_true",
        help="Read back the output and verify row counts after COPY.",
    )
    args = parser.parse_args(argv)

    if args.dataset_rowgroup_size is not None and args.dataset_rowgroup_size <= 0:
        parser.error("--row-group-size must be a positive integer")
    if args.iterations <= 0:
        parser.error("--iterations must be a positive integer")
    if any(value <= 0 for value in args.threads):
        parser.error("--threads values must be positive integers")
    if not args.source_formats:
        parser.error("At least one source format must be provided via --source-formats")
    return args


def configure_connection(
    conn: duckdb.DuckDBPyConnection, threads: int, object_cache: bool
) -> None:
    conn.execute("SET enable_profiling = 'json';")
    conn.execute("SET profiling_mode = 'detailed';")
    conn.execute(f"SET threads = {threads};")
    if object_cache:
        conn.execute("PRAGMA enable_object_cache;")
    else:
        conn.execute("PRAGMA disable_object_cache;")


def ensure_copy_results_table(conn: duckdb.DuckDBPyConnection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS copy_results (
            id UUID DEFAULT uuid(),
            benchmark TEXT NOT NULL,
            scale_factor TEXT,
            row_group_size INTEGER,
            table_name TEXT NOT NULL,
            target_format TEXT NOT NULL,
            target_path TEXT NOT NULL,
            iteration INTEGER NOT NULL,
            execution_time_ms DOUBLE,
            success BOOLEAN NOT NULL,
            error_message TEXT,
            row_count BIGINT,
            file_size_bytes BIGINT,
            threads INTEGER NOT NULL,
            object_cache BOOLEAN NOT NULL,
            created_at TIMESTAMP DEFAULT now()
        );
        """
    )


def fetch_files(
    meta_conn: duckdb.DuckDBPyConnection, metadata_path: Path
) -> Dict[UUID, FileEntry]:
    rows = meta_conn.execute(
        """
        SELECT id, benchmark, sf, rowgroup_size, table_name, file_format, path
        FROM files_uuid
        """
    ).fetchall()
    base_dir = metadata_path.parent.resolve()
    result: Dict[UUID, FileEntry] = {}
    for row in rows:
        file_path = Path(row[6])
        if not file_path.is_absolute():
            file_path = (base_dir / file_path).resolve()
        result[row[0]] = FileEntry(
            id=row[0],
            benchmark=str(row[1]),
            sf=str(row[2]) if row[2] is not None else None,
            rowgroup_size=int(row[3]) if row[3] is not None else None,
            table_name=str(row[4]),
            file_format=str(row[5]),
            path=file_path,
        )
    return result


def parse_scale_factor(part: str) -> Optional[str]:
    if not part.startswith("sf"):
        raise ValueError(f"Invalid scale factor directory '{part}'")
    raw = part[2:]
    if raw.lower() in ("na", "none", ""):
        return None
    return raw


def parse_row_group(part: str) -> Optional[int]:
    if not part.startswith("rg"):
        raise ValueError(f"Invalid row group directory '{part}'")
    raw = part[2:]
    if raw.lower() in ("na", "none", ""):
        return None
    return int(raw)


def discover_tasks(base_dir: Path) -> List[CopyTask]:
    tasks: List[CopyTask] = []
    if not base_dir.exists():
        return tasks
    for benchmark_dir in sorted(p for p in base_dir.iterdir() if p.is_dir()):
        benchmark = benchmark_dir.name
        for sf_dir in sorted(p for p in benchmark_dir.iterdir() if p.is_dir()):
            try:
                scale_factor = parse_scale_factor(sf_dir.name)
            except ValueError:
                continue
            for rg_dir in sorted(p for p in sf_dir.iterdir() if p.is_dir()):
                try:
                    row_group_size = parse_row_group(rg_dir.name)
                except ValueError:
                    continue
                for file_path in sorted(p for p in rg_dir.iterdir() if p.is_file()):
                    format_name = SUFFIX_TO_FORMAT.get(file_path.suffix.lower())
                    if format_name is None:
                        continue
                    tasks.append(
                        CopyTask(
                            benchmark=benchmark,
                            scale_factor=scale_factor,
                            row_group_size=row_group_size,
                            table_name=file_path.stem,
                            target_format=format_name,
                            target_path=file_path,
                        )
                    )
    return tasks


def filter_tasks(tasks: List[CopyTask], args: argparse.Namespace) -> List[CopyTask]:
    benchmark_filter = (
        {val.lower() for val in args.benchmarks} if args.benchmarks else None
    )
    scale_filter = set(args.scale_factors) if args.scale_factors else None
    format_filter = {val.lower() for val in args.formats} if args.formats else None
    table_filter = {val.lower() for val in args.tables} if args.tables else None
    filtered: List[CopyTask] = []
    for task in tasks:
        if benchmark_filter and task.benchmark.lower() not in benchmark_filter:
            continue
        if scale_filter and task.scale_factor not in scale_filter:
            continue
        if (
            args.dataset_rowgroup_size is not None
            and task.row_group_size != args.dataset_rowgroup_size
        ):
            continue
        if format_filter and task.target_format.lower() not in format_filter:
            continue
        if table_filter and task.table_name.lower() not in table_filter:
            continue
        filtered.append(task)
    return filtered


def select_source_entries(
    files: Dict[UUID, FileEntry],
    tasks: List[CopyTask],
    source_formats: Sequence[str],
) -> Dict[Tuple[str, Optional[str], Optional[int], str], FileEntry]:
    needed = {
        (
            task.benchmark.lower(),
            task.scale_factor,
            task.row_group_size,
            task.table_name.lower(),
        )
        for task in tasks
    }
    priority = {fmt.lower(): idx for idx, fmt in enumerate(source_formats)}
    fallback_priority = len(priority) + 1
    selected: Dict[Tuple[str, Optional[str], Optional[int], str], FileEntry] = {}
    for entry in files.values():
        fmt = entry.file_format.lower()
        if fmt not in priority:
            continue
        key = (
            entry.benchmark.lower(),
            entry.sf,
            entry.rowgroup_size,
            entry.table_name.lower(),
        )
        if key not in needed:
            continue
        existing = selected.get(key)
        if existing is None:
            selected[key] = entry
            continue
        if priority.get(fmt, fallback_priority) < priority.get(
            existing.file_format.lower(), fallback_priority
        ):
            selected[key] = entry
    missing = needed - set(selected.keys())
    if missing:
        missing_descriptions = [
            f"{benchmark}/sf{sf or 'na'}/rg{rg or 'na'}/{table}"
            for (benchmark, sf, rg, table) in sorted(missing)
        ]
        raise RuntimeError(
            "Missing dataset entries for: " + ", ".join(missing_descriptions)
        )
    return selected


def register_dataset_tables(
    conn: duckdb.DuckDBPyConnection,
    entries: Iterable[FileEntry],
) -> Tuple[List[str], List[str]]:
    attachments: List[str] = []
    created_views: List[str] = []
    attached_aliases: Dict[Path, str] = {}
    for entry in entries:
        fmt = entry.file_format.lower()
        table_identifier = quote_identifier(entry.table_name)
        path = entry.path.resolve()
        literal_path = sql_literal(path)
        if fmt == "duckdb":
            alias = attached_aliases.get(path)
            if alias is None:
                alias = f"db_{entry.id.hex}"
                conn.execute(f"ATTACH {literal_path} AS {alias};")
                attachments.append(alias)
                attached_aliases[path] = alias
            conn.execute(
                f"CREATE OR REPLACE VIEW {table_identifier} AS "
                f"SELECT * FROM {alias}.{table_identifier};"
            )
        elif fmt == "parquet":
            conn.execute(
                f"CREATE OR REPLACE VIEW {table_identifier} AS "
                f"SELECT * FROM read_parquet({literal_path});"
            )
        elif fmt == "fls":
            conn.execute(
                f"CREATE OR REPLACE VIEW {table_identifier} AS "
                f"SELECT * FROM read_fls({literal_path});"
            )
        elif fmt == "vortex":
            conn.execute(
                f"CREATE OR REPLACE VIEW {table_identifier} AS "
                f"SELECT * FROM read_vortex({literal_path});"
            )
        elif fmt == "csv":
            conn.execute(
                f"CREATE OR REPLACE VIEW {table_identifier} AS "
                f"SELECT * FROM read_csv_auto({literal_path});"
            )
        else:
            raise RuntimeError(
                f"Unsupported source format '{entry.file_format}' for table '{entry.table_name}'."
            )
        created_views.append(entry.table_name)
    return attachments, created_views


def unregister_dataset_tables(
    conn: duckdb.DuckDBPyConnection,
    attachments: Sequence[str],
    views: Sequence[str],
) -> None:
    for table_name in views:
        conn.execute(f"DROP VIEW IF EXISTS {quote_identifier(table_name)};")
    for alias in attachments:
        try:
            conn.execute(f"DETACH {alias};")
        except Exception:
            continue


def build_copy_options(format_name: str) -> str:
    return f"FORMAT {format_name}"


def remove_existing_target(path: Path) -> None:
    if path.is_dir():
        for child in path.iterdir():
            remove_existing_target(child)
        path.rmdir()
    elif path.exists():
        path.unlink()


def write_table_to_duckdb(
    conn: duckdb.DuckDBPyConnection,
    table_name: str,
    target_path: Path,
) -> None:
    alias = f"target_{uuid4().hex}"
    literal_path = sql_literal(target_path)
    conn.execute(f"ATTACH {literal_path} AS {alias};")
    source_identifier = quote_identifier(table_name)
    target_identifier = f"{alias}.{quote_identifier(table_name)}"
    try:
        conn.execute(f"DROP TABLE IF EXISTS {target_identifier};")
        conn.execute(
            f"CREATE TABLE {target_identifier} AS SELECT * FROM {source_identifier};"
        )
    finally:
        conn.execute(f"DETACH {alias};")


def verify_output(
    conn: duckdb.DuckDBPyConnection,
    table_name: str,
    format_name: str,
    target_path: Path,
) -> int:
    literal_path = sql_literal(target_path)
    if format_name == "parquet":
        query = f"SELECT COUNT(*) FROM read_parquet({literal_path});"
        return conn.execute(query).fetchone()[0]
    if format_name == "fls":
        query = f"SELECT COUNT(*) FROM read_fls({literal_path});"
        return conn.execute(query).fetchone()[0]
    if format_name == "vortex":
        query = f"SELECT COUNT(*) FROM read_vortex({literal_path});"
        return conn.execute(query).fetchone()[0]
    if format_name == "duckdb":
        alias = f"verify_{uuid4().hex}"
        conn.execute(f"ATTACH {literal_path} AS {alias};")
        try:
            query = f"SELECT COUNT(*) FROM {alias}.{quote_identifier(table_name)};"
            return conn.execute(query).fetchone()[0]
        finally:
            conn.execute(f"DETACH {alias};")
    raise RuntimeError(f"Unsupported verification format '{format_name}'.")


def execute_copy(
    conn: duckdb.DuckDBPyConnection,
    task: CopyTask,
    iteration: int,
    overwrite: bool,
    verify: bool,
    expected_rows: int,
    thread_count: int,
) -> CopyResult:
    target_suffix = TARGET_SUFFIXES[task.target_format]
    target_path = task.target_path
    if target_path.suffix.lower() != target_suffix:
        target_path = target_path.with_suffix(target_suffix)

    target_path.parent.mkdir(parents=True, exist_ok=True)

    if target_path.exists():
        if overwrite:
            remove_existing_target(target_path)
        else:
            return CopyResult(
                task=task,
                target_path=target_path,
                iteration=iteration,
                execution_time_ms=None,
                success=False,
                error_message="Target exists (rerun with --overwrite to replace).",
                row_count=None,
                file_size_bytes=None,
            )

    start = time.perf_counter()
    success = True
    error_message: Optional[str] = None
    try:
        if task.target_format == "duckdb":
            write_table_to_duckdb(conn, task.table_name, target_path)
        else:
            table_identifier = quote_identifier(task.table_name)
            copy_sql = (
                f"COPY (SELECT * FROM {table_identifier}) TO "
                f"{sql_literal(target_path)} ({build_copy_options(task.target_format)});"
            )
            conn.execute(copy_sql)
    except Exception as exc:  # pragma: no cover - script level logging only
        success = False
        error_message = str(exc)
    elapsed_ms = (time.perf_counter() - start) * 1000.0

    row_count: Optional[int] = None
    file_size: Optional[int] = None
    if success:
        file_size = target_path.stat().st_size if target_path.exists() else None
        if verify:
            try:
                row_count = verify_output(
                    conn, task.table_name, task.target_format, target_path
                )
            except Exception as exc:  # pragma: no cover - script level logging only
                success = False
                error_message = f"Verification failed: {exc}"
        else:
            row_count = expected_rows
    return CopyResult(
        task=task,
        target_path=target_path,
        iteration=iteration,
        execution_time_ms=elapsed_ms,
        success=success,
        error_message=error_message,
        row_count=row_count,
        file_size_bytes=file_size,
        threads=thread_count,
    )


def insert_results(
    meta_conn: duckdb.DuckDBPyConnection,
    results: List[CopyResult],
    object_cache: bool,
) -> None:
    if not results:
        return
    meta_conn.executemany(
        """
        INSERT INTO copy_results (
            benchmark, scale_factor, row_group_size, table_name,
            target_format, target_path, iteration,
            execution_time_ms, success, error_message,
            row_count, file_size_bytes, threads, object_cache
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            [
                result.task.benchmark,
                result.task.scale_factor,
                result.task.row_group_size,
                result.task.table_name,
                result.task.target_format,
                str(result.target_path),
                result.iteration,
                result.execution_time_ms,
                result.success,
                result.error_message,
                result.row_count,
                result.file_size_bytes,
                result.threads,
                object_cache,
            ]
            for result in results
        ],
    )


def describe_task(task: CopyTask) -> str:
    return (
        f"{task.benchmark}/sf{task.scale_factor or 'na'}/rg{task.row_group_size or 'na'}"
        f"/{task.table_name}.{task.target_format}"
    )


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = parse_args(argv)
    metadata_path = args.metadata.resolve()
    base_dir = args.base_dir.resolve()
    tasks = discover_tasks(base_dir)
    tasks = filter_tasks(tasks, args)
    if not tasks:
        print("No COPY outputs matched the requested filters.", file=sys.stderr)
        return 1

    print("Planned COPY targets:")
    for task in tasks:
        print(f"  - {describe_task(task)} @ {task.target_path}")

    meta_conn = duckdb.connect(
        str(metadata_path), config={"allow_unsigned_extensions": "true"}
    )
    ensure_copy_results_table(meta_conn)
    files = fetch_files(meta_conn, metadata_path)

    selected_entries = select_source_entries(files, tasks, args.source_formats)
    unique_entries_map: Dict[UUID, FileEntry] = {}
    for entry in selected_entries.values():
        if entry.id not in unique_entries_map:
            unique_entries_map[entry.id] = entry
    unique_entries = list(unique_entries_map.values())

    run_conn = duckdb.connect(
        database=":memory:", config={"allow_unsigned_extensions": "true"}
    )
    formats_to_load = {task.target_format.lower() for task in tasks}
    formats_to_load.update(entry.file_format.lower() for entry in unique_entries)
    ensure_extensions(run_conn, formats_to_load)
    configure_connection(run_conn, args.threads[0], args.object_cache)

    attachments, views = register_dataset_tables(run_conn, unique_entries)
    expected_rows_map: Dict[str, int] = {}
    for entry in unique_entries:
        table_identifier = quote_identifier(entry.table_name)
        expected_rows_map[entry.table_name] = run_conn.execute(
            f"SELECT COUNT(*) FROM {table_identifier};"
        ).fetchone()[0]

    all_results: List[CopyResult] = []
    try:
        for thread_count in args.threads:
            configure_connection(run_conn, thread_count, args.object_cache)
            for task in tasks:
                expected_rows = expected_rows_map[task.table_name]
                for iteration in range(1, args.iterations + 1):
                    result = execute_copy(
                        run_conn,
                        task,
                        iteration,
                        args.overwrite,
                        args.verify,
                        expected_rows,
                        thread_count,
                    )
                    latency_text = (
                        f"{result.execution_time_ms:.2f} ms"
                        if result.execution_time_ms is not None
                        else "n/a"
                    )
                    status = "OK" if result.success else "FAIL"
                    print(
                        f"[{status}] threads={thread_count:<3} "
                        f"{describe_task(task)} iter={iteration} time={latency_text}",
                        flush=True,
                    )
                    all_results.append(result)
        insert_results(meta_conn, all_results, args.object_cache)
    finally:
        unregister_dataset_tables(run_conn, attachments, views)
        run_conn.close()
        meta_conn.close()

    failures = [result for result in all_results if not result.success]
    if failures:
        print(f"{len(failures)} COPY iterations failed; see copy_results for details.")
        return 1
    print("All COPY iterations completed successfully.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
