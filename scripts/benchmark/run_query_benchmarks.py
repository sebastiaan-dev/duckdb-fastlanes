#!/usr/bin/env python3
"""
Execute benchmark queries stored in metadata.duckdb and record results.

The script:
  * ensures a `results` table exists alongside `files_uuid`/`queries`
  * filters queries by configuration (threads/object cache/external cache/ram disk)
  * groups benchmarks by their required data files, copying them to a target
    directory (e.g. RAM disk) when requested
  * runs each query for the requested number of iterations while collecting
    JSON profiling output and writing per-run timing entries into `results`
"""

from __future__ import annotations

import argparse
import math
import os
import shutil
import sys
import time
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence, Set, Tuple
from uuid import UUID

import duckdb

from scripts.common.benchmark_utils import FileEntry, parse_file_entry
from scripts.common.duckdb_utils import (
    ensure_extensions,
    escape_literal,
    quote_identifier,
    sql_literal,
)
from scripts.common.format_utils import view_statement
from scripts.common.paths import DATA_ROOT, RESULTS_ROOT


DEFAULT_METADATA = DATA_ROOT / "metadata.duckdb"
DEFAULT_PROFILE_DIR = RESULTS_ROOT / "profiles"


@dataclass(frozen=True)
class QuerySpec:
    id: UUID
    type: str
    sql: str
    file_ids: Tuple[UUID, ...]
    ram_disk: bool
    thread_count: int
    object_cache: bool
    external_file_cache: bool


def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run benchmark queries defined in metadata.duckdb and record results."
    )
    parser.add_argument(
        "--metadata",
        type=Path,
        default=DEFAULT_METADATA,
        help="Path to the metadata DuckDB database (default: benchmark/data/metadata.duckdb)",
    )
    parser.add_argument(
        "--profile-dir",
        type=Path,
        default=DEFAULT_PROFILE_DIR,
        help="Directory where JSON profiles will be written (default: benchmark/results/profiles)",
    )
    parser.add_argument(
        "--target-dir",
        type=Path,
        help="Optional directory to copy source data files into before running (e.g. a RAM disk).",
    )
    parser.add_argument(
        "--threads",
        type=int,
        nargs="*",
        help="Optional list of thread counts to include (defaults to all).",
    )
    parser.add_argument(
        "--min-threads",
        type=int,
        help="Only run queries with thread_count >= this value.",
    )
    parser.add_argument(
        "--max-threads",
        type=int,
        help="Only run queries with thread_count <= this value.",
    )
    parser.add_argument(
        "--ram-disk",
        choices=("any", "true", "false"),
        default="any",
        help="Filter by ram_disk requirement (default: any).",
    )
    parser.add_argument(
        "--object-cache",
        choices=("any", "true", "false"),
        default="any",
        help="Filter by object_cache setting (default: any).",
    )
    parser.add_argument(
        "--external-file-cache",
        choices=("any", "true", "false"),
        default="any",
        help="Filter by external_file_cache setting (default: any).",
    )
    parser.add_argument(
        "--types",
        nargs="*",
        help="Optional subset of query types to include (defaults to all types).",
    )
    parser.add_argument(
        "--benchmarks",
        nargs="+",
        choices=("tpch", "volumetric"),
        help="Benchmark categories to include (defaults to all).",
    )
    parser.add_argument(
        "--iterations",
        type=int,
        default=1,
        help="Fixed number of iterations per query (ignored when --mean-relative-error is provided).",
    )
    parser.add_argument(
        "--min-iterations",
        type=int,
        default=1,
        help="Minimum number of iterations before convergence can be evaluated (only used with --mean-relative-error).",
    )
    parser.add_argument(
        "--max-iterations",
        type=int,
        help="Maximum number of iterations when using --mean-relative-error (defaults to value of --iterations).",
    )
    parser.add_argument(
        "--connection-mode",
        choices=("group", "per-execution"),
        default="group",
        help=(
            "Reuse a single DuckDB connection per dataset group (group) or create a new "
            "connection for every query execution (per-execution)."
        ),
    )
    parser.add_argument(
        "--mean-relative-error",
        type=float,
        help="Enable mean-based stopping when the relative standard error (stddev/sqrt(n))/mean "
        "falls below this threshold. Requires at least two successful runs.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        help="Optional maximum number of queries to execute.",
    )
    return parser.parse_args(argv)


def ensure_results_table(conn: duckdb.DuckDBPyConnection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS results (
            id UUID DEFAULT uuid(),
            query_id UUID NOT NULL,
            run_id INTEGER NOT NULL,
            execution_time_ms DOUBLE,
            profile_path TEXT,
            success BOOLEAN NOT NULL,
            error_message TEXT,
            ram_disk BOOLEAN NOT NULL,
            thread_count INTEGER NOT NULL,
            object_cache BOOLEAN NOT NULL,
            external_file_cache BOOLEAN NOT NULL,
            created_at TIMESTAMP DEFAULT now()
        );
        """
    )


def fetch_files(
    conn: duckdb.DuckDBPyConnection, metadata_path: Path
) -> Dict[UUID, FileEntry]:
    rows = conn.execute(
        """
        SELECT id, benchmark, sf, rowgroup_size, table_name, file_format, path
        FROM files_uuid
        """
    ).fetchall()
    base_dir = metadata_path.parent.resolve()
    result: Dict[UUID, FileEntry] = {}
    for row in rows:
        entry = parse_file_entry(row, base_dir)
        result[entry.id] = entry
    return result


def compute_entry_size(path: Path) -> int:
    if path.is_dir():
        total = 0
        for root, _, files in os.walk(path):
            for name in files:
                file_path = Path(root) / name
                try:
                    total += file_path.stat().st_size
                except FileNotFoundError:
                    continue
        return total
    return path.stat().st_size


def format_bytes(num_bytes: int) -> str:
    if num_bytes <= 0:
        return "0 B"
    suffixes = ["B", "KiB", "MiB", "GiB", "TiB"]
    value = float(num_bytes)
    for suffix in suffixes:
        if value < 1024.0 or suffix == suffixes[-1]:
            return f"{value:.2f} {suffix}"
        value /= 1024.0


def bool_filter_matches(value: bool, option: str) -> bool:
    if option == "any":
        return True
    if option == "true":
        return bool(value)
    return not value


def load_queries(
    conn: duckdb.DuckDBPyConnection, args: argparse.Namespace
) -> List[QuerySpec]:
    rows = conn.execute(
        """
        SELECT id, type, sql, file_ids, ram_disk, thread_count, object_cache, external_file_cache
        FROM queries
        ORDER BY id
        """
    ).fetchall()

    selected: List[QuerySpec] = []
    allowed_types = {t.lower() for t in (args.types or [])}
    if args.benchmarks:
        allowed_types.update(t.lower() for t in args.benchmarks)
    allowed_threads = set(args.threads) if args.threads else None
    for row in rows:
        file_ids = tuple(UUID(str(val)) for val in row[3])
        ram_disk = bool(row[4])
        thread_count = int(row[5])
        object_cache = bool(row[6])
        external_file_cache = bool(row[7])
        qtype = str(row[1]).lower()

        if allowed_types and qtype not in allowed_types:
            continue
        if allowed_threads is not None and thread_count not in allowed_threads:
            continue
        if args.min_threads is not None and thread_count < args.min_threads:
            continue
        if args.max_threads is not None and thread_count > args.max_threads:
            continue
        if not bool_filter_matches(ram_disk, args.ram_disk):
            continue
        if not bool_filter_matches(object_cache, args.object_cache):
            continue
        if not bool_filter_matches(external_file_cache, args.external_file_cache):
            continue

        selected.append(
            QuerySpec(
                id=row[0],
                type=row[1],
                sql=row[2],
                file_ids=file_ids,
                ram_disk=ram_disk,
                thread_count=thread_count,
                object_cache=object_cache,
                external_file_cache=external_file_cache,
            )
        )
    if args.limit is not None:
        selected = selected[: args.limit]
    return selected


def copy_entry_to_target(
    entry: FileEntry,
    target_dir: Path,
    cache: Dict[Path, Tuple[Path, Path]],
) -> Tuple[Path, Path]:
    """Copy an entry into the target directory, reusing duplicates by path."""

    source = entry.path.resolve()
    if source in cache:
        return cache[source]

    cleanup_root = target_dir / entry.id.hex
    if cleanup_root.exists():
        shutil.rmtree(cleanup_root)
    cleanup_root.mkdir(parents=True, exist_ok=True)

    if source.is_dir():
        dest_path = cleanup_root / source.name
        shutil.copytree(source, dest_path)
    else:
        dest_path = cleanup_root / source.name
        shutil.copy2(source, dest_path)

    cache[source] = (dest_path, cleanup_root)
    return dest_path, cleanup_root


def rewrite_query(
    sql: str, originals: Dict[UUID, FileEntry], replacements: Dict[UUID, Path]
) -> str:
    updated = sql
    for file_id, entry in originals.items():
        original_literal = sql_literal(entry.path)
        replacement_literal = sql_literal(replacements[file_id])
        updated = updated.replace(original_literal, replacement_literal)
    return updated


def register_dataset_tables(
    conn: duckdb.DuckDBPyConnection,
    files: Dict[UUID, FileEntry],
    paths: Dict[UUID, Path],
) -> Tuple[List[str], List[str]]:
    attachments: List[str] = []
    created_views: List[str] = []
    attached_aliases: Dict[Path, str] = {}
    for file_id, entry in files.items():
        fmt = entry.file_format.lower()
        table_name = entry.table_name
        path = paths[file_id].resolve()
        alias = None
        if fmt == "duckdb":
            alias = attached_aliases.get(path)
            if alias is None:
                alias = f"db_{file_id.hex}"
                conn.execute(f"ATTACH '{escape_literal(str(path))}' AS {alias};")
                attachments.append(alias)
                attached_aliases[path] = alias
        conn.execute(view_statement(table_name, fmt, path, alias))
        created_views.append(table_name)
    return attachments, created_views


def unregister_dataset_tables(
    conn: duckdb.DuckDBPyConnection,
    attachments: Sequence[str],
    views: Sequence[str],
) -> None:
    for table_name in views:
        conn.execute(f"DROP VIEW IF EXISTS {quote_identifier(table_name)};")
    for alias in attachments:
        conn.execute(f"DETACH {alias};")


def relative_standard_error(samples: Sequence[float]) -> float:
    if len(samples) < 2:
        return math.inf
    mean = sum(samples) / len(samples)
    if mean == 0:
        return math.inf
    variance = sum((value - mean) ** 2 for value in samples) / (len(samples) - 1)
    stddev = math.sqrt(variance)
    return (stddev / math.sqrt(len(samples))) / mean


def execute_query_run(
    run_conn: duckdb.DuckDBPyConnection,
    meta_conn: duckdb.DuckDBPyConnection,
    query: QuerySpec,
    sql: str,
    profile_dir: Path,
    iteration: int,
) -> Tuple[bool, Optional[float], Path, Optional[str]]:
    profile_path = profile_dir / query.id.hex / f"run_{iteration}.json"
    profile_path.parent.mkdir(parents=True, exist_ok=True)
    run_conn.execute(f"SET profiling_output = '{escape_literal(str(profile_path))}'")

    start = time.perf_counter()
    error_message: Optional[str] = None
    success = True
    try:
        run_conn.execute(sql).fetchall()
    except Exception as exc:
        success = False
        error_message = str(exc)
    elapsed_ms = (time.perf_counter() - start) * 1000.0

    meta_conn.execute(
        """
        INSERT INTO results (query_id, run_id, execution_time_ms, profile_path, success, error_message,
                             ram_disk, thread_count, object_cache, external_file_cache)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            query.id,
            iteration,
            elapsed_ms if success else None,
            str(profile_path) if success else None,
            success,
            error_message,
            query.ram_disk,
            query.thread_count,
            query.object_cache,
            query.external_file_cache,
        ],
    )
    return success, elapsed_ms if success else None, profile_path, error_message


def group_queries(
    queries: Sequence[QuerySpec],
    require_copy: bool,
) -> Dict[Tuple[Tuple[UUID, ...], bool], List[QuerySpec]]:
    grouped: Dict[Tuple[Tuple[UUID, ...], bool], List[QuerySpec]] = defaultdict(list)
    for query in queries:
        use_copy = require_copy and query.ram_disk
        key = (tuple(sorted(query.file_ids)), use_copy)
        grouped[key].append(query)
    return grouped


def configure_connection(run_conn: duckdb.DuckDBPyConnection, query: QuerySpec) -> None:
    run_conn.execute("SET enable_profiling = 'json';")
    run_conn.execute("SET profiling_mode = 'detailed';")
    run_conn.execute(f"SET threads = {query.thread_count};")
    if query.object_cache:
        run_conn.execute("PRAGMA enable_object_cache;")
    else:
        run_conn.execute("PRAGMA disable_object_cache;")
    run_conn.execute(
        f"SET enable_external_file_cache = {'TRUE' if query.external_file_cache else 'FALSE'};"
    )


def create_run_connection(
    formats: Iterable[str],
    file_entries: Dict[UUID, FileEntry],
    replacements: Dict[UUID, Path],
) -> Tuple[duckdb.DuckDBPyConnection, List[str], List[str]]:
    run_conn = duckdb.connect(config={"allow_unsigned_extensions": "true"})
    ensure_extensions(run_conn, formats)
    attachments, views = register_dataset_tables(run_conn, file_entries, replacements)
    return run_conn, attachments, views


def run_benchmarks(args: argparse.Namespace) -> None:
    metadata_path = args.metadata.resolve()
    if not metadata_path.exists():
        print(f"Metadata database '{metadata_path}' not found.", file=sys.stderr)
        sys.exit(1)

    target_dir = args.target_dir.resolve() if args.target_dir else None
    if target_dir:
        target_dir.mkdir(parents=True, exist_ok=True)
    profile_dir = args.profile_dir.resolve()
    profile_dir.mkdir(parents=True, exist_ok=True)

    meta_conn = duckdb.connect(str(metadata_path))
    try:
        ensure_results_table(meta_conn)
        queries = load_queries(meta_conn, args)
        if any(query.ram_disk for query in queries) and target_dir is None:
            raise RuntimeError(
                "Selected queries require copying to a RAM disk, but --target-dir was not provided."
            )
        if not queries:
            print("No queries matched the requested filters; nothing to do.")
            return

        if args.iterations <= 0:
            raise ValueError("--iterations must be a positive integer")
        if args.min_iterations <= 0:
            raise ValueError("--min-iterations must be a positive integer")

        dynamic_iterations = args.mean_relative_error is not None
        if dynamic_iterations:
            if args.mean_relative_error <= 0:
                raise ValueError("--mean-relative-error must be positive")
            min_iterations = max(args.min_iterations, 2)
            max_iterations = args.max_iterations or max(args.iterations, min_iterations)
        else:
            min_iterations = max(args.iterations, 1)
            max_iterations = min_iterations
        if max_iterations < min_iterations:
            raise ValueError(
                "--max-iterations must be greater than or equal to --min-iterations"
            )

        files = fetch_files(meta_conn, metadata_path)
        # entry_sizes = {
        #     fid: compute_entry_size(entry.path) for fid, entry in files.items()
        # }
        grouped = group_queries(queries, require_copy=target_dir is not None)

        estimated_total_runs = len(queries) * max_iterations
        success_count = 0
        failure_count = 0
        elapsed_samples: List[float] = []
        run_index = 0
        overall_start = time.perf_counter()

        for (file_id_tuple, use_copy), group in grouped.items():
            file_ids = list(file_id_tuple)
            file_entries = {fid: files[fid] for fid in file_ids}
            replacements: Dict[UUID, Path] = {}
            cleanup_paths: Set[Path] = set()
            copy_cache: Dict[Path, Tuple[Path, Path]] = {}
            try:
                if use_copy:
                    assert target_dir is not None
                    print(
                        f"Copying {len(file_ids)} file(s) to {target_dir} for RAM disk execution...",
                        flush=True,
                    )
                    for fid in file_ids:
                        dest_path, cleanup_root = copy_entry_to_target(
                            file_entries[fid],
                            target_dir,
                            copy_cache,
                        )
                        replacements[fid] = dest_path
                        cleanup_paths.add(cleanup_root)
                else:
                    for fid in file_ids:
                        replacements[fid] = file_entries[fid].path

                formats = {file_entries[fid].file_format for fid in file_ids}
                reuse_connection = args.connection_mode == "group"
                run_conn: Optional[duckdb.DuckDBPyConnection] = None
                persistent_attachments: List[str] = []
                persistent_views: List[str] = []
                if reuse_connection:
                    run_conn, persistent_attachments, persistent_views = (
                        create_run_connection(formats, file_entries, replacements)
                    )
                try:
                    for query in group:
                        iteration = 0
                        elapsed_history: List[float] = []
                        while True:
                            iteration += 1
                            run_index += 1
                            sql = rewrite_query(query.sql, file_entries, replacements)
                            if reuse_connection:
                                active_conn = run_conn
                                active_attachments = persistent_attachments
                                active_views = persistent_views
                                assert active_conn is not None
                            else:
                                active_conn, active_attachments, active_views = (
                                    create_run_connection(
                                        formats, file_entries, replacements
                                    )
                                )
                            try:
                                configure_connection(active_conn, query)
                                query_formats = sorted(
                                    {
                                        file_entries[fid].file_format
                                        for fid in query.file_ids
                                    }
                                )
                                format_label = (
                                    ",".join(query_formats)
                                    if query_formats
                                    else "unknown"
                                )
                                summary = (
                                    f"[{run_index}/{estimated_total_runs}] Query {query.id.hex[:8]} "
                                    f"type={query.type} iter={iteration}"
                                )
                                if dynamic_iterations:
                                    summary += f"/{max_iterations}"
                                summary += (
                                    f" threads={query.thread_count}"
                                    f" formats={format_label}"
                                )
                                print(f"{summary} - executing...", flush=True)
                                success, elapsed_ms, profile_path, error_message = (
                                    execute_query_run(
                                        active_conn,
                                        meta_conn,
                                        query,
                                        sql,
                                        profile_dir,
                                        iteration,
                                    )
                                )
                                if success:
                                    success_count += 1
                                    if elapsed_ms is not None:
                                        elapsed_samples.append(elapsed_ms)
                                        elapsed_history.append(elapsed_ms)
                                    if elapsed_ms is not None:
                                        print(
                                            f"completed in {elapsed_ms:.2f} ms ",
                                            flush=True,
                                        )
                                    else:
                                        print(
                                            "completed (no timing recorded)",
                                            flush=True,
                                        )
                                else:
                                    failure_count += 1
                                    print(
                                        f"FAILED: {error_message}",
                                        file=sys.stderr,
                                        flush=True,
                                    )
                                if dynamic_iterations:
                                    relative_error: Optional[float] = None
                                    if (
                                        len(elapsed_history) >= min_iterations
                                        and len(elapsed_history) >= 2
                                    ):
                                        relative_error = relative_standard_error(
                                            elapsed_history
                                        )
                                        if (
                                            math.isfinite(relative_error)
                                            and relative_error
                                            <= args.mean_relative_error
                                        ):
                                            print(
                                                f"    -> convergence reached after {iteration} iterations "
                                                f"(relative stderr {relative_error:.4f})",
                                                flush=True,
                                            )
                                            break
                                    if iteration >= max_iterations:
                                        if (
                                            relative_error is None
                                            and len(elapsed_history) >= 2
                                        ):
                                            relative_error = relative_standard_error(
                                                elapsed_history
                                            )
                                        if relative_error is not None and math.isfinite(
                                            relative_error
                                        ):
                                            print(
                                                f"    -> reached max iterations ({iteration}); "
                                                f"relative stderr {relative_error:.4f}",
                                                flush=True,
                                            )
                                        else:
                                            print(
                                                f"    -> reached max iterations ({iteration}); "
                                                f"insufficient successful runs for error estimate",
                                                flush=True,
                                            )
                                        break
                                else:
                                    if iteration >= max_iterations:
                                        break
                            finally:
                                if not reuse_connection:
                                    unregister_dataset_tables(
                                        active_conn, active_attachments, active_views
                                    )
                                    active_conn.close()
                finally:
                    if reuse_connection and run_conn is not None:
                        unregister_dataset_tables(
                            run_conn, persistent_attachments, persistent_views
                        )
                        run_conn.close()
            finally:
                for cleanup_root in cleanup_paths:
                    if cleanup_root.exists():
                        shutil.rmtree(cleanup_root, ignore_errors=True)

        total_elapsed = time.perf_counter() - overall_start
        print(
            f"Completed {run_index} benchmark runs "
            f"(estimated maximum {estimated_total_runs}) in {total_elapsed:.2f} s."
        )
        print(
            f"  Successful runs: {success_count}, Failed runs: {failure_count}",
            flush=True,
        )
        if elapsed_samples:
            avg_ms = sum(elapsed_samples) / len(elapsed_samples)
            min_ms = min(elapsed_samples)
            max_ms = max(elapsed_samples)
            print(
                f"  Timing (ms): avg={avg_ms:.2f}, min={min_ms:.2f}, max={max_ms:.2f}",
                flush=True,
            )
    finally:
        meta_conn.close()


def main(argv: Optional[Sequence[str]] = None) -> None:
    args = parse_args(argv)
    try:
        run_benchmarks(args)
    except (ValueError, RuntimeError) as exc:
        print(f"Error: {exc}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
