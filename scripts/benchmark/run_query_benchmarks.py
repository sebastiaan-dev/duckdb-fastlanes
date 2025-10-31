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
import shutil
import sys
import time
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence, Tuple
from uuid import UUID

import duckdb


SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent.parent
DEFAULT_METADATA = PROJECT_ROOT / "benchmark" / "datav2" / "metadata.duckdb"
DEFAULT_PROFILE_DIR = PROJECT_ROOT / "benchmark" / "results" / "profiles"

FASTLANES_EXTENSION = (
    PROJECT_ROOT
    / "build"
    / "release"
    / "extension"
    / "fastlanes"
    / "fastlanes.duckdb_extension"
)
VORTEX_EXTENSION = (
    PROJECT_ROOT
    / "build"
    / "release"
    / "extension"
    / "vortex"
    / "vortex.duckdb_extension"
)


@dataclass(frozen=True)
class FileEntry:
    id: UUID
    benchmark: str
    sf: Optional[str]
    rowgroup_size: Optional[int]
    table_name: str
    file_format: str
    path: Path


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
        help="Path to the metadata DuckDB database (default: benchmark/datav2/metadata.duckdb)",
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
        "--iterations",
        type=int,
        default=1,
        help="Number of measured runs per query (default: 1).",
    )
    parser.add_argument(
        "--limit",
        type=int,
        help="Optional maximum number of queries to execute.",
    )
    return parser.parse_args(argv)


def load_extension(conn: duckdb.DuckDBPyConnection, path: Path, name: str) -> None:
    if path.exists():
        conn.load_extension(str(path))
    else:
        conn.load_extension(name)


def ensure_extensions(conn: duckdb.DuckDBPyConnection, formats: Iterable[str]) -> None:
    lower_formats = {fmt.lower() for fmt in formats}
    if "fls" in lower_formats:
        load_extension(conn, FASTLANES_EXTENSION, "fastlanes")
    if "vortex" in lower_formats:
        load_extension(conn, VORTEX_EXTENSION, "vortex")


def escape_literal(value: str) -> str:
    return value.replace("'", "''")


def string_literal(path: Path) -> str:
    return f"'{escape_literal(str(path))}'"


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
        file_path = Path(row[6])
        if not file_path.is_absolute():
            file_path = (base_dir / file_path).resolve()
        result[row[0]] = FileEntry(
            id=row[0],
            benchmark=str(row[1]),
            sf=str(row[2]) if row[2] is not None else None,
            rowgroup_size=int(row[3]) if row[3] is not None else None,
            table_name=str(row[4]),
            file_format=str(row[5]).lower(),
            path=file_path,
        )
    return result


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


def copy_entry_to_target(entry: FileEntry, target_dir: Path) -> Tuple[Path, Path]:
    """
    Copy the given file entry to the target directory.

    Returns a tuple of (actual_data_path, cleanup_root).
    """
    cleanup_root = target_dir / entry.id.hex
    if cleanup_root.exists():
        shutil.rmtree(cleanup_root)
    cleanup_root.mkdir(parents=True, exist_ok=True)

    source = entry.path
    if source.is_dir():
        dest_path = cleanup_root / source.name
        shutil.copytree(source, dest_path)
        return dest_path, cleanup_root
    dest_path = cleanup_root / source.name
    shutil.copy2(source, dest_path)
    return dest_path, cleanup_root


def rewrite_query(
    sql: str, originals: Dict[UUID, FileEntry], replacements: Dict[UUID, Path]
) -> str:
    updated = sql
    for file_id, entry in originals.items():
        original_literal = string_literal(entry.path)
        replacement_literal = string_literal(replacements[file_id])
        updated = updated.replace(original_literal, replacement_literal)
    return updated


def attach_duckdb_tables(
    conn: duckdb.DuckDBPyConnection,
    files: Dict[UUID, FileEntry],
    paths: Dict[UUID, Path],
) -> List[Tuple[str, str]]:
    created_views: List[Tuple[str, str]] = []
    for file_id, entry in files.items():
        if entry.file_format != "duckdb":
            continue
        alias = f"db_{file_id.hex}"
        conn.execute(f"ATTACH '{escape_literal(str(paths[file_id]))}' AS {alias};")
        view_sql = (
            f"CREATE OR REPLACE VIEW {quote_identifier(entry.table_name)} AS "
            f"SELECT * FROM {alias}.{quote_identifier(entry.table_name)};"
        )
        conn.execute(view_sql)
        created_views.append((alias, entry.table_name))
    return created_views


def detach_duckdb_tables(
    conn: duckdb.DuckDBPyConnection, views: List[Tuple[str, str]]
) -> None:
    for alias, table_name in views:
        conn.execute(f"DROP VIEW IF EXISTS {quote_identifier(table_name)};")
        conn.execute(f"DETACH {alias};")


def quote_identifier(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'


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

        files = fetch_files(meta_conn, metadata_path)
        grouped = group_queries(queries, require_copy=target_dir is not None)

        total_runs = len(queries) * args.iterations
        success_count = 0
        failure_count = 0
        elapsed_samples: List[float] = []
        run_index = 0
        overall_start = time.perf_counter()

        for (file_id_tuple, use_copy), group in grouped.items():
            file_ids = list(file_id_tuple)
            file_entries = {fid: files[fid] for fid in file_ids}
            replacements: Dict[UUID, Path] = {}
            cleanup_paths: List[Path] = []
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
                        )
                        replacements[fid] = dest_path
                        cleanup_paths.append(cleanup_root)
                else:
                    for fid in file_ids:
                        replacements[fid] = file_entries[fid].path

                formats = {file_entries[fid].file_format for fid in file_ids}
                run_conn = duckdb.connect(config={"allow_unsigned_extensions": "true"})
                try:
                    ensure_extensions(run_conn, formats)
                    views = attach_duckdb_tables(run_conn, file_entries, replacements)
                    try:
                        for query in group:
                            for iteration in range(1, args.iterations + 1):
                                run_index += 1
                                sql = rewrite_query(
                                    query.sql, file_entries, replacements
                                )
                                configure_connection(run_conn, query)
                                summary = (
                                    f"[{run_index}/{total_runs}] Query {query.id.hex[:8]} "
                                    f"type={query.type} iter={iteration} threads={query.thread_count}"
                                )
                                print(f"{summary} - executing...", flush=True)
                                success, elapsed_ms, profile_path, error_message = execute_query_run(
                                    run_conn,
                                    meta_conn,
                                    query,
                                    sql,
                                    profile_dir,
                                    iteration,
                                )
                                if success:
                                    success_count += 1
                                    if elapsed_ms is not None:
                                        elapsed_samples.append(elapsed_ms)
                                    if elapsed_ms is not None:
                                        print(
                                            f"{summary} - completed in {elapsed_ms:.2f} ms "
                                            f"(profile: {profile_path})",
                                            flush=True,
                                        )
                                    else:
                                        print(
                                            f"{summary} - completed (no timing recorded)",
                                            flush=True,
                                        )
                                else:
                                    failure_count += 1
                                    print(
                                        f"{summary} - FAILED: {error_message}",
                                        file=sys.stderr,
                                        flush=True,
                                    )
                    finally:
                        detach_duckdb_tables(run_conn, views)
                finally:
                    run_conn.close()
            finally:
                for cleanup_root in cleanup_paths:
                    if cleanup_root.exists():
                        shutil.rmtree(cleanup_root, ignore_errors=True)

        total_elapsed = time.perf_counter() - overall_start
        print(f"Completed {run_index} benchmark runs in {total_elapsed:.2f} s.")
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
    if args.target_dir is None and args.ram_disk == "true":
        print(
            "Ram disk filter requires --target-dir to copy data files.",
            file=sys.stderr,
        )
        sys.exit(1)
    run_benchmarks(args)


if __name__ == "__main__":
    main()
