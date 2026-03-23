#!/usr/bin/env python3
"""
Generate benchmark query definitions from metadata.duckdb.

This script materialises a `files_uuid` table (wrapping entries from `files`
with a synthetic UUID) and populates a `queries` table with benchmark
definitions. For now only volumetric queries are generated; TPC-H query
generation will be added later but the schema already supports linking
multiple file UUIDs to a single query entry.
"""

from __future__ import annotations

import argparse
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Sequence, Tuple

import duckdb

from scripts.common.benchmark_utils import FileEntry, parse_file_entry
from scripts.common.duckdb_utils import ensure_extensions, escape_literal, quote_identifier
from scripts.common.format_utils import reader_for_format, table_expression
from scripts.common.paths import DATA_ROOT, PROJECT_ROOT

DEFAULT_METADATA = DATA_ROOT / "metadata.duckdb"
TPCH_QUERY_DIR = PROJECT_ROOT / "duckdb" / "extension" / "tpch" / "dbgen" / "queries"

VOLUMETRIC_BENCHMARK_SOURCES = {"volumetric", "tpch"}

TPCH_QUERY_TABLES: Dict[str, Tuple[str, ...]] = {
    "q01": ("lineitem",),
    "q02": ("part", "supplier", "partsupp", "nation", "region"),
    "q03": ("customer", "orders", "lineitem"),
    "q04": ("orders", "lineitem"),
    "q05": ("customer", "orders", "lineitem", "nation", "region", "supplier"),
    "q06": ("lineitem",),
    "q07": ("supplier", "lineitem", "orders", "customer", "nation"),
    "q08": ("part", "supplier", "lineitem", "orders", "customer", "nation", "region"),
    "q09": ("part", "partsupp", "supplier", "lineitem", "orders", "nation"),
    "q10": ("customer", "orders", "lineitem", "nation"),
    "q11": ("partsupp", "supplier", "nation"),
    "q12": ("orders", "lineitem"),
    "q13": ("customer", "orders"),
    "q14": ("lineitem", "part"),
    "q15": ("supplier", "lineitem"),
    "q16": ("part", "partsupp", "supplier"),
    "q17": ("lineitem", "part"),
    "q18": ("customer", "orders", "lineitem"),
    "q19": ("lineitem", "part"),
    "q20": ("supplier", "nation", "partsupp", "part", "lineitem"),
    "q21": ("supplier", "lineitem", "orders", "nation"),
    "q22": ("customer", "orders"),
}

# Normalised type categories so we can pick suitable aggregation templates
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


def load_tpch_sql_queries() -> Dict[str, str]:
    queries: Dict[str, str] = {}
    if not TPCH_QUERY_DIR.exists():
        return queries
    for path in sorted(TPCH_QUERY_DIR.glob("q*.sql")):
        name = path.stem.lower()
        queries[name] = path.read_text(encoding="utf-8").strip()
    return queries


@dataclass(frozen=True)
class QueryConfig:
    ram_disk: bool
    thread_count: int
    object_cache: bool
    external_file_cache: bool


def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Populate benchmark query definitions from metadata.duckdb"
    )
    parser.add_argument(
        "--metadata",
        type=Path,
        default=DEFAULT_METADATA,
        help="Path to the metadata DuckDB file (defaults to benchmark/data/metadata.duckdb)",
    )
    return parser.parse_args(argv)


def recreate_files_uuid(conn: duckdb.DuckDBPyConnection) -> None:
    conn.execute("DROP TABLE IF EXISTS files_uuid;")
    conn.execute("CREATE TABLE files_uuid AS SELECT uuid() AS id, * FROM files;")


def recreate_queries_table(conn: duckdb.DuckDBPyConnection) -> None:
    conn.execute("DROP TABLE IF EXISTS queries;")
    conn.execute(
        """
        CREATE TABLE queries (
            id UUID DEFAULT uuid(),
            type VARCHAR NOT NULL,
            sql TEXT NOT NULL,
            file_ids UUID[] NOT NULL,
            column_name VARCHAR,
            table_name VARCHAR,
            operator VARCHAR,
            tpch_query INTEGER,
            ram_disk BOOLEAN NOT NULL,
            thread_count INTEGER,
            object_cache BOOLEAN NOT NULL,
            external_file_cache BOOLEAN NOT NULL
        );
        """
    )


def get_files(conn: duckdb.DuckDBPyConnection, metadata_path: Path) -> List[FileEntry]:
    rows = conn.execute(
        """
        SELECT id, benchmark, sf, rowgroup_size, table_name, file_format, path
        FROM files_uuid
        ORDER BY benchmark, file_format, table_name
        """
    ).fetchall()
    entries: List[FileEntry] = []
    base_dir = metadata_path.parent.resolve()
    for row in rows:
        entries.append(parse_file_entry(row, base_dir))
    return entries


def build_query_configs() -> List[QueryConfig]:
    thread_values: List[int] = [1, 2, 4, 8]
    boolean_states = [False, True]

    configs: List[QueryConfig] = []
    for thread_count in thread_values:
        for ram_disk in boolean_states:
            for object_cache in boolean_states:
                for external_file_cache in boolean_states:
                    configs.append(
                        QueryConfig(
                            ram_disk=ram_disk,
                            thread_count=thread_count,
                            object_cache=object_cache,
                            external_file_cache=external_file_cache,
                        )
                    )
    return configs


def normalise_type(type_name: str) -> str:
    base = type_name.strip().upper()
    if not base:
        return base
    if "(" in base:
        base = base.split("(", 1)[0]
    if " " in base:
        base = base.split(" ", 1)[0]
    return base


def volumetric_templates(base_type: str) -> List[str]:
    if base_type in NUMERIC_BASE_TYPES:
        return [
            "SUM({col})",
            "AVG({col})",
            "MIN({col})",
            "MAX({col})",
            "COUNT({col})",
        ]
    if base_type in BOOLEAN_BASE_TYPES:
        return [
            "SUM(CASE WHEN {col} THEN 1 ELSE 0 END)",
            "COUNT({col})",
        ]
    if base_type in DATE_BASE_TYPES or base_type in DATETIME_BASE_TYPES:
        return [
            "MIN({col})",
            "MAX({col})",
            "COUNT({col})",
        ]
    if base_type in STRING_BASE_TYPES:
        return [
            "COUNT({col})",
            "COUNT(DISTINCT {col})",
        ]
    if base_type in BLOB_BASE_TYPES:
        return ["COUNT({col})"]
    return []


def aggregate_operator_name(aggregate: str) -> Optional[str]:
    text = aggregate.strip()
    if not text:
        return None
    upper = text.upper()
    if upper.startswith("COUNT(DISTINCT"):
        return "count_distinct"
    idx = text.find("(")
    if idx != -1:
        text = text[:idx]
    return text.strip().lower() or None


def column_definitions_for_file(
    file_entry: FileEntry,
    formats_requiring_extensions: Sequence[str],
) -> List[Dict[str, str]]:
    conn = duckdb.connect(config={"allow_unsigned_extensions": "true"})
    try:
        ensure_extensions(conn, formats_requiring_extensions)

        fmt = file_entry.file_format.lower()
        path_literal = escape_literal(str(file_entry.path))

        if fmt == "duckdb":
            alias = f"db_{file_entry.id.hex}"
            conn.execute(f"ATTACH '{path_literal}' AS {alias};")
            try:
                rows = conn.execute(
                    f"PRAGMA table_info({alias}.{quote_identifier(file_entry.table_name)});"
                ).fetchall()
            finally:
                conn.execute(f"DETACH {alias};")
            return [{"name": row[1], "type": row[2]} for row in rows]

        reader = reader_for_format(fmt)
        select_sql = f"SELECT * FROM {reader}('{path_literal}')"

        try:
            rows = conn.execute(f"DESCRIBE {select_sql}").fetchall()
            return [{"name": row[0], "type": row[1]} for row in rows]
        except duckdb.Error:
            view_name = f"tmp_view_{file_entry.id.hex}"
            try:
                conn.execute(
                    f"CREATE TEMP VIEW {quote_identifier(view_name)} AS {select_sql};"
                )
                rows = conn.execute(
                    f"PRAGMA table_info({quote_identifier(view_name)});"
                ).fetchall()
                return [{"name": row[1], "type": row[2]} for row in rows]
            except duckdb.Error as exc:
                print(
                    f"Warning: skipping schema extraction for '{file_entry.path}' "
                    f"(format {file_entry.file_format}): {exc}"
                )
                return []
            finally:
                conn.execute(f"DROP VIEW IF EXISTS {quote_identifier(view_name)};")
    finally:
        conn.close()


def generate_volumetric_queries(
    files: Sequence[FileEntry],
    configs: Sequence[QueryConfig],
    formats_requiring_extensions: Sequence[str],
) -> List[Dict[str, object]]:
    volumetric_queries: List[Dict[str, object]] = []
    for file_entry in files:
        table_expr = table_expression(file_entry)
        columns = column_definitions_for_file(file_entry, formats_requiring_extensions)
        if not columns:
            continue
        for column in columns:
            base_type = normalise_type(column["type"])
            aggregates = volumetric_templates(base_type)
            if not aggregates:
                continue
            column_ref = quote_identifier(column["name"])
            for aggregate in aggregates:
                sql = f"SELECT {aggregate.format(col=column_ref)} FROM {table_expr}"
                for config in configs:
                    volumetric_queries.append(
                        {
                            "type": "volumetric",
                            "sql": sql,
                            "file_ids": [file_entry.id],
                            "column_name": column["name"],
                            "table_name": file_entry.table_name,
                            "operator": aggregate_operator_name(aggregate),
                            "ram_disk": config.ram_disk,
                            "thread_count": config.thread_count,
                            "object_cache": config.object_cache,
                            "external_file_cache": config.external_file_cache,
                        }
                    )
    return volumetric_queries


def group_tpch_datasets(
    files: Sequence[FileEntry],
) -> Dict[Tuple[str, Optional[str], Optional[int]], Dict[str, FileEntry]]:
    grouped: Dict[Tuple[str, Optional[str], Optional[int]], Dict[str, FileEntry]] = {}
    for entry in files:
        if entry.benchmark.lower() != "tpch":
            continue
        key = (entry.file_format.lower(), entry.sf, entry.rowgroup_size)
        table_map = grouped.setdefault(key, {})
        table_map[entry.table_name.lower()] = entry
    return grouped


def generate_tpch_queries(
    files: Sequence[FileEntry],
    configs: Sequence[QueryConfig],
    tpch_sql: Dict[str, str],
) -> List[Dict[str, object]]:
    grouped = group_tpch_datasets(files)
    tpch_entries: List[Dict[str, object]] = []
    for key, table_map in grouped.items():
        for query_name, required_tables in TPCH_QUERY_TABLES.items():
            sql = tpch_sql.get(query_name)
            if not sql:
                continue
            missing = [table for table in required_tables if table not in table_map]
            if missing:
                continue
            file_ids = [table_map[table].id for table in required_tables]
            for config in configs:
                try:
                    query_number = int(query_name.lstrip("q"))
                except ValueError:
                    query_number = None
                tpch_entries.append(
                    {
                        "type": "tpch",
                        "sql": sql,
                        "file_ids": file_ids,
                        "tpch_query": query_number,
                        "ram_disk": config.ram_disk,
                        "thread_count": config.thread_count,
                        "object_cache": config.object_cache,
                        "external_file_cache": config.external_file_cache,
                    }
                )
    return tpch_entries


def insert_queries(
    conn: duckdb.DuckDBPyConnection, query_rows: Sequence[Dict[str, object]]
) -> None:
    if not query_rows:
        return
    insert_sql = """
        INSERT INTO queries (
            type,
            sql,
            file_ids,
            column_name,
            table_name,
            operator,
            tpch_query,
            ram_disk,
            thread_count,
            object_cache,
            external_file_cache
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """
    data = [
        (
            row["type"],
            row["sql"],
            [str(file_id) for file_id in row["file_ids"]],
            row.get("column_name"),
            row.get("table_name"),
            row.get("operator"),
            row.get("tpch_query"),
            row["ram_disk"],
            row["thread_count"],
            row["object_cache"],
            row["external_file_cache"],
        )
        for row in query_rows
    ]
    conn.executemany(insert_sql, data)


def main(argv: Optional[Sequence[str]] = None) -> None:
    args = parse_args(argv)
    metadata_path = args.metadata.resolve()
    if not metadata_path.exists():
        print(f"Metadata file '{metadata_path}' not found.", file=sys.stderr)
        sys.exit(1)

    configs = build_query_configs()
    conn = duckdb.connect(str(metadata_path))
    try:
        recreate_files_uuid(conn)
        recreate_queries_table(conn)
        files = get_files(conn, metadata_path)

        if not files:
            print("No entries found in files table; nothing to do.")
            return
        volumetric = [
            entry
            for entry in files
            if entry.benchmark.lower() in VOLUMETRIC_BENCHMARK_SOURCES
        ]
        if volumetric:
            formats = {entry.file_format for entry in volumetric}
            volumetric_rows = generate_volumetric_queries(volumetric, configs, formats)
            insert_queries(conn, volumetric_rows)
        else:
            print(
                "No volumetric entries found in files table; skipping volumetric query generation."
            )

        tpch_sql = load_tpch_sql_queries()
        tpch_rows = generate_tpch_queries(files, configs, tpch_sql)
        if tpch_rows:
            insert_queries(conn, tpch_rows)
        else:
            if not tpch_sql:
                print(
                    f"TPC-H query directory '{TPCH_QUERY_DIR}' not found; skipping TPC-H query generation."
                )
            else:
                print("No TPC-H datasets available for query generation.")

        total = conn.execute("SELECT COUNT(*) FROM queries;").fetchone()[0]
        print(f"Generated {total} query definitions.")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
