from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple
from uuid import UUID

import duckdb
import pandas as pd


TABLE_OPERATOR_KEYWORDS = (
    "TABLE_FUNCTION",
    "TABLE_SCAN",
    "SEQ_SCAN",
    "TABLE SCAN",
    "PARQUET_SCAN",
    "FLS_SCAN",
    "READ_PARQUET",
    "READ_FLS",
    "READ_CSV",
)

FLAG = False


def resolve_profile_path(profile_path: str | None, db_path: Path) -> Optional[Path]:
    if not profile_path:
        return None
    path = Path(profile_path)
    if not path.is_absolute():
        path = db_path.parent / path
    return path.resolve()


def _is_table_operator(node: Dict[str, Any]) -> bool:
    operator_type = str(node.get("operator_type", "")).upper()
    operator_name = str(node.get("operator_name", "")).upper()
    extra_type = (
        str(node.get("extra_info", {}).get("Type", "")).upper()
        if isinstance(node.get("extra_info"), dict)
        else ""
    )
    all_strings = {operator_type, operator_name, extra_type}
    return any(
        keyword in value
        for keyword in TABLE_OPERATOR_KEYWORDS
        for value in all_strings
        if value
    )


def _extract_table_time_ms(
    profile: Dict[str, Any],
) -> Tuple[Optional[float], Optional[float], Optional[float]]:
    if not profile:
        return None, None, None
    try:
        latency_ms = float(profile.get("latency", 0.0)) * 1000.0
    except (TypeError, ValueError):
        latency_ms = None

    def traverse(node: Dict[str, Any]) -> Tuple[float, float]:
        if not isinstance(node, dict):
            return 0.0, 0.0
        operator_timing = node.get("operator_timing")
        timing_ms = 0.0
        if operator_timing is not None:
            try:
                timing_ms = float(operator_timing) * 1000.0
            except (TypeError, ValueError):
                timing_ms = 0.0
        total_time = timing_ms
        table_time = timing_ms if _is_table_operator(node) else 0.0
        for child in node.get("children", []):
            child_total, child_table = traverse(child)
            total_time += child_total
            table_time += child_table
        return total_time, table_time

    total_cpu_time_ms, table_time_ms = traverse(profile)
    other_time_ms: Optional[float]
    if total_cpu_time_ms > 0.0:
        other_time_ms = max(total_cpu_time_ms - table_time_ms, 0.0)
    elif latency_ms is not None:
        other_time_ms = max(latency_ms - table_time_ms, 0.0)
    else:
        other_time_ms = None
    return table_time_ms, other_time_ms, total_cpu_time_ms


def _load_profile_metrics(
    profile_path: Optional[Path],
) -> Tuple[Optional[float], Optional[float], Optional[float], Optional[float]]:
    if not profile_path or not profile_path.exists():
        return None, None, None, None
    try:
        with profile_path.open("r", encoding="utf-8") as fh:
            profile = json.load(fh)
    except (OSError, json.JSONDecodeError):
        return None, None, None, None
    table_time_ms, other_time_ms, cpu_total_ms = _extract_table_time_ms(profile)
    latency_ms: Optional[float] = None
    latency = profile.get("latency")
    if latency is not None:
        try:
            latency_ms = float(latency) * 1000.0
        except (TypeError, ValueError):
            latency_ms = None
    return table_time_ms, other_time_ms, cpu_total_ms, latency_ms


def _load_file_mapping(conn: duckdb.DuckDBPyConnection) -> Dict[UUID, Dict[str, Any]]:
    file_mapping: Dict[UUID, Dict[str, Any]] = {}
    try:
        files_uuid_df = conn.execute("SELECT * FROM files_uuid").fetchdf()
    except duckdb.Error:
        return file_mapping
    for _, row in files_uuid_df.iterrows():
        entry: Dict[str, Any] = row.to_dict()
        try:
            entry_id = UUID(str(entry["id"]))
        except (TypeError, ValueError):
            continue
        file_mapping[entry_id] = entry
    return file_mapping


def _label_from_file_ids(
    file_ids: Sequence[UUID],
    file_mapping: Dict[UUID, Dict[str, Any]],
    field: str,
) -> Optional[str]:
    values = {
        str(file_mapping[file_id].get(field))
        for file_id in file_ids
        if file_id in file_mapping and file_mapping[file_id].get(field) is not None
    }
    if not values:
        return None
    if len(values) == 1:
        return values.pop()
    return ", ".join(sorted(values))


def load_benchmark_data(
    db_path: Path,
    *,
    include_failed: bool = False,
) -> pd.DataFrame:
    db_path = db_path.resolve()
    conn = duckdb.connect(str(db_path))
    try:
        query = """
            SELECT
                r.id AS result_id,
                r.query_id,
                r.run_id,
                r.execution_time_ms,
                r.profile_path,
                r.success,
                r.thread_count,
                r.ram_disk,
                r.object_cache,
                r.external_file_cache,
                r.created_at,
                q.table_name,
                q.column_name,
                q.operator,
                q.tpch_query,
                q.type AS query_type,
                q.sql,
                q.file_ids
            FROM results r
            JOIN queries q ON q.id = r.query_id
        """
        if not include_failed:
            query += " WHERE r.success"
        df = conn.execute(query).fetchdf()

        file_mapping = _load_file_mapping(conn)
    finally:
        conn.close()

    if df.empty:
        return df

    df["result_id"] = df["result_id"].astype(str)
    df["query_id"] = df["query_id"].astype(str)

    formats: List[Optional[str]] = []
    benchmarks: List[Optional[str]] = []
    scale_factors: List[Optional[str]] = []
    rowgroup_sizes: List[Optional[str]] = []
    table_times: List[Optional[float]] = []
    other_times: List[Optional[float]] = []
    profile_cpu_totals: List[Optional[float]] = []
    profile_latencies: List[Optional[float]] = []

    for idx, row in df.iterrows():
        file_ids: Sequence[UUID] = row["file_ids"]
        formats.append(_label_from_file_ids(file_ids, file_mapping, "file_format"))
        benchmarks.append(_label_from_file_ids(file_ids, file_mapping, "benchmark"))
        scale_factors.append(_label_from_file_ids(file_ids, file_mapping, "sf"))
        rowgroup_sizes.append(
            _label_from_file_ids(file_ids, file_mapping, "rowgroup_size")
        )

        profile_path = resolve_profile_path(row["profile_path"], db_path)
        (
            table_time_ms,
            other_time_ms,
            cpu_total_ms,
            latency_ms,
        ) = _load_profile_metrics(profile_path)
        table_times.append(table_time_ms)
        other_times.append(other_time_ms)
        profile_cpu_totals.append(cpu_total_ms)
        profile_latencies.append(latency_ms)

    df["format"] = formats
    df["benchmark"] = benchmarks
    df["scale_factor"] = scale_factors
    df["rowgroup_size"] = rowgroup_sizes
    df["table_time_ms"] = table_times
    df["other_time_ms"] = other_times
    df["profile_cpu_total_ms"] = profile_cpu_totals
    df["profile_latency_ms"] = profile_latencies
    # Backwards compatibility: keep legacy column name pointing to latency measurements.
    df["profile_total_ms"] = df["profile_latency_ms"]

    def summarise_sql(sql_text: str) -> str:
        if not sql_text:
            return ""
        first_line = sql_text.strip().splitlines()[0]
        return first_line[:120]

    df["sql_summary"] = df["sql"].apply(summarise_sql)

    if "execution_time_ms" in df.columns:
        df["execution_time_ms"] = pd.to_numeric(
            df["execution_time_ms"], errors="coerce"
        )
    if "table_time_ms" in df.columns:
        df["table_time_ms"] = pd.to_numeric(df["table_time_ms"], errors="coerce")
    if "other_time_ms" in df.columns:
        df["other_time_ms"] = pd.to_numeric(df["other_time_ms"], errors="coerce")
    if "profile_cpu_total_ms" in df.columns:
        df["profile_cpu_total_ms"] = pd.to_numeric(
            df["profile_cpu_total_ms"], errors="coerce"
        )
    if "profile_latency_ms" in df.columns:
        df["profile_latency_ms"] = pd.to_numeric(
            df["profile_latency_ms"], errors="coerce"
        )
    if "profile_total_ms" in df.columns:
        df["profile_total_ms"] = pd.to_numeric(df["profile_total_ms"], errors="coerce")
    return df


def filter_dataframe(
    df: pd.DataFrame,
    *,
    benchmark: Optional[Iterable[str]] = None,
    query_type: Optional[Iterable[str]] = None,
    query_id: Optional[Iterable[str]] = None,
    ram_disk: Optional[bool] = None,
    object_cache: Optional[bool] = None,
    external_file_cache: Optional[bool] = None,
    formats: Optional[Iterable[str]] = None,
    thread_count: Optional[int] = None,
) -> pd.DataFrame:
    filtered = df.copy()
    if benchmark:
        benchmark_lower = {value.lower() for value in benchmark}
        filtered = filtered[
            filtered["benchmark"].fillna("").str.lower().isin(benchmark_lower)
        ]
    if query_type:
        query_type_lower = {value.lower() for value in query_type}
        filtered = filtered[
            filtered["query_type"].fillna("").str.lower().isin(query_type_lower)
        ]
    if query_id:
        query_id_lower = {value.lower() for value in query_id}
        filtered = filtered[filtered["query_id"].str.lower().isin(query_id_lower)]
    if thread_count is not None:
        filtered = filtered[filtered["thread_count"] == thread_count]
    if ram_disk is not None:
        filtered = filtered[filtered["ram_disk"] == ram_disk]
    if object_cache is not None:
        filtered = filtered[filtered["object_cache"] == object_cache]
    if external_file_cache is not None:
        filtered = filtered[filtered["external_file_cache"] == external_file_cache]
    if formats:
        format_lower = {value.lower() for value in formats}
        filtered = filtered[
            filtered["format"].fillna("").str.lower().isin(format_lower)
        ]
    return filtered
