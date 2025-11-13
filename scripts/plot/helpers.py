from __future__ import annotations

import numpy as np
import re
from typing import Any, Optional
import pandas as pd
import argparse


def parse_agg(sql: str) -> str:
    if not isinstance(sql, str):
        return np.nan
    s = sql.strip()
    if re.search(r"(?i)\bcount\s*\(\s*distinct\b", s):
        return "count distinct"
    for op in ("sum", "avg", "mean", "min", "max", "count"):
        if re.search(rf"(?i)\b{op}\s*\(", s):
            return "avg" if op == "mean" else op
    return np.nan


def filter_by_value(df: pd.DataFrame, key: Any, value: Optional[Any]) -> pd.DataFrame:
    if value is None:
        return df
    return df[df[key] == value]


def aggregate_by_query_id(df: pd.DataFrame) -> pd.DataFrame:
    grouped = df.groupby("query_id", dropna=False).agg(
        mean_profile_total_ms=("profile_total_ms", "mean"),
        mean_execution_ms=("execution_time_ms", "mean"),
        mean_table_ms=("table_time_ms", "mean"),
        mean_other_ms=("other_time_ms", "mean"),
        runs=("result_id", "count"),
    )
    return df.merge(grouped, on="query_id", how="left").drop(
        columns=[
            "run_id",
            "result_id",
            "created_at",
            "query_id",
            "file_ids",
            "sql_summary",
            "benchmark",
            "success",
            "ram_disk",
            "object_cache",
            "external_file_cache",
            "query_type",
            "execution_time_ms",
            "table_time_ms",
            "other_time_ms",
        ],
        errors="ignore",
    )


def parse_bool_filter(value: str) -> Optional[bool]:
    mapping = {"true": True, "false": False, "any": None}
    key = value.strip().lower()
    if key not in mapping:
        raise argparse.ArgumentTypeError(
            f"Expected one of {tuple(mapping.keys())}, got '{value}'"
        )
    return mapping[key]


def ensure_time_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["table_time_ms"] = pd.to_numeric(df["table_time_ms"], errors="coerce")
    df["other_time_ms"] = pd.to_numeric(df["other_time_ms"], errors="coerce")
    df["execution_time_ms"] = pd.to_numeric(df["execution_time_ms"], errors="coerce")

    df.loc[df["table_time_ms"].isna(), "table_time_ms"] = 0.0
    missing_other = df["other_time_ms"].isna()
    df.loc[missing_other, "other_time_ms"] = (
        df.loc[missing_other, "execution_time_ms"]
        - df.loc[missing_other, "table_time_ms"]
    )
    df["other_time_ms"] = df["other_time_ms"].clip(lower=0.0)
    return df


def aggregate_latency(df: pd.DataFrame) -> pd.DataFrame:
    grouped = (
        df.groupby(
            ["query_id", "query_type", "sql_summary", "format", "thread_count"],
            dropna=False,
        )
        .agg(
            mean_execution_ms=("execution_time_ms", "mean"),
            mean_table_ms=("table_time_ms", "mean"),
            mean_other_ms=("other_time_ms", "mean"),
            runs=("result_id", "count"),
        )
        .reset_index()
    )
    grouped["mean_other_ms"] = grouped["mean_other_ms"].clip(lower=0.0)
    return grouped
