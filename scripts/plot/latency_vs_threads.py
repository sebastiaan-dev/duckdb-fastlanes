from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Dict, Iterable, Optional, Tuple

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

CURRENT_DIR = Path(__file__).resolve().parent
SCRIPTS_DIR = CURRENT_DIR.parent
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

from plot.common import load_benchmark_data


def parse_bool_filter(value: str) -> Optional[bool]:
    mapping = {"true": True, "false": False, "any": None}
    key = value.strip().lower()
    if key not in mapping:
        raise argparse.ArgumentTypeError(
            f"Expected one of {tuple(mapping.keys())}, got '{value}'"
        )
    return mapping[key]


def create_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Plot end-to-end latency versus thread count, broken down by table scan vs other operators."
    )
    parser.add_argument(
        "database",
        type=Path,
        help="Path to the DuckDB results database.",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("plots"),
        help="Directory where plots will be written (default: plots).",
    )
    parser.add_argument(
        "--benchmark",
        nargs="*",
        help="Benchmark labels to include (matches metadata; default: all).",
    )
    parser.add_argument(
        "--query-type",
        nargs="*",
        help="Query types to include (e.g. tpch, volumetric; default: all).",
    )
    parser.add_argument(
        "--query-id",
        nargs="*",
        help="Specific query IDs to include (default: all).",
    )
    parser.add_argument(
        "--ram-disk",
        type=parse_bool_filter,
        default=None,
        help="Filter by ram_disk flag (true/false/any, default:any).",
    )
    parser.add_argument(
        "--object-cache",
        type=parse_bool_filter,
        default=None,
        help="Filter by object_cache flag (true/false/any, default:any).",
    )
    parser.add_argument(
        "--external-file-cache",
        type=parse_bool_filter,
        default=None,
        help="Filter by external_file_cache flag (true/false/any, default:any).",
    )
    parser.add_argument(
        "--max-queries",
        type=int,
        help="Optional limit on the number of queries to plot.",
    )
    parser.add_argument(
        "--formats",
        nargs="*",
        help="Limit to specific format labels (e.g. duckdb parquet).",
    )
    parser.add_argument(
        "--per-query",
        action="store_true",
        help="Produce one plot per query instead of aggregated per query type.",
    )
    return parser


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
) -> pd.DataFrame:
    filtered = df.copy()
    if benchmark:
        benchmark_lower = {value.lower() for value in benchmark}
        filtered = filtered[
            filtered["benchmark_label"].fillna("").str.lower().isin(benchmark_lower)
        ]
    if query_type:
        query_type_lower = {value.lower() for value in query_type}
        filtered = filtered[
            filtered["query_type"].fillna("").str.lower().isin(query_type_lower)
        ]
    if query_id:
        query_id_lower = {value.lower() for value in query_id}
        filtered = filtered[filtered["query_id"].str.lower().isin(query_id_lower)]
    if ram_disk is not None:
        filtered = filtered[filtered["ram_disk"] == ram_disk]
    if object_cache is not None:
        filtered = filtered[filtered["object_cache"] == object_cache]
    if external_file_cache is not None:
        filtered = filtered[filtered["external_file_cache"] == external_file_cache]
    if formats:
        format_lower = {value.lower() for value in formats}
        filtered = filtered[
            filtered["format_label"].fillna("").str.lower().isin(format_lower)
        ]
    return filtered


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
            ["query_id", "query_type", "sql_summary", "format_label", "thread_count"],
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


def aggregate_by_query_type(grouped: pd.DataFrame) -> pd.DataFrame:
    summary = (
        grouped.groupby(["query_type", "format_label", "thread_count"], dropna=False)
        .agg(
            mean_table_ms=("mean_table_ms", "mean"),
            mean_other_ms=("mean_other_ms", "mean"),
            mean_execution_ms=("mean_execution_ms", "mean"),
            queries=("query_id", "nunique"),
        )
        .reset_index()
    )
    summary["mean_other_ms"] = summary["mean_other_ms"].clip(lower=0.0)
    return summary


def plot_query_latency(
    grouped: pd.DataFrame,
    output_dir: Path,
    *,
    query_id: str,
    query_type: str,
    sql_summary: str,
) -> Optional[Path]:
    if grouped.empty:
        return None
    grouped = grouped.sort_values(["thread_count", "format_label"])
    thread_counts = sorted(grouped["thread_count"].unique())
    formats = grouped["format_label"].dropna().unique()
    if len(formats) == 0:
        formats = np.array(["unknown"])

    # Prepare colors per format
    cmap = plt.get_cmap("tab10")
    format_colors: Dict[str, Tuple[float, float, float, float]] = {}
    for idx, fmt in enumerate(sorted(formats)):
        format_colors[fmt] = cmap(idx % cmap.N)

    x_positions = np.arange(len(thread_counts))
    width = 0.8 / max(len(formats), 1)

    fig, ax = plt.subplots(figsize=(10, 6))
    for fmt_index, fmt in enumerate(sorted(formats)):
        fmt_data = grouped[grouped["format_label"] == fmt]
        table_means = []
        other_means = []
        for thread in thread_counts:
            subset = fmt_data[fmt_data["thread_count"] == thread]
            if subset.empty:
                table_means.append(0.0)
                other_means.append(0.0)
            else:
                table_means.append(subset["mean_table_ms"].iloc[0])
                other_means.append(subset["mean_other_ms"].iloc[0])

        offsets = x_positions + fmt_index * width
        base_color = format_colors[fmt]
        table_color = base_color
        other_color = tuple(min(1.0, channel + 0.2) for channel in base_color[:3]) + (
            base_color[3],
        )
        ax.bar(
            offsets,
            table_means,
            width,
            label=f"{fmt} table scan",
            color=table_color,
        )
        ax.bar(
            offsets,
            other_means,
            width,
            bottom=table_means,
            label=f"{fmt} other operators",
            color=other_color,
        )

    ax.set_xticks(
        x_positions + width * (len(formats) - 1) / 2
        if len(formats) > 0
        else x_positions
    )
    ax.set_xticklabels([str(tc) for tc in thread_counts])
    ax.set_xlabel("Thread count")
    ax.set_ylabel("Mean latency (ms)")
    query_preview = sql_summary if sql_summary else ""
    if len(query_preview) > 120:
        query_preview = query_preview[:117] + "..."
    ax.set_title(f"Query {query_id[:8]} ({query_type})\n{query_preview}")
    ax.legend(loc="best", fontsize="small", ncol=2)
    ax.grid(True, axis="y", linestyle="--", linewidth=0.5, alpha=0.5)
    fig.tight_layout()

    safe_query_type = query_type.replace("/", "_").replace(" ", "_")
    output_path = (
        output_dir / f"{safe_query_type}_{query_id[:8]}_latency_vs_threads.png"
    )
    output_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(output_path, dpi=150)
    plt.close(fig)
    return output_path


def main() -> None:
    parser = create_parser()
    args = parser.parse_args()

    df = load_benchmark_data(args.database, include_failed=False)
    if df.empty:
        print("No benchmark results found in the database.")
        return

    df = filter_dataframe(
        df,
        benchmark=args.benchmark,
        query_type=args.query_type,
        query_id=args.query_id,
        ram_disk=args.ram_disk,
        object_cache=args.object_cache,
        external_file_cache=args.external_file_cache,
        formats=args.formats,
    )
    if df.empty:
        print("No results match the requested filters.")
        return

    df = df[df["thread_count"].notna() & df["execution_time_ms"].notna()]
    df = ensure_time_columns(df)
    df = df[df["format_label"].notna()]
    if df.empty:
        print("No valid rows remain after filtering for thread counts and formats.")
        return

    grouped = aggregate_latency(df)

    grouped_queries = grouped.groupby(
        ["query_type", "sql_summary", "format_label"]
    ).groups
    output_dir = args.output_dir.resolve()
    output_dir.mkdir(parents=True, exist_ok=True)

    plotted = 0
    for (query_id, query_type, sql_summary), indices in grouped_queries.items():
        if args.max_queries is not None and plotted >= args.max_queries:
            break
        data_subset = grouped.loc[indices]
        output_path = plot_query_latency(
            data_subset,
            output_dir,
            query_id=query_id,
            query_type=query_type,
            sql_summary=sql_summary,
        )
        if output_path:
            print(f"Wrote {output_path}")
            plotted += 1

    if plotted == 0:
        print("No plots generated. Check filters or data availability.")


if __name__ == "__main__":
    main()
