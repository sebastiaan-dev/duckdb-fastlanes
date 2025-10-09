#!/usr/bin/env python
"""Compute FastLanes vs Parquet speed-ups for volumetric benchmarks.

The script scans volumetric benchmark summaries, compares FastLanes and
Parquet timings per table/column/operation, and emits matrix-style CSV files
with relative speed-ups for both scan time and total query latency.
"""

from __future__ import annotations

import argparse
import csv
import json
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence, Tuple

import matplotlib.pyplot as plt
import numpy as np
from matplotlib import colors
from math import isfinite
import re


RESULTS_SUBDIR = Path("benchmark/results/volumetric")
DEFAULT_OUTPUT_DIR = RESULTS_SUBDIR / "comparisons"
SUMMARY_PREFIX = "summary__"
SUMMARY_SUFFIX = ".json"

FASTLANES = "fls"
PARQUET = "parquet"
SUPPORTED_FORMATS = {FASTLANES, PARQUET}


@dataclass
class RunRecord:
    topic: str
    dataset: str
    file_format: str
    table: str
    column: str
    operation: str
    run: int
    total_time: float
    table_time: float
    config_tag: str


class ResultLoader:
    def __init__(self, results_root: Path) -> None:
        self.results_root = results_root

    def iter_summary_files(self) -> Iterable[Path]:
        pattern = f"**/{SUMMARY_PREFIX}*{SUMMARY_SUFFIX}"
        yield from self.results_root.glob(pattern)

    def load(self, filters: argparse.Namespace) -> List[RunRecord]:
        records: List[RunRecord] = []
        for summary_path in sorted(self.iter_summary_files()):
            summary = self._load_summary(summary_path)
            if not summary:
                continue

            topic = summary.get("topic")
            dataset = summary.get("dataset")
            file_format = summary.get("file_format")
            if file_format not in SUPPORTED_FORMATS:
                continue
            if not self._matches_filters(topic, dataset, file_format, filters):
                continue

            config_tag = self._infer_config_tag(summary_path)
            dataset_dir = summary_path.parent
            for benchmark in summary.get("benchmarks", []):
                table = benchmark.get("table")
                column = benchmark.get("column")
                operation = benchmark.get("operation")
                if filters.tables and table not in filters.tables:
                    continue
                if filters.columns and column not in filters.columns:
                    continue
                if filters.operations and operation not in filters.operations:
                    continue

                for run in benchmark.get("runs", []):
                    total_time = float(run.get("duration_s", 0.0))
                    table_time = run.get("table_function_time")
                    profiling_rel = run.get("profiling_output")
                    if table_time is None:
                        table_time = self._extract_table_time(dataset_dir, profiling_rel)
                    else:
                        table_time = float(table_time)
                    records.append(
                        RunRecord(
                            topic=str(topic),
                            dataset=str(dataset),
                            file_format=str(file_format),
                            table=str(table),
                            column=str(column),
                            operation=str(operation),
                            run=int(run.get("run", 0)),
                            total_time=total_time,
                            table_time=table_time,
                            config_tag=config_tag,
                        )
                    )
        return records

    def _load_summary(self, path: Path) -> Optional[Dict[str, object]]:
        try:
            with path.open("r", encoding="utf-8") as fh:
                return json.load(fh)
        except Exception:
            return None

    def _infer_config_tag(self, summary_path: Path) -> str:
        name = summary_path.name
        if name.startswith(SUMMARY_PREFIX) and name.endswith(SUMMARY_SUFFIX):
            return name[len(SUMMARY_PREFIX) : -len(SUMMARY_SUFFIX)]
        return summary_path.stem

    def _matches_filters(
        self,
        topic: Optional[str],
        dataset: Optional[str],
        file_format: Optional[str],
        filters: argparse.Namespace,
    ) -> bool:
        if filters.topics and topic not in filters.topics:
            return False
        if filters.datasets and dataset not in filters.datasets:
            return False
        if filters.formats and file_format not in filters.formats:
            return False
        return True

    def _extract_table_time(self, dataset_dir: Path, profiling_rel: Optional[str]) -> float:
        if not profiling_rel:
            return 0.0
        profile_path = (dataset_dir / profiling_rel).resolve()
        if not profile_path.exists():
            return 0.0
        try:
            with profile_path.open("r", encoding="utf-8") as fh:
                profile = json.load(fh)
        except Exception:
            return 0.0
        return collect_table_operator_time(profile.get("children") or [])


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


def collect_table_operator_time(nodes: List[Dict[str, object]]) -> float:
    total = 0.0
    for node in nodes:
        total += get_operator_time(node)
        children = node.get("children") or []
        if isinstance(children, list):
            total += collect_table_operator_time(children)  # type: ignore[arg-type]
    return total


def get_operator_time(node: Dict[str, object]) -> float:
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
    return float(timing)


def aggregate_records(records: Iterable[RunRecord]) -> Dict[Tuple[str, str, str, str, str, str, str], Dict[str, float]]:
    buckets: Dict[Tuple[str, str, str, str, str, str, str], Dict[str, float]] = {}
    counters: Dict[Tuple[str, str, str, str, str, str, str], int] = defaultdict(int)

    for record in records:
        key = (
            record.config_tag,
            record.topic,
            record.dataset,
            record.table,
            record.column,
            record.operation,
            record.file_format,
        )
        bucket = buckets.setdefault(
            key,
            {
                "total_sum": 0.0,
                "table_sum": 0.0,
            },
        )
        bucket["total_sum"] += record.total_time
        bucket["table_sum"] += record.table_time
        counters[key] += 1

    averages: Dict[Tuple[str, str, str, str, str, str, str], Dict[str, float]] = {}
    for key, sums in buckets.items():
        count = counters[key]
        if count == 0:
            continue
        averages[key] = {
            "avg_total": sums["total_sum"] / count,
            "avg_table": sums["table_sum"] / count,
        }
    return averages


def build_speedup_tables(
    aggregated: Dict[Tuple[str, str, str, str, str, str, str], Dict[str, float]]
) -> Dict[str, Dict[str, Dict[Tuple[str, str, str, str], Dict[str, float]]]]:
    result: Dict[str, Dict[str, Dict[Tuple[str, str, str, str], Dict[str, float]]]] = defaultdict(
        lambda: defaultdict(lambda: defaultdict(dict))
    )

    per_format: Dict[str, Dict[Tuple[str, str, str, str, str, str], Dict[str, float]]] = defaultdict(dict)
    for key, values in aggregated.items():
        config_tag, topic, dataset, table, column, operation, fmt = key
        per_format[fmt][(config_tag, topic, dataset, table, column, operation)] = values

    fastlanes_metrics = per_format.get(FASTLANES, {})
    parquet_metrics = per_format.get(PARQUET, {})

    for pkey, parquet_vals in parquet_metrics.items():
        config_tag, topic, dataset, table, column, operation = pkey
        fast_vals = fastlanes_metrics.get(pkey)
        if not fast_vals:
            continue
        total_fast = fast_vals["avg_total"]
        scan_fast = fast_vals["avg_table"]
        total_parquet = parquet_vals["avg_total"]
        scan_parquet = parquet_vals["avg_table"]

        total_ratio = compute_speedup(total_parquet, total_fast)
        scan_ratio = compute_speedup(scan_parquet, scan_fast)

        row_key = (topic, dataset, table, column)
        result[config_tag]["total"][row_key][operation] = total_ratio
        result[config_tag]["scan"][row_key][operation] = scan_ratio

    return result


def compute_speedup(parquet_time: float, fastlanes_time: float) -> Optional[float]:
    if fastlanes_time <= 0:
        return None
    if parquet_time <= 0:
        return None
    return parquet_time / fastlanes_time


def format_ratio(value: Optional[float]) -> str:
    if value is None:
        return ""
    return f"{value:.2f}"


def write_table(
    output_path: Path,
    table_data: Dict[Tuple[str, str, str, str], Dict[str, float]],
    operations: List[str],
    row_keys: List[Tuple[str, str, str, str]],
) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.writer(fh)
        header = ["topic", "dataset", "table", "column"] + operations
        writer.writerow(header)
        for row_key in row_keys:
            topic, dataset, table, column = row_key
            row = [topic, dataset, table, column]
            metrics = table_data.get(row_key, {})
            for operation in operations:
                row.append(format_ratio(metrics.get(operation)))
            writer.writerow(row)


def render_heatmap(
    output_path: Path,
    table_data: Dict[Tuple[str, str, str, str], Dict[str, float]],
    operations: List[str],
    row_keys: List[Tuple[str, str, str, str]],
    title: str,
) -> None:
    if not operations or not row_keys:
        return

    data = np.full((len(row_keys), len(operations)), np.nan, dtype=float)
    for i, row_key in enumerate(row_keys):
        metrics = table_data.get(row_key)
        if not metrics:
            continue
        for j, op in enumerate(operations):
            value = metrics.get(op)
            if value is None:
                continue
            data[i, j] = float(value)

    if np.isnan(data).all():
        return
    min_ratio = np.nanmin(data)
    max_ratio = np.nanmax(data)
    if not isfinite(min_ratio):
        min_ratio = 1.0
    if not isfinite(max_ratio):
        max_ratio = 1.0
    if min_ratio <= 0:
        min_ratio = 0.01
    if max_ratio <= 0:
        max_ratio = 1.0
    if min_ratio >= 1.0 and max_ratio > 1.0:
        span = max_ratio - 1.0
        min_ratio = max(0.01, 1.0 - span)
    if max_ratio <= 1.0 and min_ratio < 1.0:
        span = 1.0 - min_ratio
        max_ratio = 1.0 + span
    # Ensure the scale has some spread
    if abs(max_ratio - min_ratio) < 1e-6:
        max_ratio = max(min_ratio + 0.01, 1.01 if min_ratio <= 1.0 else min_ratio + 0.01)

    cmap = colors.LinearSegmentedColormap.from_list(
        "fls_speedup", ["#c72020", "#ffffff", "#158f15"], N=256
    )
    cmap.set_bad("#d9d9d9")
    norm = colors.TwoSlopeNorm(vmin=min_ratio, vcenter=1.0, vmax=max_ratio)

    fig, ax = plt.subplots(
        figsize=(max(6, len(operations) * 0.9), max(6, len(row_keys) * 0.35))
    )
    im = ax.imshow(data, cmap=cmap, norm=norm, aspect="auto")

    x_positions = np.arange(len(operations))
    y_positions = np.arange(len(row_keys))
    x_labels = operations
    y_labels = [f"{table}.{column}" for (_, _, table, column) in row_keys]
    ax.set_xticks(x_positions)
    ax.set_xticklabels(x_labels, rotation=45, ha="right")
    ax.set_yticks(y_positions)
    ax.set_yticklabels(y_labels)
    ax.set_title(title)

    ax.set_xlabel("Operation")
    ax.set_ylabel("Table.Column")

    table_order = [row_key[2] for row_key in row_keys]
    last = None
    for idx, table_name in enumerate(table_order):
        if last is not None and table_name != last:
            ax.axhline(idx - 0.5, color="#555555", linewidth=0.5, alpha=0.7)
        last = table_name

    for i in range(len(row_keys) + 1):
        ax.axhline(i - 0.5, color="#f0f0f0", linewidth=0.2, alpha=0.5)
    for j in range(len(operations) + 1):
        ax.axvline(j - 0.5, color="#f0f0f0", linewidth=0.2, alpha=0.5)

    for i in range(len(row_keys)):
        for j in range(len(operations)):
            value = data[i, j]
            if np.isnan(value):
                continue
            deviation = abs(value - 1.0)
            span = max(max_ratio - 1.0, 1.0 - min_ratio)
            text_color = "white" if deviation > span * 0.4 else "black"
            ax.text(
                j,
                i,
                f"{value:.2f}",
                ha="center",
                va="center",
                fontsize=7,
                color=text_color,
            )

    cbar = fig.colorbar(im, ax=ax)
    cbar.set_label("Relative speed-up (Parquet / FastLanes)")

    fig.tight_layout()
    output_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(output_path, dpi=200, bbox_inches="tight")
    plt.close(fig)


def slugify_table_name(value: str) -> str:
    value = value.strip().lower()
    value = re.sub(r"[^a-z0-9]+", "-", value)
    value = value.strip("-")
    return value or "table"


def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Compute FastLanes vs Parquet speed-up tables",
    )
    parser.add_argument(
        "--results-root",
        default=str(RESULTS_SUBDIR),
        help="Root directory containing volumetric benchmark results",
    )
    parser.add_argument(
        "--output-dir",
        default=str(DEFAULT_OUTPUT_DIR),
        help="Directory where comparison tables will be written",
    )
    parser.add_argument("--topics", nargs="*", help="Filter by topics")
    parser.add_argument("--datasets", nargs="*", help="Filter by datasets")
    parser.add_argument("--formats", nargs="*", help="Filter by formats")
    parser.add_argument("--tables", nargs="*", help="Filter by tables")
    parser.add_argument("--columns", nargs="*", help="Filter by columns")
    parser.add_argument("--operations", nargs="*", help="Filter by operations")
    return parser.parse_args(argv)


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = parse_args(argv)
    results_root = Path(args.results_root).resolve()
    output_dir = Path(args.output_dir).resolve()
    if not results_root.exists():
        print(f"Results root '{results_root}' does not exist")
        return 1

    loader = ResultLoader(results_root)
    records = loader.load(args)
    if not records:
        print("No matching FastLanes/Parquet records found")
        return 1

    aggregated = aggregate_records(records)
    comparison = build_speedup_tables(aggregated)

    for config_tag, views in comparison.items():
        config_dir = output_dir / config_tag
        # Collect union of operations for consistent headers
        operations = sorted(
            {
                op
                for table_view in views.values()
                for metrics in table_view.values()
                for op in metrics.keys()
            }
        )
        row_keys = sorted(
            set(views["total"].keys()) | set(views["scan"].keys()),
            key=lambda key: (key[2], key[3], key[0], key[1]),
        )

        if not operations or not row_keys:
            continue

        write_table(
            config_dir / "speedup_total.csv",
            views["total"],
            operations,
            row_keys,
        )
        write_table(
            config_dir / "speedup_scan.csv",
            views["scan"],
            operations,
            row_keys,
        )

        render_heatmap(
            config_dir / "speedup_total.png",
            views["total"],
            operations,
            row_keys,
            title=f"Total runtime speed-up ({config_tag})",
        )
        render_heatmap(
            config_dir / "speedup_scan.png",
            views["scan"],
            operations,
            row_keys,
            title=f"Scan time speed-up ({config_tag})",
        )

        per_table_dir = config_dir / "per_table"
        per_table_dir.mkdir(parents=True, exist_ok=True)
        tables = sorted({table for (_, _, table, _) in row_keys})
        for table_name in tables:
            table_rows = [rk for rk in row_keys if rk[2] == table_name]
            table_ops = sorted(
                {
                    op
                    for metric_map in (views["total"], views["scan"])
                    for rk in table_rows
                    for op in metric_map.get(rk, {}).keys()
                }
            )
            if not table_rows or not table_ops:
                continue
            slug = slugify_table_name(table_name)
            render_heatmap(
                per_table_dir / f"{slug}_total.png",
                views["total"],
                table_ops,
                table_rows,
                title=f"{table_name} total runtime speed-up ({config_tag})",
            )
            render_heatmap(
                per_table_dir / f"{slug}_scan.png",
                views["scan"],
                table_ops,
                table_rows,
                title=f"{table_name} scan time speed-up ({config_tag})",
            )

        print(f"Speed-up tables and heatmaps written to {config_dir}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
