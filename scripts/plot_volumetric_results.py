#!/usr/bin/env python
"""Parse volumetric benchmark results and generate comparison plots.

The script expects the benchmark output produced by `benchmark/run_volumetric_benchmarks.py`.
It computes aggregated metrics (average timings per file format, table/column/operation)
while also parsing DuckDB profiling JSON to split the total query latency into the
portion spent inside the table/scanning operator and the remaining time.

Several stacked bar charts are emitted to highlight performance differences between
formats while visualising where the execution time is spent.
"""

from __future__ import annotations

import argparse
import json
import sys
from collections import defaultdict
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, Iterator, List, Optional, Sequence, Tuple

import matplotlib.pyplot as plt
import numpy as np
from matplotlib import colors as mcolors


RESULTS_SUBDIR = Path("benchmark/results/volumetric")
DEFAULT_OUTPUT_DIR = RESULTS_SUBDIR / "plots"
SUMMARY_PREFIX = "summary__"
SUMMARY_SUFFIX = ".json"

# Operators that should count towards the "table function" portion of the runtime.
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
    table_function_time: float
    other_time: float
    config_tag: str


class ResultLoader:
    def __init__(self, results_root: Path) -> None:
        self.results_root = results_root

    def iter_summary_files(self) -> Iterator[Path]:
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
            if not self._matches_filters(topic, dataset, file_format, filters):
                continue

            dataset_dir = summary_path.parent
            config_tag = self._infer_config_tag(summary_path)
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
                    run_idx = run.get("run")
                    total_time = float(run.get("duration_s", 0.0))
                    profiling_rel = run.get("profiling_output")
                    profile_path = None
                    if profiling_rel:
                        profile_path = (dataset_dir / profiling_rel).resolve()
                    table_time = 0.0
                    if profile_path and profile_path.exists():
                        try:
                            table_time = self._extract_table_time(profile_path)
                        except Exception as exc:  # pragma: no cover - defensive
                            print(
                                f"Failed to parse profiling output '{profile_path}': {exc}",
                                file=sys.stderr,
                            )
                    table_time = min(table_time, total_time)
                    other_time = max(0.0, total_time - table_time)
                    records.append(
                        RunRecord(
                            topic=topic,
                            dataset=dataset,
                            file_format=file_format,
                            table=table,
                            column=column,
                            operation=operation,
                            run=run_idx,
                            total_time=total_time,
                            table_function_time=table_time,
                            other_time=other_time,
                            config_tag=config_tag,
                        )
                    )
        return records

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

    def _load_summary(self, path: Path) -> Optional[Dict[str, object]]:
        try:
            with path.open("r", encoding="utf-8") as fh:
                return json.load(fh)
        except Exception as exc:  # pragma: no cover - defensive
            print(f"Failed to load summary '{path}': {exc}", file=sys.stderr)
            return None

    def _extract_table_time(self, profile_path: Path) -> float:
        with profile_path.open("r", encoding="utf-8") as fh:
            profile = json.load(fh)

        def traverse(nodes: Sequence[Dict[str, object]]) -> float:
            total = 0.0
            for node in nodes:
                total += self._table_operator_timing(node)
                children = node.get("children") or []
                if children:
                    total += traverse(children)  # recursive descent
            return total

        children = profile.get("children") or []
        return traverse(children)

    def _table_operator_timing(self, node: Dict[str, object]) -> float:
        operator_type = str(node.get("operator_type", "")).upper()
        operator_name = str(node.get("operator_name", "")).strip().upper()
        extra = node.get("extra_info") or {}
        function_name = str(extra.get("Function", "")).strip().upper()

        is_table = False
        if operator_type in TABLE_OPERATOR_TYPES:
            is_table = True
        elif operator_name in TABLE_OPERATOR_NAMES:
            is_table = True
        elif any(
            operator_name.startswith(prefix) for prefix in TABLE_OPERATOR_PREFIXES
        ):
            is_table = True
        elif function_name and (
            function_name in TABLE_OPERATOR_NAMES
            or any(
                function_name.startswith(prefix) for prefix in TABLE_OPERATOR_PREFIXES
            )
        ):
            is_table = True

        if not is_table:
            return 0.0
        timing = node.get("operator_timing")
        if timing is None:
            return 0.0
        return float(timing)


@dataclass
class AggregatedMetric:
    topic: str
    dataset: str
    file_format: str
    table: str
    column: str
    operation: str
    count: int
    avg_total: float
    avg_table: float
    avg_other: float


def aggregate_records(
    records: Iterable[RunRecord], group_keys: Sequence[str]
) -> List[AggregatedMetric]:
    buckets: Dict[Tuple[str, ...], Dict[str, float]] = {}
    for record in records:
        key = tuple(getattr(record, key_name) for key_name in group_keys)
        bucket = buckets.setdefault(
            key,
            {
                "count": 0,
                "total_sum": 0.0,
                "table_sum": 0.0,
                "other_sum": 0.0,
            },
        )
        bucket["count"] += 1
        bucket["total_sum"] += record.total_time
        bucket["table_sum"] += record.table_function_time
        bucket["other_sum"] += record.other_time

    metrics: List[AggregatedMetric] = []
    for key, values in buckets.items():
        count = int(values["count"])
        if count == 0:
            continue

        attrs = {
            "topic": "*",
            "dataset": "*",
            "file_format": "*",
            "table": "*",
            "column": "*",
            "operation": "*",
        }
        for name, value in zip(group_keys, key):
            attrs[name] = value

        metrics.append(
            AggregatedMetric(
                topic=str(attrs["topic"]),
                dataset=str(attrs["dataset"]),
                file_format=str(attrs["file_format"]),
                table=str(attrs["table"]),
                column=str(attrs["column"]),
                operation=str(attrs["operation"]),
                count=count,
                avg_total=values["total_sum"] / count,
                avg_table=values["table_sum"] / count,
                avg_other=values["other_sum"] / count,
            )
        )
    return metrics


def create_output_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def lighten_color(color: str, factor: float = 0.6) -> Tuple[float, float, float]:
    base = np.array(mcolors.to_rgb(color))
    return tuple(base + (1.0 - base) * factor)


def slugify(value: str) -> str:
    value = value.strip().lower()
    value = re.sub(r"[^a-z0-9]+", "-", value)
    value = value.strip("-")
    return value or "table"


def plot_avg_duration_by_operation(
    metrics: List[AggregatedMetric],
    output_dir: Path,
    title_suffix: str = "",
) -> None:
    if not metrics:
        return
    formats = sorted({m.file_format for m in metrics})
    operations = sorted({m.operation for m in metrics})
    format_to_color = {
        fmt: plt.get_cmap("tab10")(idx % 10) for idx, fmt in enumerate(formats)
    }

    # Index metrics for quick lookup
    lookup: Dict[Tuple[str, str], AggregatedMetric] = {}
    for metric in metrics:
        lookup[(metric.file_format, metric.operation)] = metric

    fig, ax = plt.subplots(figsize=(max(8, len(operations) * 1.5), 6))
    x = np.arange(len(operations))
    width = 0.8 / max(1, len(formats))

    legend_handles = {}
    for idx, fmt in enumerate(formats):
        table_vals = []
        other_vals = []
        for operation in operations:
            metric = lookup.get((fmt, operation))
            if metric:
                table_vals.append(metric.avg_table)
                other_vals.append(metric.avg_other)
            else:
                table_vals.append(0.0)
                other_vals.append(0.0)
        offsets = x - 0.4 + (idx + 0.5) * width
        base_color = format_to_color[fmt]
        table_color = lighten_color(base_color, 0.0)
        other_color = lighten_color(base_color, 0.55)
        bars_table = ax.bar(
            offsets,
            table_vals,
            width,
            color=table_color,
            label=f"{fmt} table" if fmt not in legend_handles else "",
        )
        bars_other = ax.bar(
            offsets,
            other_vals,
            width,
            bottom=table_vals,
            color=other_color,
            label=f"{fmt} other" if fmt not in legend_handles else "",
        )
        legend_handles[fmt] = (bars_table, bars_other)

    ax.set_ylabel("Average duration (s)")
    ax.set_title(f"Average query duration per operation{title_suffix}")
    ax.set_xticks(x)
    ax.set_xticklabels(operations, rotation=30, ha="right")
    ax.set_ylim(bottom=0)

    legend_entries = []
    legend_labels = []
    for fmt, (bars_table, bars_other) in legend_handles.items():
        legend_entries.append(bars_table)
        legend_labels.append(f"{fmt}: table scan")
        legend_entries.append(bars_other)
        legend_labels.append(f"{fmt}: other operators")
    ax.legend(legend_entries, legend_labels, loc="upper right")
    ax.grid(axis="y", linestyle="--", alpha=0.3)

    create_output_dir(output_dir)
    fig.tight_layout()
    output_path = output_dir / "avg_duration_by_operation.png"
    fig.savefig(output_path, dpi=200)
    plt.close(fig)


def plot_avg_duration_by_dataset(
    metrics: List[AggregatedMetric],
    output_dir: Path,
    title_suffix: str = "",
) -> None:
    if not metrics:
        return
    datasets = sorted({(m.topic, m.dataset) for m in metrics})
    formats = sorted({m.file_format for m in metrics})
    if len(datasets) == 1 and len(formats) == 1:
        return  # nothing comparative to plot

    lookup: Dict[Tuple[str, str, str], AggregatedMetric] = {}
    for metric in metrics:
        lookup[(metric.topic, metric.dataset, metric.file_format)] = metric

    fig, ax = plt.subplots(figsize=(max(8, len(datasets) * 1.8), 6))
    x = np.arange(len(datasets))
    width = 0.8 / max(1, len(formats))
    cmap = plt.get_cmap("tab10")

    legend_handles = {}
    for idx, fmt in enumerate(formats):
        table_vals = []
        other_vals = []
        for topic, dataset in datasets:
            metric = lookup.get((topic, dataset, fmt))
            if metric:
                table_vals.append(metric.avg_table)
                other_vals.append(metric.avg_other)
            else:
                table_vals.append(0.0)
                other_vals.append(0.0)
        offsets = x - 0.4 + (idx + 0.5) * width
        base_color = cmap(idx % 10)
        table_color = lighten_color(base_color, 0.0)
        other_color = lighten_color(base_color, 0.55)
        bars_table = ax.bar(
            offsets,
            table_vals,
            width,
            color=table_color,
            label=f"{fmt} table" if fmt not in legend_handles else "",
        )
        bars_other = ax.bar(
            offsets,
            other_vals,
            width,
            bottom=table_vals,
            color=other_color,
            label=f"{fmt} other" if fmt not in legend_handles else "",
        )
        legend_handles[fmt] = (bars_table, bars_other)

    labels = [f"{topic}/{dataset}" for topic, dataset in datasets]
    ax.set_xticks(x)
    ax.set_xticklabels(labels, rotation=25, ha="right")
    ax.set_ylabel("Average duration (s)")
    ax.set_title(f"Average query duration per dataset{title_suffix}")
    ax.set_ylim(bottom=0)
    ax.grid(axis="y", linestyle="--", alpha=0.3)

    legend_entries = []
    legend_labels = []
    for fmt, (bars_table, bars_other) in legend_handles.items():
        legend_entries.append(bars_table)
        legend_labels.append(f"{fmt}: table scan")
        legend_entries.append(bars_other)
        legend_labels.append(f"{fmt}: other operators")
    ax.legend(legend_entries, legend_labels, loc="upper right")

    create_output_dir(output_dir)
    fig.tight_layout()
    output_path = output_dir / "avg_duration_by_dataset.png"
    fig.savefig(output_path, dpi=200)
    plt.close(fig)


def plot_top_hotspots(
    metrics: List[AggregatedMetric],
    output_dir: Path,
    top_n: int = 12,
    title_suffix: str = "",
) -> None:
    if not metrics:
        return
    # Identify the heaviest (table, column, operation) combinations by total runtime
    key_to_metric: Dict[Tuple[str, str, str], Dict[str, float]] = defaultdict(
        lambda: {"count": 0, "total": 0.0}
    )
    for metric in metrics:
        key = (metric.table, metric.column, metric.operation)
        bucket = key_to_metric[key]
        bucket["count"] += metric.count
        bucket["total"] += metric.avg_total * metric.count

    ranked = sorted(
        ((key, values["total"]) for key, values in key_to_metric.items()),
        key=lambda item: item[1],
        reverse=True,
    )
    top_list = [key for key, _ in ranked[:top_n]]
    if not top_list:
        return
    top_key_set = set(top_list)

    formats = sorted({m.file_format for m in metrics})
    cmap = plt.get_cmap("tab10")

    # Gather data for plotting
    hotspot_metrics: Dict[Tuple[str, str, str], Dict[str, AggregatedMetric]] = (
        defaultdict(dict)
    )
    for metric in metrics:
        key = (metric.table, metric.column, metric.operation)
        if key not in top_key_set:
            continue
        hotspot_metrics[key][metric.file_format] = metric

    labels = [f"{table}.{column}\n{operation}" for table, column, operation in top_list]
    x = np.arange(len(labels))
    width = 0.8 / max(1, len(formats))

    fig, ax = plt.subplots(figsize=(max(10, len(labels) * 1.6), 6))
    legend_handles = {}

    for idx, fmt in enumerate(formats):
        table_vals = []
        other_vals = []
        for key in top_list:
            metric = hotspot_metrics.get(key, {}).get(fmt)
            if metric:
                table_vals.append(metric.avg_table)
                other_vals.append(metric.avg_other)
            else:
                table_vals.append(0.0)
                other_vals.append(0.0)
        offsets = x - 0.4 + (idx + 0.5) * width
        base_color = cmap(idx % 10)
        table_color = lighten_color(base_color, 0.0)
        other_color = lighten_color(base_color, 0.55)
        bars_table = ax.bar(
            offsets,
            table_vals,
            width,
            color=table_color,
            label=f"{fmt} table" if fmt not in legend_handles else "",
        )
        bars_other = ax.bar(
            offsets,
            other_vals,
            width,
            bottom=table_vals,
            color=other_color,
            label=f"{fmt} other" if fmt not in legend_handles else "",
        )
        legend_handles[fmt] = (bars_table, bars_other)

    ax.set_xticks(x)
    ax.set_xticklabels(labels, rotation=25, ha="right")
    ax.set_ylabel("Average duration (s)")
    ax.set_title(f"Top {len(top_list)} hotspots by column/operation{title_suffix}")
    ax.set_ylim(bottom=0)
    ax.grid(axis="y", linestyle="--", alpha=0.3)

    legend_entries = []
    legend_labels = []
    for fmt, (bars_table, bars_other) in legend_handles.items():
        legend_entries.append(bars_table)
        legend_labels.append(f"{fmt}: table scan")
        legend_entries.append(bars_other)
        legend_labels.append(f"{fmt}: other operators")
    ax.legend(legend_entries, legend_labels, loc="upper right")

    create_output_dir(output_dir)
    fig.tight_layout()
    output_path = output_dir / "top_hotspots.png"
    fig.savefig(output_path, dpi=200)
    plt.close(fig)


def plot_per_table_breakdowns(
    metrics: List[AggregatedMetric],
    output_dir: Path,
    title_suffix: str = "",
) -> None:
    if not metrics:
        return

    tables = sorted({metric.table for metric in metrics})
    if not tables:
        return

    formats = sorted({metric.file_format for metric in metrics})
    cmap = plt.get_cmap("tab10")

    per_table_dir = output_dir / "per_table"
    per_table_dir.mkdir(parents=True, exist_ok=True)

    metrics_by_key: Dict[Tuple[str, str, str], Dict[str, AggregatedMetric]] = defaultdict(dict)
    for metric in metrics:
        key = (metric.table, metric.column, metric.operation)
        metrics_by_key[key][metric.file_format] = metric

    def render_chart(
        table: str,
        combos: List[Tuple[str, str]],
        filename_suffix: str,
        title_label: str,
    ) -> None:
        if not combos:
            return

        ordered_combos = sorted(combos, key=lambda pair: (pair[0], pair[1]))
        labels = [f"{column}\n{operation}" for column, operation in ordered_combos]
        x = np.arange(len(labels))
        width = 0.8 / max(1, len(formats))

        fig, ax = plt.subplots(figsize=(max(10, len(labels) * 1.5), 6))
        legend_handles: Dict[str, Tuple[plt.Container, plt.Container]] = {}

        for idx, fmt in enumerate(formats):
            table_vals: List[float] = []
            other_vals: List[float] = []
            for column, operation in ordered_combos:
                metric = metrics_by_key.get((table, column, operation), {}).get(fmt)
                if metric:
                    table_vals.append(metric.avg_table)
                    other_vals.append(metric.avg_other)
                else:
                    table_vals.append(0.0)
                    other_vals.append(0.0)

            offsets = x - 0.4 + (idx + 0.5) * width
            base_color = cmap(idx % 10)
            table_color = lighten_color(base_color, 0.0)
            other_color = lighten_color(base_color, 0.55)

            bars_table = ax.bar(
                offsets,
                table_vals,
                width,
                color=table_color,
                label=f"{fmt} table" if fmt not in legend_handles else "",
            )
            bars_other = ax.bar(
                offsets,
                other_vals,
                width,
                bottom=table_vals,
                color=other_color,
                label=f"{fmt} other" if fmt not in legend_handles else "",
            )
            legend_handles[fmt] = (bars_table, bars_other)

        ax.set_xticks(x)
        ax.set_xticklabels(labels, rotation=30, ha="right")
        ax.set_ylabel("Average duration (s)")
        ax.set_title(title_label)
        ax.set_ylim(bottom=0)
        ax.grid(axis="y", linestyle="--", alpha=0.3)

        legend_entries = []
        legend_labels = []
        for fmt, (bars_table, bars_other) in legend_handles.items():
            legend_entries.append(bars_table)
            legend_labels.append(f"{fmt}: table scan")
            legend_entries.append(bars_other)
            legend_labels.append(f"{fmt}: other operators")
        ax.legend(legend_entries, legend_labels, loc="upper right")

        fig.tight_layout()
        slug = slugify(table)
        filename = f"{slug}{filename_suffix}.png"
        output_path = per_table_dir / filename
        fig.savefig(output_path, dpi=200)
        plt.close(fig)

    for table in tables:
        combos_all = [
            (column, operation)
            for (tbl, column, operation) in metrics_by_key.keys()
            if tbl == table
        ]
        if not combos_all:
            continue

        string_combos = [
            (column, operation)
            for (column, operation) in combos_all
            if operation.lower() == "count_distinct"
        ]
        other_combos = [
            (column, operation)
            for (column, operation) in combos_all
            if operation.lower() != "count_distinct"
        ]

        render_chart(
            table,
            other_combos,
            filename_suffix="",
            title_label=f"{table} column/operation breakdown{title_suffix}",
        )
        render_chart(
            table,
            string_combos,
            filename_suffix="__string",
            title_label=f"{table} string-intensive breakdown{title_suffix}",
        )


def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Plot volumetric benchmark results")
    parser.add_argument(
        "--results-root",
        default=str(RESULTS_SUBDIR),
        help="Root directory containing volumetric benchmark results",
    )
    parser.add_argument(
        "--output-dir",
        default=str(DEFAULT_OUTPUT_DIR),
        help="Directory where plots will be written",
    )
    parser.add_argument(
        "--topics",
        nargs="*",
        help="Optional list of topics to include",
    )
    parser.add_argument(
        "--datasets",
        nargs="*",
        help="Optional list of datasets to include",
    )
    parser.add_argument(
        "--formats",
        nargs="*",
        help="Optional list of file formats to include",
    )
    parser.add_argument(
        "--tables",
        nargs="*",
        help="Optional list of tables to include",
    )
    parser.add_argument(
        "--columns",
        nargs="*",
        help="Optional list of columns to include",
    )
    parser.add_argument(
        "--operations",
        nargs="*",
        help="Optional list of operations (e.g. sum, max) to include",
    )
    parser.add_argument(
        "--top-n-hotspots",
        type=int,
        default=12,
        help="Number of (table, column, operation) hotspots to include in the hotspot plot",
    )
    return parser.parse_args(argv)


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = parse_args(argv)
    results_root = Path(args.results_root).resolve()
    output_dir = Path(args.output_dir).resolve()
    if not results_root.exists():
        print(f"Results root '{results_root}' does not exist", file=sys.stderr)
        return 1

    loader = ResultLoader(results_root)
    records = loader.load(args)
    if not records:
        print("No benchmark records matched the provided filters", file=sys.stderr)
        return 1

    records_by_config: Dict[str, List[RunRecord]] = defaultdict(list)
    for record in records:
        records_by_config[record.config_tag].append(record)

    for config_tag in sorted(records_by_config):
        config_records = records_by_config[config_tag]
        config_output_dir = output_dir / config_tag

        op_metrics = aggregate_records(config_records, ["file_format", "operation"])
        plot_avg_duration_by_operation(
            op_metrics, config_output_dir, title_suffix=f" ({config_tag})"
        )

        dataset_metrics = aggregate_records(
            config_records, ["topic", "dataset", "file_format"]
        )
        plot_avg_duration_by_dataset(
            dataset_metrics, config_output_dir, title_suffix=f" ({config_tag})"
        )

        tco_metrics = aggregate_records(
            config_records, ["table", "column", "operation", "file_format"]
        )
        plot_top_hotspots(
            tco_metrics,
            config_output_dir,
            top_n=args.top_n_hotspots,
            title_suffix=f" ({config_tag})",
        )

        plot_per_table_breakdowns(
            tco_metrics,
            config_output_dir,
            title_suffix=f" ({config_tag})",
        )

        print(f"Plots written to {config_output_dir}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
