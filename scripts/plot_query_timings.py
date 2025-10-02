"""Generate per-query benchmark comparison charts for a given scale factor."""

from __future__ import annotations

import argparse
from pathlib import Path
import sys

import matplotlib.pyplot as plt
import numpy as np

SCRIPT_DIR = Path(__file__).resolve().parent
REPO_ROOT = SCRIPT_DIR.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts.benchmark_parser import average_timings_by_query, load_benchmark_results


def _normalise_scale_factor(raw: str) -> str:
    cleaned = raw.strip().lower()
    if cleaned.startswith("sf"):
        cleaned = cleaned[2:]
    cleaned = cleaned.replace("_", ".")
    try:
        value = float(cleaned)
    except ValueError as exc:  # pragma: no cover - validation guard
        raise ValueError(f"Invalid scale factor: {raw}") from exc
    if value.is_integer():
        return str(int(value))
    return format(value, ".15g")


def _prepare_plot_data(df, scale_factor: str):
    subset = df[df["scale_factor"] == scale_factor]
    if subset.empty:
        available = sorted(df["scale_factor"].unique())
        raise ValueError(
            f"No data for scale factor {scale_factor}. Available scale factors: {', '.join(available)}"
        )

    subset = subset.copy()
    subset["query_num"] = subset["query"].str.extract(r"q(\d+)").astype(int)
    subset = subset.sort_values("query_num").drop(columns=["query_num"])

    pivot = subset.pivot(index="query", columns="engine", values="timing")
    pivot = pivot.reindex(sorted(pivot.index, key=lambda q: int(q[1:]) if q and q[0] == "q" else q))
    return pivot


def plot_query_comparison(pivot, scale_factor: str, title: str | None = None):
    queries = pivot.index.tolist()
    engines = list(pivot.columns)

    x_positions = np.arange(len(queries))
    bar_width = 0.8 / max(len(engines), 1)

    fig, ax = plt.subplots(figsize=(12, 6))

    for idx, engine in enumerate(engines):
        offsets = x_positions + (idx - (len(engines) - 1) / 2) * bar_width
        ax.bar(offsets, pivot[engine].values, bar_width, label=engine)

    ax.set_xticks(x_positions)
    ax.set_xticklabels(queries, rotation=45)
    ax.set_ylabel("Average runtime (s)")
    ax.set_xlabel("Query")
    ax.set_title(title or f"Benchmark comparison for scale factor {scale_factor}")
    ax.legend()
    ax.grid(axis="y", linestyle="--", linewidth=0.5, alpha=0.6)
    fig.tight_layout()
    return fig, ax


def _run_cli():
    parser = argparse.ArgumentParser(description="Plot per-query benchmark comparisons for a scale factor")
    parser.add_argument(
        "--scale-factor",
        required=True,
        help="Scale factor to plot (accepts values like 0.1, sf0.1, 10, or 10).",
    )
    parser.add_argument(
        "--results-dir",
        default=Path("benchmark/results/tpch"),
        type=Path,
        help="Directory containing benchmark CSV results (default: benchmark/results/tpch).",
    )
    parser.add_argument(
        "--output",
        type=Path,
        help="Optional path to save the plot image (png). Defaults to benchmark/results/tpch/plots.",
    )
    parser.add_argument(
        "--title",
        help="Optional custom title for the plot.",
    )
    args = parser.parse_args()

    scale_factor = _normalise_scale_factor(args.scale_factor)
    df = average_timings_by_query(load_benchmark_results(args.results_dir))
    pivot = _prepare_plot_data(df, scale_factor)

    title = args.title or f"Benchmark comparison for scale factor {scale_factor}"
    fig, _ = plot_query_comparison(pivot, scale_factor, title)

    if args.output:
        output_path = args.output
    else:
        default_dir = Path("benchmark/results/tpch/plots")
        default_dir.mkdir(parents=True, exist_ok=True)
        safe_scale = scale_factor.replace(".", "_")
        output_path = default_dir / f"tpch_sf{safe_scale}_comparison.png"

    fig.savefig(output_path, dpi=200)
    print(f"Saved plot to {output_path}")


if __name__ == "__main__":  # pragma: no cover - CLI entry point
    _run_cli()
