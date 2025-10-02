"""Utilities for loading and transforming benchmark result files."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Iterable
import pandas as pd


_RESULTS_PATTERN = "tpch-sf"


def _parse_metadata(path: Path) -> tuple[str, str, str]:
    """Return (benchmark, scale_factor, engine) parsed from a results filename."""
    stem = path.stem  # e.g. "tpch-sf0_1-duckdb"
    try:
        benchmark_part, remainder = stem.split("-sf", maxsplit=1)
        scale_part, engine = remainder.rsplit("-", maxsplit=1)
    except ValueError as exc:  # pragma: no cover - defensive guard
        raise ValueError(f"Cannot parse metadata from filename: {path.name}") from exc
    scale_factor = scale_part.replace("_", ".")
    return benchmark_part, scale_factor, engine


def _extract_query_names(names: Iterable[str]) -> pd.Series:
    """Extract short query identifiers like q01 from a name column."""
    series = pd.Series(list(names), dtype="string")
    queries = series.str.extract(r"(q\d+)\.benchmark", expand=False)
    return queries


def load_benchmark_results(results_dir: Path | str) -> pd.DataFrame:
    """Load all benchmark result CSVs and return a unified DataFrame."""
    results_path = Path(results_dir)
    if not results_path.exists():
        raise FileNotFoundError(f"Results directory not found: {results_dir}")

    frames: list[pd.DataFrame] = []
    for path in sorted(results_path.glob("*.csv")):
        try:
            df = pd.read_csv(path, sep="\t")
        except pd.errors.EmptyDataError:
            print(f"Skipping {path.name}: file is empty", file=sys.stderr)
            continue
        except pd.errors.ParserError as exc:
            print(f"Skipping {path.name}: {exc}", file=sys.stderr)
            continue

        if "timing" not in df.columns:
            print(f"Skipping {path.name}: missing 'timing' column", file=sys.stderr)
            continue

        benchmark, scale_factor, engine = _parse_metadata(path)
        df = df.copy()

        if "name" in df.columns:
            df["query"] = _extract_query_names(df["name"])
        elif "query" in df.columns:
            df["query"] = df["query"].astype("string")
        else:
            print(
                f"Skipping {path.name}: neither 'name' nor 'query' column is present",
                file=sys.stderr,
            )
            continue

        if "benchmark" not in df.columns:
            df["benchmark"] = benchmark
        if "scale_factor" not in df.columns:
            df["scale_factor"] = scale_factor
        if "engine" not in df.columns:
            df["engine"] = engine

        frames.append(df)

    if not frames:
        raise FileNotFoundError(f"No CSV result files found in: {results_dir}")

    return pd.concat(frames, ignore_index=True)


def average_timings_by_query(df: pd.DataFrame) -> pd.DataFrame:
    """Return mean timing grouped by benchmark, scale factor, engine, and query."""
    required_cols = {"benchmark", "scale_factor", "engine", "query", "timing"}
    missing = required_cols.difference(df.columns)
    if missing:
        missing_cols = ", ".join(sorted(missing))
        raise ValueError(f"DataFrame missing required columns: {missing_cols}")

    clean = df.dropna(subset=["query"])
    grouped = clean.groupby(
        ["benchmark", "scale_factor", "engine", "query"], as_index=False
    )["timing"].mean()
    return grouped


def _run_cli() -> None:
    parser = argparse.ArgumentParser(
        description="Load DuckDB Fastlanes benchmark results into a DataFrame"
    )
    parser.add_argument(
        "--results-dir",
        default=Path("benchmark/results/tpch"),
        type=Path,
        help="Directory containing benchmark result CSV files (default: benchmark/results)",
    )
    parser.add_argument(
        "--aggregate",
        action="store_true",
        help="Aggregate average timings per query instead of per-run records.",
    )
    parser.add_argument(
        "--output",
        type=Path,
        help="Optional path to write the resulting data as CSV (defaults to stdout).",
    )
    args = parser.parse_args()

    df = load_benchmark_results(args.results_dir)
    if args.aggregate:
        df = average_timings_by_query(df)

    if args.output:
        df.to_csv(args.output, index=False)
    else:
        df.to_csv(sys.stdout, index=False)


if __name__ == "__main__":  # pragma: no cover - CLI entry point
    _run_cli()
