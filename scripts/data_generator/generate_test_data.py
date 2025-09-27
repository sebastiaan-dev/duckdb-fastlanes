from __future__ import annotations

import argparse
import shutil
from pathlib import Path

import duckdb

from tpch_utils import parse_scale_list


BASE_PATH = Path(__file__).resolve().parent.parent.parent / "data" / "generated"


def ensure_directories_exist() -> None:
    for subdir in ("parquet", "csv", "duckdb"):
        target = BASE_PATH / subdir
        target.mkdir(parents=True, exist_ok=True)


def remove_if_exists(path: Path) -> None:
    if path.is_dir():
        shutil.rmtree(path)
    elif path.exists():
        path.unlink()


def export_tpch_scale(scale) -> None:
    dataset_name = scale.dataset_name
    parquet_path = BASE_PATH / "parquet" / dataset_name
    csv_path = BASE_PATH / "csv" / dataset_name
    duckdb_path = BASE_PATH / "duckdb" / f"{dataset_name}.duckdb"

    print(f"Generating TPC-H {scale.display_name}")

    remove_if_exists(parquet_path)
    remove_if_exists(csv_path)
    remove_if_exists(duckdb_path)

    parquet_path.parent.mkdir(parents=True, exist_ok=True)
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    duckdb_path.parent.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect()
    try:
        con.execute(f"CALL dbgen(sf={scale.normalized});")
        con.execute(f"EXPORT DATABASE '{parquet_path}' (FORMAT parquet);")
        con.execute(
            "EXPORT DATABASE '{}' (FORMAT csv, delimiter '|', header false, new_line '\n');".format(
                csv_path
            )
        )
        con.execute(f"ATTACH '{duckdb_path}' AS persistent;")
        con.execute("COPY FROM DATABASE memory TO persistent;")
        con.execute("DETACH persistent;")
    finally:
        con.close()


def parse_args():
    parser = argparse.ArgumentParser(description="Generate TPC-H datasets for the requested scale factors")
    parser.add_argument(
        "--scale-factors",
        "-s",
        nargs="+",
        help="List of scale factors to generate (e.g. 0.01 1 10)",
    )
    parser.add_argument(
        "--sf",
        dest="scale_factors_repeat",
        action="append",
        help="Scale factor to generate (can be repeated)",
    )
    return parser.parse_args()


def main():
    args = parse_args()
    factors = []
    if args.scale_factors:
        factors.extend(args.scale_factors)
    if args.scale_factors_repeat:
        factors.extend(args.scale_factors_repeat)

    scales = parse_scale_list(factors or None, default=["1"])

    ensure_directories_exist()
    for scale in scales:
        export_tpch_scale(scale)


if __name__ == "__main__":
    main()
