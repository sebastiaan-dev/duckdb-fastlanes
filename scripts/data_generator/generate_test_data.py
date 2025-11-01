from __future__ import annotations

import argparse
import shutil
import sys
from pathlib import Path

import duckdb

from tpch_utils import parse_scale_list


BASE_PATH = (
    Path(__file__).resolve().parent.parent.parent / "benchmark" / "data" / "tpch"
)

SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent.parent
BUILD_DIR = PROJECT_ROOT / "build" / "release"
FASTLANES_EXTENSION = BUILD_DIR / "extension/fastlanes/fastlanes.duckdb_extension"
# TODO: Load git repo and build automatically
VORTEX_EXTENSION = (
    Path("/Users/sebastiaan/repos/duckdb-vortex")
    / "build"
    / "release"
    / "extension/vortex/vortex.duckdb_extension"
)


def resolve_fastlanes_extension() -> Path:
    if not FASTLANES_EXTENSION.exists():
        print(f"Error: fastlanes extension not found at {FASTLANES_EXTENSION}")
        print("Make sure to build the project first with 'make'")
        sys.exit(1)
    return FASTLANES_EXTENSION


def resolve_vortex_extension() -> Path:
    if not VORTEX_EXTENSION.exists():
        print(f"Error: vortex extension not found at {VORTEX_EXTENSION}")
        print("Make sure to build the project first with 'make'")
        sys.exit(1)
    return VORTEX_EXTENSION


def ensure_directories_exist() -> None:
    for subdir in ("parquet", "csv", "duckdb", "vortex", "fls"):
        target = BASE_PATH / subdir
        target.mkdir(parents=True, exist_ok=True)


def remove_if_exists(path: Path) -> None:
    if path.is_dir():
        shutil.rmtree(path)
    elif path.exists():
        path.unlink()


def export_tpch_scale(scale, row_group_size: int | None = None) -> None:
    suffix = ""
    if row_group_size is not None:
        suffix = f"_rg{row_group_size}"
    dataset_name = f"{scale.dataset_name}{suffix}"
    parquet_path = BASE_PATH / "parquet" / dataset_name
    csv_path = BASE_PATH / "csv" / dataset_name
    fls_path = BASE_PATH / "fls" / dataset_name
    vortex_path = BASE_PATH / "vortex" / dataset_name
    duckdb_path = BASE_PATH / "duckdb" / f"{dataset_name}.duckdb"

    print(f"Generating TPC-H {scale.display_name} -> dataset '{dataset_name}'")

    remove_if_exists(parquet_path)
    remove_if_exists(csv_path)
    remove_if_exists(fls_path)
    remove_if_exists(vortex_path)
    remove_if_exists(duckdb_path)

    parquet_path.parent.mkdir(parents=True, exist_ok=True)
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    vortex_path.parent.mkdir(parents=True, exist_ok=True)
    fls_path.parent.mkdir(parents=True, exist_ok=True)
    duckdb_path.parent.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect(config={"allow_unsigned_extensions": "true"})
    con.load_extension(str(resolve_fastlanes_extension()))
    con.load_extension(str(resolve_vortex_extension()))

    try:
        con.execute(f"CALL dbgen(sf={scale.normalized});")
        parquet_options = "FORMAT parquet"
        if row_group_size is not None:
            parquet_options += f", ROW_GROUP_SIZE {row_group_size}"
        con.execute(f"EXPORT DATABASE '{parquet_path}' ({parquet_options});")

        vortex_options = "FORMAT vortex"
        con.execute(f"EXPORT DATABASE '{vortex_path}' ({vortex_options});")

        fls_options = "FORMAT fls"
        if row_group_size is not None:
            fls_options += f", ROW_GROUP_SIZE {row_group_size}"
        con.execute(f"EXPORT DATABASE '{fls_path}' ({fls_options});")
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
    parser = argparse.ArgumentParser(
        description="Generate TPC-H datasets for the requested scale factors"
    )
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

    row_group_group = parser.add_mutually_exclusive_group()
    row_group_group.add_argument(
        "--row-group-size",
        type=int,
        help="Explicit row group size to use when exporting data",
    )
    row_group_group.add_argument(
        "--row-group-vectors",
        type=int,
        help="Number of vectors per row group (row group size = vectors * 1024)",
    )

    args = parser.parse_args()

    if args.row_group_size is not None and args.row_group_size <= 0:
        parser.error("--row-group-size must be a positive integer")
    if args.row_group_vectors is not None and args.row_group_vectors <= 0:
        parser.error("--row-group-vectors must be a positive integer")

    return args


def main():
    args = parse_args()
    factors = []
    if args.scale_factors:
        factors.extend(args.scale_factors)
    if args.scale_factors_repeat:
        factors.extend(args.scale_factors_repeat)

    scales = parse_scale_list(factors or None, default=["1"])

    row_group_size = args.row_group_size
    if args.row_group_vectors is not None:
        row_group_size = args.row_group_vectors * 1024

    ensure_directories_exist()
    for scale in scales:
        export_tpch_scale(scale, row_group_size=row_group_size)


if __name__ == "__main__":
    main()
