from __future__ import annotations

import argparse
import shutil
import sys
from pathlib import Path

import duckdb

SCRIPT_DIR = Path(__file__).resolve().parent
DATA_GENERATOR_DIR = SCRIPT_DIR.parent
PROJECT_ROOT = DATA_GENERATOR_DIR.parent.parent

if str(DATA_GENERATOR_DIR) not in sys.path:
    sys.path.insert(0, str(DATA_GENERATOR_DIR))

from tpch_utils import TPCH_TABLES, parse_scale_list  # noqa: E402

DATA_DIR = PROJECT_ROOT / "benchmark" / "data" / "generated"
BUILD_DIR = PROJECT_ROOT / "build" / "release"
FASTLANES_EXTENSION = BUILD_DIR / "extension/fastlanes/fastlanes.duckdb_extension"


def resolve_fastlanes_extension() -> Path:
    if not FASTLANES_EXTENSION.exists():
        print(f"Error: fastlanes extension not found at {FASTLANES_EXTENSION}")
        print("Make sure to build the project first with 'make'")
        sys.exit(1)
    return FASTLANES_EXTENSION


def convert_scale(scale, extension_path: Path) -> None:
    print(f"Writing FastLanes files for {scale.display_name}")

    duckdb_path = DATA_DIR / "duckdb" / f"{scale.dataset_name}.duckdb"
    fls_dir = DATA_DIR / "fls" / scale.dataset_name

    if not duckdb_path.exists():
        raise FileNotFoundError(
            f"DuckDB database not found for {scale.display_name} at {duckdb_path}"
        )

    if fls_dir.exists():
        shutil.rmtree(fls_dir)
    fls_dir.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect(config={"allow_unsigned_extensions": "true"})
    try:
        con.load_extension(str(extension_path))
        attached = False
        try:
            con.execute("ATTACH ? AS tpch_src (READ_ONLY);", [str(duckdb_path)])
            attached = True
            for table in TPCH_TABLES:
                fls_path = fls_dir / f"{table}.fls"
                print(f"  - {table}")
                con.execute(
                    f"COPY (SELECT * FROM tpch_src.{table}) TO ?;",
                    [str(fls_path)],
                )
        finally:
            if attached:
                con.execute("DETACH tpch_src;")
    finally:
        con.close()


def parse_args():
    parser = argparse.ArgumentParser(
        description="Convert CSV data to FastLanes format for the requested scale factors"
    )
    parser.add_argument(
        "--scale-factors",
        "-s",
        nargs="+",
        help="List of scale factors to convert (e.g. 0.01 1 10)",
    )
    parser.add_argument(
        "--sf",
        dest="scale_factors_repeat",
        action="append",
        help="Scale factor to convert (can be repeated)",
    )
    return parser.parse_args()


def main():
    args = parse_args()
    factors: list[str] = []
    if args.scale_factors:
        factors.extend(args.scale_factors)
    if args.scale_factors_repeat:
        factors.extend(args.scale_factors_repeat)

    scales = parse_scale_list(factors or None, default=["1"])
    extension_path = resolve_fastlanes_extension()

    for scale in scales:
        convert_scale(scale, extension_path)


if __name__ == "__main__":
    main()
