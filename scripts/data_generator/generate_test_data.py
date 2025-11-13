from __future__ import annotations

import argparse
import shutil
import subprocess
import sys
from pathlib import Path

from tpch_utils import parse_scale_list


BASE_PATH = (
    Path(__file__).resolve().parent.parent.parent / "benchmark" / "data" / "tpch"
)

SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent.parent
BUILD_DIR = PROJECT_ROOT / "build" / "release"
DUCKDB_BINARY = BUILD_DIR / "duckdb"
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


def resolve_duckdb_binary() -> Path:
    if not DUCKDB_BINARY.exists():
        print(f"Error: duckdb binary not found at {DUCKDB_BINARY}")
        print("Make sure to build the project first with 'make'")
        sys.exit(1)
    if not DUCKDB_BINARY.is_file():
        print(f"Error: expected duckdb binary at {DUCKDB_BINARY}, but found something else")
        sys.exit(1)
    return DUCKDB_BINARY


def sql_literal(value: str | Path) -> str:
    text = str(value)
    return "'" + text.replace("'", "''") + "'"


DUCKDB_CLI_FLAGS: tuple[str, ...] = ("-unsigned", "-bail")


def run_duckdb_sql(sql: str) -> None:
    duckdb_binary = resolve_duckdb_binary()
    args = [str(duckdb_binary), *DUCKDB_CLI_FLAGS, ":memory:"]
    try:
        subprocess.run(
            args,
            input=sql,
            text=True,
            check=True,
            capture_output=True,
        )
    except subprocess.CalledProcessError as exc:
        print("Error: duckdb execution failed")
        if exc.stdout:
            print(exc.stdout)
        if exc.stderr:
            print(exc.stderr, file=sys.stderr)
        sys.exit(exc.returncode)


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

    parquet_options = "FORMAT parquet"
    if row_group_size is not None:
        parquet_options += f", ROW_GROUP_SIZE {row_group_size}"

    fls_options = "FORMAT fls"
    if row_group_size is not None:
        fls_options += f", ROW_GROUP_SIZE {row_group_size}"

    fastlanes_extension = sql_literal(resolve_fastlanes_extension())
    vortex_extension = sql_literal(resolve_vortex_extension())
    parquet_literal = sql_literal(parquet_path)
    vortex_literal = sql_literal(vortex_path)
    fls_literal = sql_literal(fls_path)
    csv_literal = sql_literal(csv_path)
    duckdb_literal = sql_literal(duckdb_path)

    sql_commands = [
        f"LOAD {fastlanes_extension};",
        f"LOAD {vortex_extension};",
        f"CALL dbgen(sf={scale.normalized});",
        f"EXPORT DATABASE {parquet_literal} ({parquet_options});",
        f"EXPORT DATABASE {vortex_literal} (FORMAT vortex);",
        f"EXPORT DATABASE {fls_literal} ({fls_options});",
        (
            f"EXPORT DATABASE {csv_literal} "
            "(FORMAT csv, delimiter '|', header false, new_line '\n');"
        ),
        f"ATTACH {duckdb_literal} AS persistent;",
        "COPY FROM DATABASE memory TO persistent;",
        "DETACH persistent;",
    ]

    run_duckdb_sql("\n".join(sql_commands))


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
