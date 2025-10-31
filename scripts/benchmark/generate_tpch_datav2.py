#!/usr/bin/env python3
"""
Generate TPC-H benchmark data for the datav2 layout.

For every combination of scale factor and row group size this script:
  * runs DuckDB's `dbgen` to materialize the dataset
  * exports each TPC-H table to the requested file formats with a flat file layout
  * records file metadata inside a DuckDB database for easy discovery
"""

from __future__ import annotations

import argparse
import shutil
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import List, Sequence, Set, Tuple

import duckdb


SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent.parent
BENCHMARK_ROOT = PROJECT_ROOT / "benchmark" / "datav2"

# Ensure we can import the shared TPCH utilities
SCRIPTS_PATH = PROJECT_ROOT / "scripts"
if str(SCRIPTS_PATH) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_PATH))

from data_generator.tpch_utils import (  # noqa: E402
    TPCH_TABLES,
    ScaleInfo,
    parse_scale_list,
)


DEFAULT_SCALE_FACTORS = ["1", "10", "30"]
DEFAULT_ROW_GROUP_SIZES = [64 * 1024, 128 * 1024, 256 * 1024]
DEFAULT_FILE_FORMATS = ["parquet", "fls", "vortex", "duckdb"]

BUILD_DIR = PROJECT_ROOT / "build" / "release"
FASTLANES_EXTENSION = (
    BUILD_DIR / "extension" / "fastlanes" / "fastlanes.duckdb_extension"
)
VORTEX_EXTENSION = (
    PROJECT_ROOT
    / "build"
    / "duckdb-vortex"
    / "build"
    / "release"
    / "extension"
    / "vortex"
    / "vortex.duckdb_extension"
)


@dataclass(frozen=True)
class ExportTask:
    """Represents a single export operation for metadata and execution."""

    benchmark: str
    scale_factor: str
    row_group_size: int
    table_name: str
    file_format: str
    output_path: Path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate benchmark data for the simplified datav2 layout."
    )
    parser.add_argument(
        "--scale-factors",
        "-s",
        nargs="+",
        default=None,
        help="Scale factors to generate (defaults to 1, 10, 30).",
    )
    parser.add_argument(
        "--row-group-sizes",
        "-r",
        nargs="+",
        type=int,
        default=None,
        help="Row group sizes (defaults to 64, 128, 256).",
    )
    parser.add_argument(
        "--file-formats",
        "-f",
        nargs="+",
        default=None,
        help="File formats to export (defaults to parquet and fls).",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=BENCHMARK_ROOT,
        help="Destination directory for generated data (defaults to benchmark/datav2).",
    )
    parser.add_argument(
        "--metadata",
        type=Path,
        default=None,
        help="Path to the DuckDB metadata database (defaults to <output-dir>/metadata.duckdb).",
    )
    parser.add_argument(
        "--clean",
        action="store_true",
        help="Remove existing generated files before writing new ones.",
    )
    return parser.parse_args()


def resolve_fastlanes_extension() -> Path:
    if FASTLANES_EXTENSION.exists():
        return FASTLANES_EXTENSION
    raise FileNotFoundError(
        f"fastlanes extension not found at {FASTLANES_EXTENSION}. "
        "Please build the extension with 'make' before running this script."
    )


def resolve_vortex_extension() -> Path:
    if VORTEX_EXTENSION.exists():
        return VORTEX_EXTENSION
    raise FileNotFoundError(
        f"vortex extension not found at {VORTEX_EXTENSION}. "
        "Please build the vortex extension with 'make vortex-extension' before running this script."
    )


def prepare_output_paths(tasks: Sequence[ExportTask]) -> None:
    seen: Set[Path] = set()
    for task in tasks:
        if task.output_path in seen:
            continue
        seen.add(task.output_path)
        task.output_path.parent.mkdir(parents=True, exist_ok=True)
        if task.output_path.exists():
            if task.output_path.is_dir():
                shutil.rmtree(task.output_path)
            else:
                task.output_path.unlink()


def escape_path(path: Path) -> str:
    return str(path).replace("'", "''")


def required_extensions(file_formats: Sequence[str]) -> List[Path]:
    extensions: List[Path] = []
    if any(fmt.lower() == "fls" for fmt in file_formats):
        extensions.append(resolve_fastlanes_extension())
    if any(fmt.lower() == "vortex" for fmt in file_formats):
        extensions.append(resolve_vortex_extension())
    return extensions


def connect_duckdb(file_formats: Sequence[str]) -> duckdb.DuckDBPyConnection:
    con = duckdb.connect(config={"allow_unsigned_extensions": "true"})
    for extension in required_extensions(file_formats):
        con.load_extension(str(extension))
    return con


def create_export_tasks(
    benchmark: str,
    scale_infos: Sequence[ScaleInfo],
    row_group_sizes: Sequence[int],
    file_formats: Sequence[str],
    output_dir: Path,
) -> List[ExportTask]:
    tasks: List[ExportTask] = []
    for scale in scale_infos:
        for row_group_size in row_group_sizes:
            if row_group_size <= 0:
                raise ValueError("Row group size must be positive")
            for table in TPCH_TABLES:
                for file_format in file_formats:
                    suffix = file_extension(file_format)
                    if file_format.lower() == "duckdb":
                        filename = (
                            f"{benchmark}_sf{scale.slug}_rg{row_group_size}.{suffix}"
                        )
                    else:
                        filename = f"{benchmark}_sf{scale.slug}_rg{row_group_size}_{table}.{suffix}"
                    format_dir = output_dir / file_format.lower()
                    tasks.append(
                        ExportTask(
                            benchmark=benchmark,
                            scale_factor=scale.normalized,
                            row_group_size=row_group_size,
                            table_name=table,
                            file_format=file_format.lower(),
                            output_path=format_dir / filename,
                        )
                    )
    return tasks


def file_extension(file_format: str) -> str:
    file_format_lower = file_format.lower()
    if file_format_lower == "parquet":
        return "parquet"
    if file_format_lower == "fls":
        return "fls"
    if file_format_lower == "csv":
        return "csv"
    if file_format_lower == "vortex":
        return "vortex"
    if file_format_lower == "duckdb":
        return "duckdb"
    raise ValueError(f"Unsupported file format '{file_format}'")


def copy_table(con: duckdb.DuckDBPyConnection, task: ExportTask) -> None:
    options: List[str] = []
    if task.file_format in {"parquet", "fls"}:
        options.append(f"ROW_GROUP_SIZE {task.row_group_size}")

    option_clause = ""
    if options:
        option_clause = ", " + ", ".join(options)

    con.execute(
        f"COPY {task.table_name} TO '{escape_path(task.output_path)}' "
        f"(FORMAT {task.file_format.upper()}{option_clause});"
    )


def export_duckdb_database(
    con: duckdb.DuckDBPyConnection, output_path: Path, row_group_size: int
) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    if output_path.exists():
        output_path.unlink()
    con.execute(
        f"ATTACH '{escape_path(output_path)}' AS persistent (ROW_GROUP_SIZE {row_group_size}, STORAGE_VERSION 'v1.2.0');"
    )
    try:
        con.execute("COPY FROM DATABASE memory TO persistent;")
    finally:
        con.execute("DETACH persistent;")


def run_exports(
    benchmark: str,
    scales: Sequence[str],
    row_group_sizes: Sequence[int],
    file_formats: Sequence[str],
    output_dir: Path,
    clean: bool,
) -> List[ExportTask]:
    normalized_formats = [fmt.lower() for fmt in file_formats]
    scale_infos = parse_scale_list(scales)
    tasks = create_export_tasks(
        benchmark=benchmark,
        scale_infos=scale_infos,
        row_group_sizes=row_group_sizes,
        file_formats=normalized_formats,
        output_dir=output_dir,
    )

    if clean and output_dir.exists():
        for child in output_dir.iterdir():
            if child.is_dir():
                shutil.rmtree(child)
            else:
                child.unlink()
    output_dir.mkdir(parents=True, exist_ok=True)

    prepare_output_paths(tasks)

    for scale in scale_infos:
        con = connect_duckdb(file_formats=normalized_formats)
        try:
            sf_value = float(scale.normalized)
            con.execute("CALL dbgen(sf=?);", [sf_value])
            for row_group_size in row_group_sizes:
                relevant = [
                    task
                    for task in tasks
                    if task.scale_factor == scale.normalized
                    and task.row_group_size == row_group_size
                ]
                if not relevant:
                    continue
                formats = {task.file_format for task in relevant}
                for file_format in formats:
                    format_tasks = [
                        task for task in relevant if task.file_format == file_format
                    ]
                    if not format_tasks:
                        continue
                    if file_format == "duckdb":
                        export_duckdb_database(
                            con, format_tasks[0].output_path, row_group_size
                        )
                    else:
                        for task in format_tasks:
                            copy_table(con, task)
        finally:
            con.close()

    return tasks


def write_metadata(metadata_path: Path, tasks: Sequence[ExportTask]) -> None:
    metadata_path.parent.mkdir(parents=True, exist_ok=True)
    with duckdb.connect(str(metadata_path)) as con:
        con.execute(
            """
            CREATE TABLE IF NOT EXISTS files (
                benchmark VARCHAR,
                sf VARCHAR,
                rowgroup_size INTEGER,
                table_name VARCHAR,
                file_format VARCHAR,
                path VARCHAR
            );
            """
        )
        con.execute("DELETE FROM files;")
        if tasks:
            records: List[Tuple[str, str, int, str, str, str]] = []
            for task in tasks:
                try:
                    relative_path = task.output_path.relative_to(metadata_path.parent)
                    path_string = str(relative_path)
                except ValueError:
                    path_string = str(task.output_path)
                records.append(
                    (
                        task.benchmark,
                        task.scale_factor,
                        task.row_group_size,
                        task.table_name,
                        task.file_format,
                        path_string,
                    )
                )
            con.executemany(
                "INSERT INTO files VALUES (?, ?, ?, ?, ?, ?);",
                records,
            )


def main() -> None:
    args = parse_args()

    scale_factors = args.scale_factors or DEFAULT_SCALE_FACTORS
    row_group_sizes = args.row_group_sizes or DEFAULT_ROW_GROUP_SIZES
    file_formats = args.file_formats or DEFAULT_FILE_FORMATS

    output_dir = args.output_dir.resolve()
    metadata_path = (args.metadata or (output_dir / "metadata.duckdb")).resolve()

    tasks = run_exports(
        benchmark="tpch",
        scales=scale_factors,
        row_group_sizes=row_group_sizes,
        file_formats=file_formats,
        output_dir=output_dir,
        clean=args.clean,
    )
    write_metadata(metadata_path, tasks)

    print(f"Generated {len(tasks)} files.")
    print(f"Metadata stored in {metadata_path}")


if __name__ == "__main__":
    main()
