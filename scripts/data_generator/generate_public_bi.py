#!/usr/bin/env python
"""Download and prepare Public BI benchmark datasets.

This script fetches compressed CSV data from the Public BI benchmark site,
materialises the decompressed CSV files under `benchmark/data/public-bi/csv`,
and writes equivalent Parquet, FastLanes, and DuckDB representations so the
volumetric benchmark runner can operate on the new topic.
"""

from __future__ import annotations

import argparse
import bz2
import shutil
import sys
from html.parser import HTMLParser
from pathlib import Path
from typing import Iterable, List, Optional, Tuple
from urllib.error import HTTPError, URLError
from urllib.parse import urljoin
from urllib.request import urlopen

import duckdb

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
DATA_ROOT = PROJECT_ROOT / "benchmark" / "data" / "public-bi"
CSV_ROOT = DATA_ROOT / "csv"
PARQUET_ROOT = DATA_ROOT / "parquet"
FLS_ROOT = DATA_ROOT / "fls"
DUCKDB_ROOT = DATA_ROOT / "duckdb"

PUBLIC_BI_BASE = "https://event.cwi.nl/da/PublicBIbenchmark/"
PUBLIC_BI_SCHEMA_BASE = (
    "https://raw.githubusercontent.com/cwida/public_bi_benchmark/master/benchmark/"
)

FASTLANES_EXTENSION_PATH = (
    PROJECT_ROOT
    / "build"
    / "release"
    / "extension"
    / "fastlanes"
    / "fastlanes.duckdb_extension"
)


class LinkCollector(HTMLParser):
    """Simple anchor tag collector."""

    def __init__(self) -> None:
        super().__init__()
        self.links: List[str] = []

    def handle_starttag(self, tag: str, attrs: List[tuple[str, str]]) -> None:
        if tag.lower() != "a":
            return
        attr_map = dict(attrs)
        href = attr_map.get("href")
        if href:
            self.links.append(href)


def parse_links(url: str) -> List[str]:
    with urlopen(url) as response:  # nosec - trusted domain provided by user
        content = response.read().decode("utf-8", errors="replace")
    parser = LinkCollector()
    parser.feed(content)
    return parser.links


def discover_datasets() -> List[str]:
    links = parse_links(PUBLIC_BI_BASE)
    datasets: List[str] = []
    for link in links:
        if not link.endswith("/"):
            continue
        name = link.rstrip("/")
        if name in {"..", "."}:
            continue
        datasets.append(name)
    datasets.sort()
    return datasets


def discover_tables(dataset: str) -> List[Tuple[str, str]]:
    dataset_url = urljoin(PUBLIC_BI_BASE, f"{dataset}/")
    links = parse_links(dataset_url)
    tables: List[str] = []
    for link in links:
        if not link.lower().endswith(".csv.bz2"):
            continue
        filename = link.rsplit("/", 1)[-1]
        if not filename.lower().endswith(".csv.bz2"):
            continue
        table_name = filename[:-len(".csv.bz2")]
        tables.append((table_name, urljoin(dataset_url, link)))
    tables.sort(key=lambda item: item[0])
    return tables


def download_to_csv(url: str, output_path: Path, force: bool = False) -> bool:
    if output_path.exists() and not force:
        return False

    output_path.parent.mkdir(parents=True, exist_ok=True)
    decompressor = bz2.BZ2Decompressor()
    with urlopen(url) as response, output_path.open("wb") as out_file:  # nosec
        while True:
            chunk = response.read(1024 * 1024)
            if not chunk:
                break
            data = decompressor.decompress(chunk)
            if data:
                out_file.write(data)
    return True


def download_schema(dataset: str, table_name: str) -> str:
    schema_url = urljoin(
        PUBLIC_BI_SCHEMA_BASE,
        f"{dataset}/tables/{table_name}.table.sql",
    )
    with urlopen(schema_url) as response:  # nosec - trusted domain
        return response.read().decode("utf-8")


def escape_literal(value: str) -> str:
    return value.replace("'", "''")


def quote_identifier(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'


def execute_sql_script(conn: duckdb.DuckDBPyConnection, script: str) -> None:
    statements = [segment.strip() for segment in script.split(";")]
    for statement in statements:
        if not statement:
            continue
        conn.execute(statement)


def ensure_fastlanes(conn: duckdb.DuckDBPyConnection) -> None:
    if FASTLANES_EXTENSION_PATH.exists():
        try:
            conn.load_extension(str(FASTLANES_EXTENSION_PATH))
            return
        except duckdb.Error:
            pass
    try:
        conn.execute("LOAD fastlanes")
    except duckdb.Error as err:
        raise RuntimeError(
            "Failed to load FastLanes extension. Ensure it is built or installed."
        ) from err


def process_dataset(dataset: str, overwrite: bool, force_download: bool) -> None:
    tables_with_urls = discover_tables(dataset)
    if not tables_with_urls:
        print(f"No tables found for dataset '{dataset}', skipping")
        return

    csv_dir = CSV_ROOT / dataset
    parquet_dir = PARQUET_ROOT / dataset
    fls_dir = FLS_ROOT / dataset
    duckdb_path = DUCKDB_ROOT / f"{dataset}.duckdb"

    if overwrite:
        shutil.rmtree(csv_dir, ignore_errors=True)
        shutil.rmtree(parquet_dir, ignore_errors=True)
        shutil.rmtree(fls_dir, ignore_errors=True)
        if duckdb_path.exists():
            duckdb_path.unlink()

    csv_dir.mkdir(parents=True, exist_ok=True)
    parquet_dir.mkdir(parents=True, exist_ok=True)
    fls_dir.mkdir(parents=True, exist_ok=True)
    DUCKDB_ROOT.mkdir(parents=True, exist_ok=True)

    conn = duckdb.connect(str(duckdb_path), config={"allow_unsigned_extensions": "true"})
    try:
        fastlanes_loaded = False
        for table_name, table_url in tables_with_urls:
            csv_path = csv_dir / f"{table_name}.csv"
            downloaded = download_to_csv(
                table_url,
                csv_path,
                force=force_download,
            )
            if downloaded:
                print(f"Downloaded {dataset}/{table_name}")
            else:
                print(f"Skipping download for {dataset}/{table_name} (cached)")

            try:
                schema_sql = download_schema(dataset, table_name)
            except (HTTPError, URLError) as err:
                raise RuntimeError(
                    f"Failed to download schema for {dataset}/{table_name}: {err}"
                ) from err

            table_identifier = quote_identifier(table_name)
            conn.execute(f"DROP TABLE IF EXISTS {table_identifier}")
            execute_sql_script(conn, schema_sql)

            # escaped_csv = escape_literal(str(csv_path))
            # conn.execute(
            #     f"COPY {table_identifier} FROM '{escaped_csv}' (FORMAT CSV, HEADER FALSE, DELIM '|', NULL 'null')"
            # )

            parquet_path = parquet_dir / f"{table_name}.parquet"
            escaped_parquet = escape_literal(str(parquet_path))
            conn.execute(
                f"COPY {table_identifier} TO '{escaped_parquet}' (FORMAT PARQUET)"
            )

            fls_path = fls_dir / f"{table_name}.fls"
            escaped_fls = escape_literal(str(fls_path))
            if not fastlanes_loaded:
                ensure_fastlanes(conn)
                fastlanes_loaded = True
            conn.execute(
                f"COPY {table_identifier} TO '{escaped_fls}' (FORMAT 'fls')"
            )
    finally:
        conn.close()


def parse_args(argv: Optional[Iterable[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Download and prepare Public BI benchmark datasets",
    )
    parser.add_argument(
        "--datasets",
        nargs="*",
        help="Specific dataset directories to download (defaults to all available)",
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Overwrite existing dataset directories",
    )
    parser.add_argument(
        "--force-download",
        action="store_true",
        help="Always re-download CSV sources even if cached",
    )
    return parser.parse_args(argv)


def main(argv: Optional[Iterable[str]] = None) -> int:
    args = parse_args(argv)

    try:
        datasets = args.datasets or discover_datasets()
        if not datasets:
            print("No datasets discovered. Specify --datasets explicitly.", file=sys.stderr)
            return 1

        for dataset in datasets:
            try:
                process_dataset(
                    dataset,
                    overwrite=args.overwrite,
                    force_download=args.force_download,
                )
            except Exception as err:  # pragma: no cover - surfaces errors to CLI
                print(f"Failed to process dataset '{dataset}': {err}", file=sys.stderr)
                return 1
        return 0
    except Exception as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
