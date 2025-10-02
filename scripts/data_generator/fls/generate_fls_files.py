#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import shutil
import subprocess
import sys
from pathlib import Path
from tempfile import TemporaryDirectory

SCRIPT_DIR = Path(__file__).resolve().parent
DATA_GENERATOR_DIR = SCRIPT_DIR.parent
PROJECT_ROOT = DATA_GENERATOR_DIR.parent.parent

if str(DATA_GENERATOR_DIR) not in sys.path:
    sys.path.insert(0, str(DATA_GENERATOR_DIR))

from tpch_utils import TPCH_TABLES, parse_scale_list  # noqa: E402

schemas = {
    "customer": [
        {"name": "c_custkey", "type": "FLS_I64"},
        {"name": "c_name", "type": "FLS_STR"},
        {"name": "c_address", "type": "FLS_STR"},
        {"name": "c_nationkey", "type": "FLS_I64"},
        {"name": "c_phone", "type": "FLS_STR"},
        {"name": "c_acctbal", "type": "FLS_DBL"},
        {"name": "c_mktsegment", "type": "FLS_STR"},
        {"name": "c_comment", "type": "FLS_STR"},
    ],
    "lineitem": [
        {"name": "l_orderkey", "type": "FLS_I64"},
        {"name": "l_partkey", "type": "FLS_I64"},
        {"name": "l_suppkey", "type": "FLS_I64"},
        {"name": "l_linenumber", "type": "FLS_I64"},
        {"name": "l_quantity", "type": "FLS_DBL"},
        {"name": "l_extendedprice", "type": "FLS_DBL"},
        {"name": "l_discount", "type": "FLS_DBL"},
        {"name": "l_tax", "type": "FLS_DBL"},
        {"name": "l_returnflag", "type": "FLS_STR"},
        {"name": "l_linestatus", "type": "FLS_STR"},
        {"name": "l_shipdate", "type": "DATE"},
        {"name": "l_commitdate", "type": "DATE"},
        {"name": "l_receiptdate", "type": "DATE"},
        {"name": "l_shipinstruct", "type": "FLS_STR"},
        {"name": "l_shipmode", "type": "FLS_STR"},
        {"name": "l_comment", "type": "FLS_STR"},
    ],
    "nation": [
        {"name": "n_nationkey", "type": "FLS_I32"},
        {"name": "n_name", "type": "FLS_STR"},
        {"name": "n_regionkey", "type": "FLS_I32"},
        {"name": "n_comment", "type": "FLS_STR"},
    ],
    "orders": [
        {"name": "o_orderkey", "type": "FLS_I64"},
        {"name": "o_custkey", "type": "FLS_I64"},
        {"name": "o_orderstatus", "type": "FLS_STR"},
        {"name": "o_totalprice", "type": "FLS_DBL"},
        {"name": "o_orderdate", "type": "DATE"},
        {"name": "o_orderpriority", "type": "FLS_STR"},
        {"name": "o_clerk", "type": "FLS_STR"},
        {"name": "o_shippriority", "type": "FLS_I64"},
        {"name": "o_comment", "type": "FLS_STR"},
    ],
    "part": [
        {"name": "p_partkey", "type": "FLS_I64"},
        {"name": "p_name", "type": "FLS_STR"},
        {"name": "p_mfgr", "type": "FLS_STR"},
        {"name": "p_brand", "type": "FLS_STR"},
        {"name": "p_type", "type": "FLS_STR"},
        {"name": "p_size", "type": "FLS_I64"},
        {"name": "p_container", "type": "FLS_STR"},
        {"name": "p_retailprice", "type": "FLS_DBL"},
        {"name": "p_comment", "type": "FLS_STR"},
    ],
    "partsupp": [
        {"name": "ps_partkey", "type": "FLS_I64"},
        {"name": "ps_suppkey", "type": "FLS_I64"},
        {"name": "ps_availqty", "type": "FLS_I64"},
        {"name": "ps_supplycost", "type": "FLS_DBL"},
        {"name": "ps_comment", "type": "FLS_STR"},
    ],
    "region": [
        {"name": "r_regionkey", "type": "FLS_I64"},
        {"name": "r_name", "type": "FLS_STR"},
        {"name": "r_comment", "type": "FLS_STR"},
    ],
    "supplier": [
        {"name": "s_suppkey", "type": "FLS_I64"},
        {"name": "s_name", "type": "FLS_STR"},
        {"name": "s_address", "type": "FLS_STR"},
        {"name": "s_nationkey", "type": "FLS_I64"},
        {"name": "s_phone", "type": "FLS_STR"},
        {"name": "s_acctbal", "type": "FLS_DBL"},
        {"name": "s_comment", "type": "FLS_STR"},
    ],
}

DATA_DIR = PROJECT_ROOT / "benchmark" / "data" / "tpch"
BUILD_DIR = PROJECT_ROOT / "build" / "release"
GENERATE_FLS_BINARY = BUILD_DIR / "extension/fastlanes/scripts/data_generator/fls/generate_fls"


def resolve_generate_fls_binary() -> Path:
    if not GENERATE_FLS_BINARY.exists():
        print(f"Error: generate_fls executable not found at {GENERATE_FLS_BINARY}")
        print("Make sure to build the project first with 'make'")
        sys.exit(1)
    return GENERATE_FLS_BINARY


def convert_scale(scale, generate_fls_binary: Path) -> None:
    print(f"Converting CSV to FastLanes for {scale.display_name}")
    csv_base = DATA_DIR / "csv" / scale.dataset_name
    if not csv_base.is_dir():
        raise FileNotFoundError(f"CSV data not found for {scale.display_name} at {csv_base}")

    fls_dir = DATA_DIR / "fls" / scale.dataset_name
    if fls_dir.exists():
        shutil.rmtree(fls_dir)
    fls_dir.mkdir(parents=True, exist_ok=True)

    with TemporaryDirectory(prefix=f"fls_{scale.slug}_") as tmp_root:
        tmp_root_path = Path(tmp_root)
        for table in TPCH_TABLES:
            table_dir = tmp_root_path / table
            table_dir.mkdir(parents=True, exist_ok=True)

            csv_path = csv_base / f"{table}.csv"
            if not csv_path.exists():
                raise FileNotFoundError(f"CSV file missing for table '{table}' at {csv_path}")

            shutil.copyfile(csv_path, table_dir / f"{table}.csv")

            schema = {
                "columns": [
                    {"name": col["name"], "type": col["type"]} for col in schemas[table]
                ]
            }
            (table_dir / "schema.json").write_text(json.dumps(schema))

            fls_path = fls_dir / f"{table}.fls"
            print(f"  - {table}")
            subprocess.run(
                [str(generate_fls_binary), str(table_dir), str(fls_path)],
                check=True,
                text=True,
            )


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
    generate_fls_binary = resolve_generate_fls_binary()

    for scale in scales:
        convert_scale(scale, generate_fls_binary)


if __name__ == "__main__":
    main()
