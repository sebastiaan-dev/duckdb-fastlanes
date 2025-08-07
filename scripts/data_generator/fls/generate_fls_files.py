#!/usr/bin/env python3
import subprocess
import os
import sys
import argparse
import shutil
import json

# TPC-H tables
tables = [
    "customer",
    "lineitem",
    "nation",
    "orders",
    "part",
    "partsupp",
    "region",
    "supplier",
]
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
        {"name": "n_nationkey", "type": "FLS_U08"},
        {"name": "n_name", "type": "FLS_STR"},
        {"name": "n_regionkey", "type": "FLS_U08"},
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

DATA_DIR = "data/generated"
BUILD_DIR = "build/release"
BIN = "extension/fastlanes/scripts/data_generator/fls/generate_fls"


def main():
    parser = argparse.ArgumentParser(description="Convert Parquet files to FLS format")
    parser.add_argument("--sf", default="1", help="Scale factor (default: 1)")
    args = parser.parse_args()

    script_dir = os.path.dirname(os.path.realpath(__file__))
    project_root = os.path.abspath(os.path.join(script_dir, "../../.."))
    base_path = os.path.join(project_root, DATA_DIR)
    generate_fls_path = os.path.join(project_root, BUILD_DIR, BIN)

    if not os.path.exists(generate_fls_path):
        print(f"Error: generate_fls executable not found at {generate_fls_path}")
        print("Make sure to build the project first with 'make'")
        sys.exit(1)

    fls_dir = os.path.join(base_path, "fls", f"tpch_sf{args.sf}")
    os.makedirs(fls_dir, exist_ok=True)

    for table in tables:
        tmpdir = f"./TMP/{table}"
        if not os.path.exists(tmpdir):
            os.makedirs(tmpdir)
        csv_path = os.path.join(base_path, "csv", f"tpch_sf{args.sf}", f"{table}.csv")
        if not os.path.exists(csv_path):
            print(f"Warning: CSV file not found: {csv_path}")
            continue

        tmp_csv_path = os.path.join(tmpdir, f"{table}.csv")
        shutil.copyfile(csv_path, tmp_csv_path)

        tmp_schema_path = os.path.join(tmpdir, "schema.json")
        if table not in schemas:
            print(f"Warning: Schema not defined for table: {table}")
            continue
        schema = {
            "columns": [
                {"name": col["name"], "type": col["type"]} for col in schemas[table]
            ]
        }
        with open(tmp_schema_path, mode="w") as schemafile:
            schemafile.write(json.dumps(schema))

        fls_path = os.path.join(fls_dir, f"{table}.fls")

        print(f"Converting {table} to FLS format...")
        result = subprocess.run(
            [generate_fls_path, tmpdir, fls_path],
            capture_output=True,
            text=True,
        )

        if result.returncode == 0:
            print(f"Successfully converted {table} to FLS format")
        else:
            print(f"Error converting {table}: {result.stderr}")
            print(f"Command output: {result.stdout}")

    shutil.rmtree("TMP")


if __name__ == "__main__":
    main()
