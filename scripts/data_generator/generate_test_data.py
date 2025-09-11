import duckdb
import os

BASE_PATH = os.path.dirname(os.path.realpath(__file__)) + "/../../data/generated"
TMP_PATH = "/tmp"

################################################
### TPC-H
################################################

if not os.path.isdir(BASE_PATH):
    os.makedirs(BASE_PATH)

if not os.path.isdir(BASE_PATH + "/parquet"):
    os.makedirs(BASE_PATH + "/parquet")

if not os.path.isdir(BASE_PATH + "/csv"):
    os.makedirs(BASE_PATH + "/csv")

if not os.path.isdir(BASE_PATH + "/duckdb"):
    os.makedirs(BASE_PATH + "/duckdb")


## TPC-H SF0
print("Generating TPC-H SF0")
con = duckdb.connect()
con.query("call dbgen(sf=0);")

con.query(f"EXPORT DATABASE '{BASE_PATH}/parquet/tpch_sf0' (FORMAT parquet);")
# This is temporarily used as source for FastLanes conversion
con.query(
    f"EXPORT DATABASE '{BASE_PATH}/csv/tpch_sf0' (FORMAT csv, delimiter '|', header false, new_line '\n');"
)

con.query(f"ATTACH '{BASE_PATH}/duckdb/tpch_sf0.duckdb' AS persistent;")
con.query("COPY FROM DATABASE memory TO persistent;")
con.query("DETACH persistent;")

## TPC-H SF0.01
print("Generating TPC-H SF0.01")
con = duckdb.connect()
con.query("call dbgen(sf=0.01);")

con.query(f"EXPORT DATABASE '{BASE_PATH}/parquet/tpch_sf0_01' (FORMAT parquet);")
# This is temporarily used as source for FastLanes conversion
con.query(
    f"EXPORT DATABASE '{BASE_PATH}/csv/tpch_sf0_01' (FORMAT csv, delimiter '|', header false, new_line '\n');"
)

con.query(f"ATTACH '{BASE_PATH}/duckdb/tpch_sf0_01.duckdb' AS persistent;")
con.query("COPY FROM DATABASE memory TO persistent;")
con.query("DETACH persistent;")

## TPC-H SF1
print("Generating TPC-H SF1")
con = duckdb.connect()
con.query("call dbgen(sf=1);")

con.query(f"EXPORT DATABASE '{BASE_PATH}/parquet/tpch_sf1' (FORMAT parquet);")
# This is temporarily used as source for FastLanes conversion
con.query(
    f"EXPORT DATABASE '{BASE_PATH}/csv/tpch_sf1' (FORMAT csv, delimiter '|', header false, new_line '\n');"
)

con.query(f"ATTACH '{BASE_PATH}/duckdb/tpch_sf1.duckdb' AS persistent;")
con.query("COPY FROM DATABASE memory TO persistent;")
con.query("DETACH persistent;")
