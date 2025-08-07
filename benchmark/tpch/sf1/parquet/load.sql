create view customer as from parquet_scan('./data/generated/parquet/tpch_sf1/customer.parquet');
create view lineitem as from parquet_scan('./data/generated/parquet/tpch_sf1/lineitem.parquet');
create view nation as from parquet_scan('./data/generated/parquet/tpch_sf1/nation.parquet');
create view orders as from parquet_scan('./data/generated/parquet/tpch_sf1/orders.parquet');
create view part as from parquet_scan('./data/generated/parquet/tpch_sf1/part.parquet');
create view partsupp as from parquet_scan('./data/generated/parquet/tpch_sf1/partsupp.parquet');
create view region as from parquet_scan('./data/generated/parquet/tpch_sf1/region.parquet');
create view supplier as from parquet_scan('./data/generated/parquet/tpch_sf1/supplier.parquet');