create view customer as from read_fls('./data/generated/fls/tpch_sf1/customer.fls');
create view lineitem as from read_fls('./data/generated/fls/tpch_sf1/lineitem.fls');
create view nation as from read_fls('./data/generated/fls/tpch_sf1/nation.fls');
create view orders as from read_fls('./data/generated/fls/tpch_sf1/orders.fls');
create view part as from read_fls('./data/generated/fls/tpch_sf1/part.fls');
create view partsupp as from read_fls('./data/generated/fls/tpch_sf1/partsupp.fls');
create view region as from read_fls('./data/generated/fls/tpch_sf1/region.fls');
create view supplier as from read_fls('./data/generated/fls/tpch_sf1/supplier.fls');