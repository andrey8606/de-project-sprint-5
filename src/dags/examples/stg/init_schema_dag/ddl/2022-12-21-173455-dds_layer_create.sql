create table if not exists dds.dm_couriers (
	id Serial not null primary key,
	courier_id varchar not null,
	courier_name varchar not null
);

create table if not exists dds.dm_deliveries (
	id Serial not null primary key,
	order_id int4 not null,
	order_ts timestamp not null,
	delivery_id int4 not null,
	courier_id int4 not null,
	address text,
	delivery_ts timestamp not null,
	rate int4 not null,
	sum numeric(14, 2) not null,
	tip_sum numeric(14, 2) not null
);

ALTER TABLE dds.dm_deliveries DROP CONSTRAINT IF EXISTS fk_order_id;
ALTER TABLE dds.dm_deliveries DROP CONSTRAINT IF EXISTS fk_courier_id;
ALTER table dds.dm_deliveries ADD CONSTRAINT fk_order_id FOREIGN KEY (order_id) REFERENCES dds.dm_orders(id);
ALTER TABLE dds.dm_deliveries ADD CONSTRAINT fk_courier_id FOREIGN KEY (courier_id) REFERENCES dds.dm_couriers(id);


-- CREATE TABLE IF NOT EXISTS test_dds.dm_orders(
-- 	order_id_dwh serial PRIMARY key,
-- 	order_id_source	varchar(30) UNIQUE
-- );
--
-- CREATE TABLE IF NOT EXISTS test_dds.dm_couriers(
-- 	courier_id_dwh serial PRIMARY KEY,
-- 	courier_id_source varchar(30) UNIQUE,
-- 	name varchar
-- );
--
-- CREATE TABLE IF NOT EXISTS test_dds.dm_deliveries(
-- 	delivery_id_dwh serial PRIMARY KEY,
-- 	delivery_id_source varchar(30) UNIQUE
-- );
--
-- CREATE TABLE IF NOT EXISTS test_dds.fct_deliveries(
--     order_id_dwh integer PRIMARY KEY
-- 	delivery_id_dwh	integer,
-- 	courier_id_dwh	integer,
-- 	order_ts timestamp,
--     delivery_ts timestamp,
-- 	address varchar,
-- 	rate  integer,
-- 	tip_sum numeric (14, 2),
-- 	total_sum numeric (14, 2),
--
--     CONSTRAINT fct_deliveries_order_id_dwh_fkey
--     FOREIGN KEY (order_id_dwh)
--     REFERENCES test_dds.dm_orders(order_id_dwh),
--
--     CONSTRAINT fct_deliveries_delivery_id_dwh_fkey
--     FOREIGN KEY (delivery_id_dwh)
--     REFERENCES test_dds.dm_deliveries(delivery_id_dwh),
--
--     CONSTRAINT fct_deliveries_courier_id_dwh_fkey
--     FOREIGN KEY (courier_id_dwh)
--     REFERENCES test_dds.dm_couriers(courier_id_dwh)
-- );