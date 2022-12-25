create table if not exists dds.dm_couriers (
	id Serial not null primary key,
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

CREATE TABLE if not exists dds.dm_timestamps_delivery (
	id serial4 NOT NULL,
	ts timestamp NOT NULL,
	"year" int4 NOT NULL,
	"month" int4 NOT NULL,
	"day" int4 NOT NULL,
	"time" time NOT NULL,
	"date" date NOT NULL,
	CONSTRAINT dm_timestamps_delivery_day_check CHECK (((day >= 1) AND (day <= 31))),
	CONSTRAINT dm_timestamps_delivery_month_check CHECK (((month >= 1) AND (month <= 12))),
	CONSTRAINT dm_timestamps_delivery_pkey PRIMARY KEY (id),
	CONSTRAINT dm_timestamps_delivery_year_check CHECK (((year >= 2022) AND (year < 2500)))
);

ALTER TABLE dds.dm_deliveries DROP CONSTRAINT IF EXISTS fk_order_id;
ALTER TABLE dds.dm_deliveries DROP CONSTRAINT IF EXISTS fk_courier_id;
ALTER table dds.dm_deliveries ADD CONSTRAINT fk_order_id FOREIGN KEY (order_id) REFERENCES dds.dm_orders(id);
ALTER TABLE dds.dm_deliveries ADD CONSTRAINT fk_courier_id FOREIGN KEY (courier_id) REFERENCES dds.dm_couriers(id);