CREATE TABLE IF NOT EXISTS stg.ordersystem_orders (
	id serial NOT null constraint pk_ordersystem_orders primary key,
	object_id varchar not null,
	object_value text not null,
	update_ts timestamp not null,
	constraint ordersystem_orders_object_id_uindex unique (object_id)
);