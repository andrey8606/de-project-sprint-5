CREATE TABLE IF NOT EXISTS stg.ordersystem_users (
	id Serial NOT null constraint pk_ordersystem_users primary key,
	object_id varchar not null,
	object_value text not null,
	update_ts timestamp not null,
	constraint ordersystem_users_object_id_uindex unique (object_id)
);