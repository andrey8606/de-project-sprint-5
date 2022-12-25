import datetime
from logging import Logger
from typing import List

from examples.dds import EtlSetting, DdsEtlSettingsRepository
from lib import PgConnect
from lib.dict_util import json2str, str2json
from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel


class ProductObj(BaseModel):
    restaurant_id: str
    product_id: str
    product_name: str
    product_price: float
    active_from: datetime.datetime


class ProductsOriginRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def list_products(self) -> List[ProductObj]:
        with self._db.client().cursor(row_factory=class_row(ProductObj)) as cur:
            cur.execute(
                """
                    select distinct 
                    id_restaurant restaurant_id, 
                    items::JSON->>'id' product_id,
                    items::JSON->>'name'::varchar product_name,
                    items::JSON->>'price'::varchar product_price,
                    min(update_ts) active_from
                    from (
                        select (oo.object_value::JSON->'restaurant')::JSON->>'id' id_restaurant, 
                        json_array_elements(oo.object_value::JSON->'order_items') items, update_ts
                        from stg.ordersystem_orders oo 
                    ) ap
                    group by 1, 2, 3, 4
                """
            )
            objs = cur.fetchall()
        return objs

    def list_products_in_base(self) -> List[ProductObj]:
        with self._db.client().cursor(row_factory=class_row(ProductObj)) as cur:
            cur.execute(
                """
                    SELECT id, product_id, product_name, product_price, active_from, 
                    active_to, restaurant_id
                    FROM dds.dm_products;
                """
            )
            obj = cur.fetchall()
        return obj


class ProductsDestRepository:

    def get_restaurant_id(self, conn: Connection, res_id: str) -> int:
        with conn.cursor() as cur:
            cur.execute(
                """
                    select id from dds.dm_restaurants dr 
                    where restaurant_id = %(res_id)s
                """, {
                    "res_id": res_id
                }
            )
            objs = cur.fetchall()
        if len(objs) > 0:
            return int(objs[0][0])
        return -1

    def insert_product(self, conn: Connection, product: ProductObj) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO dds.dm_products(product_id, product_name, product_price, active_from,
                                                active_to, restaurant_id)
                    VALUES (%(product_id)s, %(product_name)s, %(product_price)s, %(active_from)s, 
                            %(active_to)s, %(restaurant_id)s)
                """,
                {
                    "product_id": product.product_id,
                    "product_name": product.product_name,
                    "product_price": product.product_price,
                    "active_from": product.active_from,
                    "active_to": datetime.datetime(2099, 12, 31, 0, 0, 0, 0),
                    "restaurant_id": self.get_restaurant_id(conn, product.restaurant_id)
                },
            )


class ProductLoader:
    WF_KEY = "products_to_dds_workflow"
    LAST_LOADED_ID_KEY = "last_loaded_id"

    def __init__(self, pg_con: PgConnect, log: Logger) -> None:
        self.pg_dest = pg_con
        self.origin = ProductsOriginRepository(pg_con)
        self.stg = ProductsDestRepository()
        self.settings_repository = DdsEtlSettingsRepository()
        self.log = log

    def load_products(self):
        # открываем транзакцию.
        # Транзакция будет закоммичена, если код в блоке with пройдет успешно (т.е. без ошибок).
        # Если возникнет ошибка, произойдет откат изменений (rollback транзакции).
        with self.pg_dest.connection() as conn:

            # Прочитываем состояние загрузки
            # Если настройки еще нет, заводим ее.
            # wf_setting = self.settings_repository.get_setting(conn, self.WF_KEY)
            # if not wf_setting:
            #     wf_setting = EtlSetting(id=0, workflow_key=self.WF_KEY,
            #     workflow_settings={self.LAST_LOADED_ID_KEY: -1})

            # Вычитываем очередную пачку объектов.
            # last_loaded = wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]
            loaded_products_list = []
            loaded_products = self.origin.list_products_in_base()
            for product in loaded_products:
                loaded_products_list.append(product.product_id)

            load_queue = self.origin.list_products()
            self.log.info(f"Found {len(load_queue)} products to load.")
            if not load_queue:
                self.log.info("Quitting.")
                return

            # Сохраняем объекты в базу dwh.
            for product in load_queue:
                if product.product_id not in loaded_products_list:
                    self.stg.insert_product(conn, product)

            # Сохраняем прогресс.
            # Мы пользуемся тем же connection, поэтому настройка сохранится вместе с объектами,
            # либо откатятся все изменения целиком.
            # wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = max([t.id for t in load_queue])
            # wf_setting_json = json2str(wf_setting.workflow_settings)  # Преобразуем к строке, чтобы положить в БД.
            # self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

            # self.log.info(f"Load finished on {wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]}")
