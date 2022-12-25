import datetime
from logging import Logger
from typing import List

from examples.dds import EtlSetting, DdsEtlSettingsRepository
from lib import PgConnect
from lib.dict_util import json2str, str2json
from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel


class OrderObj(BaseModel):
    id: int
    object_id: str
    object_value: str
    update_ts: datetime.datetime


class OrdersOriginRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def list_orders(self, order_threshold: datetime.datetime) -> List[OrderObj]:
        with self._db.client().cursor(row_factory=class_row(OrderObj)) as cur:
            cur.execute(
                """
                    SELECT id, object_id, object_value, update_ts
                    FROM stg.ordersystem_orders
                    WHERE update_ts > %(threshold)s --Пропускаем те объекты, которые уже загрузили.
                    ORDER BY update_ts ASC --Обязательна сортировка по id, т.к. id используем в качестве курсора.
                """, {
                    "threshold": order_threshold
                }
            )
            objs = cur.fetchall()
        return objs


class OrderDestRepository:

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


    def get_user_id(self, conn: Connection, user_id: str) -> int:
        with conn.cursor() as cur:
            cur.execute(
                """
                    select id from dds.dm_users du 
                    where user_id = %(user_id)s
                """, {
                    "user_id": user_id
                }
            )
            objs = cur.fetchall()
        if len(objs) > 0:
            return int(objs[0][0])
        return -1


    def get_timestamp_id(self, conn: Connection, ts: datetime.datetime) -> int:
        with conn.cursor() as cur:
            cur.execute(
                """
                    select id from dds.dm_timestamps tis
                    where tis.ts = %(ts)s
                """, {
                    "ts": ts
                }
            )
            objs = cur.fetchall()
        if len(objs) > 0:
            return int(objs[0][0])
        return -1

    def get_latest_status(self, statuses):
        max_date = None
        result_status = None
        for val in statuses:
            dt = datetime.datetime.strptime(val['dttm'], '%Y-%m-%d %H:%M:%S')
            if not max_date:
                max_date = dt
                result_status = val['status']
            else:
                if dt > max_date:
                    max_date = dt
                    result_status = val['status']
        return result_status

    def insert_order(self, conn: Connection, order: OrderObj) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO dds.dm_orders(user_id, restaurant_id, timestamp_id,
                    order_key, order_status)
                    VALUES (%(user_id)s, %(restaurant_id)s, %(timestamp_id)s, 
                    %(order_key)s, %(order_status)s)
                """,
                {
                    "user_id": self.get_user_id(conn, str2json(order.object_value)['user']['id']),
                    "restaurant_id": self.get_restaurant_id(conn, str2json(order.object_value)['restaurant']['id']),
                    "timestamp_id": self.get_timestamp_id(conn, str2json(order.object_value)['date']),
                    "order_key": str2json(order.object_value)['_id'],
                    "order_status": self.get_latest_status(str2json(order.object_value)['statuses']),
                },
            )


class OrderLoader:
    WF_KEY = "orders_to_dds_workflow"
    LAST_LOADED_ID_KEY = "last_loaded_ts"

    def __init__(self, pg_con: PgConnect, log: Logger) -> None:
        self.pg_dest = pg_con
        self.origin = OrdersOriginRepository(pg_con)
        self.stg = OrderDestRepository()
        self.settings_repository = DdsEtlSettingsRepository()
        self.log = log

    def load_orders(self):
        # открываем транзакцию.
        # Транзакция будет закоммичена, если код в блоке with пройдет успешно (т.е. без ошибок).
        # Если возникнет ошибка, произойдет откат изменений (rollback транзакции).
        with self.pg_dest.connection() as conn:

            # Прочитываем состояние загрузки
            # Если настройки еще нет, заводим ее.
            wf_setting = self.settings_repository.get_setting(conn, self.WF_KEY)
            if not wf_setting:
                wf_setting = EtlSetting(id=0, workflow_key=self.WF_KEY,
                                        workflow_settings={self.LAST_LOADED_ID_KEY:
                                                               datetime.datetime(2022, 1, 1).strftime('%Y-%m-%d %H:%M:%S')})

            # Вычитываем очередную пачку объектов.
            last_loaded = wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]
            load_queue = self.origin.list_orders(datetime.datetime.strptime(last_loaded, '%Y-%m-%d %H:%M:%S'))
            self.log.info(f"Found {len(load_queue)} orders to load.")
            if not load_queue:
                self.log.info("Quitting.")
                return

            # Сохраняем объекты в базу dwh.
            for order in load_queue:
                self.stg.insert_order(conn, order)

            # Сохраняем прогресс.
            # Мы пользуемся тем же connection, поэтому настройка сохранится вместе с объектами,
            # либо откатятся все изменения целиком.
            wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = max([t.update_ts for t in load_queue])
            wf_setting_json = json2str(wf_setting.workflow_settings)  # Преобразуем к строке, чтобы положить в БД.
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

            self.log.info(f"Load finished on {wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]}")
