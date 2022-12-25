import datetime
from logging import Logger
from typing import List

from examples.dds import EtlSetting, DdsEtlSettingsRepository
from lib import PgConnect
from lib.dict_util import json2str, str2json
from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel


class FpsObj(BaseModel):
    id: int
    order_id: str
    product_id: str
    product_price: float
    product_count: int
    order_sum: float
    payment_sum: float
    granted_sum: float


class FpsOriginRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def list_fps(self, fps_threshold: int) -> List[FpsObj]:
        with self._db.client().cursor(row_factory=class_row(FpsObj)) as cur:
            cur.execute(
                """
                    SELECT id, order_id, product_id, product_price, product_count, 
                    order_sum, payment_sum, granted_sum
                    FROM public.bonus_transactions
                    WHERE id > %(threshold)s --Пропускаем те объекты, которые уже загрузили.
                    ORDER BY id ASC --Обязательна сортировка по id, т.к. id используем в качестве курсора.
                """, {
                    "threshold": fps_threshold
                }
            )
            objs = cur.fetchall()
        return objs


class FpsDestRepository:
    def __init__(self, log: Logger) -> None:
        self.log = log

    def get_order_id(self, conn: Connection, order_id: str) -> int:
        with conn.cursor() as cur:
            cur.execute(
                """
                    select id from dds.dm_orders dr 
                    where order_key = %(order_id)s
                """, {
                    "order_id": order_id
                }
            )
            objs = cur.fetchall()
        if len(objs) > 0:
            return int(objs[0][0])
        self.log.info(f"Found {str(order_id)} order_id not in order table.")
        return -1

    def get_product_id(self, conn: Connection, product_id: str) -> int:
        with conn.cursor() as cur:
            cur.execute(
                """
                    select id from dds.dm_products pr 
                    where product_id = %(product_id)s
                """, {
                    "product_id": product_id
                }
            )
            objs = cur.fetchall()
        if len(objs) > 0:
            return int(objs[0][0])
        return -1

    def insert_fps(self, conn: Connection, fps: FpsObj) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO dds.fct_product_sales(id, product_id, order_id, count, price, 
                                             total_sum, bonus_payment, bonus_grant)
                    VALUES (%(id)s, %(product_id)s, %(order_id)s, %(count)s, %(price)s, 
                            %(total_sum)s, %(bonus_payment)s, %(bonus_grant)s)
                    ON CONFLICT (id) DO UPDATE
                    SET
                        product_id = EXCLUDED.product_id,
                        order_id = EXCLUDED.order_id,
                        count = EXCLUDED.count,
                        price = EXCLUDED.price,
                        total_sum = EXCLUDED.total_sum,
                        bonus_payment = EXCLUDED.bonus_payment,
                        bonus_grant = EXCLUDED.bonus_grant;
                """,
                {
                    "id": fps.id,
                    "product_id": self.get_product_id(conn, fps.product_id),
                    "order_id": self.get_order_id(conn, fps.order_id),
                    "count": fps.product_count,
                    "price": fps.product_price,
                    "total_sum": fps.order_sum,
                    "bonus_payment": fps.payment_sum,
                    "bonus_grant": fps.granted_sum
                },
            )


class FpsLoader:
    WF_KEY = "fps_to_dds_workflow"
    LAST_LOADED_ID_KEY = "last_loaded_id"

    def __init__(self, pg_con: PgConnect, origin_pg_con: PgConnect, log: Logger) -> None:
        self.pg_dest = pg_con
        self.origin = FpsOriginRepository(origin_pg_con)
        self.stg = FpsDestRepository(log)
        self.settings_repository = DdsEtlSettingsRepository()
        self.log = log

    def load_fps(self):
        # открываем транзакцию.
        # Транзакция будет закоммичена, если код в блоке with пройдет успешно (т.е. без ошибок).
        # Если возникнет ошибка, произойдет откат изменений (rollback транзакции).
        with self.pg_dest.connection() as conn:

            # Прочитываем состояние загрузки
            # Если настройки еще нет, заводим ее.
            wf_setting = self.settings_repository.get_setting(conn, self.WF_KEY)
            if not wf_setting:
                wf_setting = EtlSetting(id=0, workflow_key=self.WF_KEY, workflow_settings={self.LAST_LOADED_ID_KEY: -1})

            # Вычитываем очередную пачку объектов.
            last_loaded = wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]
            load_queue = self.origin.list_fps(last_loaded)
            self.log.info(f"Found {len(load_queue)} fps to load.")
            if not load_queue:
                self.log.info("Quitting.")
                return

            # Сохраняем объекты в базу dwh.
            for fps in load_queue:
                if ((self.stg.get_order_id(conn, fps.order_id) > 0) and
                        (self.stg.get_product_id(conn, fps.product_id) > 0)):
                    self.stg.insert_fps(conn, fps)

            # Сохраняем прогресс.
            # Мы пользуемся тем же connection, поэтому настройка сохранится вместе с объектами,
            # либо откатятся все изменения целиком.
            wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = max([t.id for t in load_queue])
            wf_setting_json = json2str(wf_setting.workflow_settings)  # Преобразуем к строке, чтобы положить в БД.
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

            self.log.info(f"Load finished on {wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]}")
