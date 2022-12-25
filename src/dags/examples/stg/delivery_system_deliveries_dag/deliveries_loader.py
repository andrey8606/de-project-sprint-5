import datetime

import requests

from logging import Logger
from typing import List, Type

from examples.stg import EtlSetting, StgEtlSettingsRepository
from lib import PgConnect
from lib.dict_util import json2str
from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel


HEADERS = {'X-Nickname': 'azaitsev',
           'X-Cohort': '6',
           'X-API-KEY': '25c27781-8fde-4b30-a22e-524044a7580f'}


class DeliveryObj(BaseModel):
    id: int
    object_id: str
    object_value: str


class DeliveriesOriginRepository:

    def list_deliveries(self, last_loaded: int) -> List[DeliveryObj]:
        result = []
        sort_direction = 'asc'
        sort_field = '_id'
        from_date = datetime.datetime.strftime(datetime.datetime.today() - datetime.timedelta(days=7),
                                               '%Y-%m-%d %H:%M:%S')
        limit = 50
        offset = 0
        id = 1
        url_gen = 'https://d5d04q7d963eapoepsqr.apigw.yandexcloud.net/deliveries?'
        filter_date = f'&from={from_date}'
        filter = f'&sort_field={sort_field}&sort_direction={sort_direction}&limit={limit}&offset={offset}'
        url = url_gen + filter_date + filter
        req = requests.get(url, headers=HEADERS)
        while len(req.json()) > 0:
            for delivery in req.json():
                delivery_obj = DeliveryObj(id=id, object_id=delivery['delivery_id'], object_value=str(delivery))
                if delivery_obj.id > last_loaded:
                    result.append(delivery_obj)
                id += 1
            limit += 50
            offset += 50
            filter = f'&sort_field={sort_field}&sort_direction={sort_direction}&limit={limit}&offset={offset}'
            url = url_gen + filter_date + filter
            req = requests.get(url, headers=HEADERS)
        return result

class DeliveryDestRepository:

    def insert_delivery(self, conn: Connection, delivery: DeliveryObj) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO stg.deliverysystem_deliveries(object_id, object_value)
                    VALUES (%(object_id)s, %(object_value)s)
                    ON CONFLICT (object_id) DO UPDATE
                    SET
                        object_value = EXCLUDED.object_value;
                """,
                {
                    "object_id": delivery.object_id,
                    "object_value": delivery.object_value
                },
            )


class DeliveryLoader:
    WF_KEY = "deliveries_origin_to_stg_workflow"
    LAST_LOADED_ID_KEY = "last_loaded_id"

    def __init__(self, pg_dest: PgConnect, log: Logger) -> None:
        self.pg_dest = pg_dest
        self.origin = DeliveriesOriginRepository()
        self.stg = DeliveryDestRepository()
        self.settings_repository = StgEtlSettingsRepository()
        self.log = log

    def load_deliveries(self):
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
            load_queue = self.origin.list_deliveries(last_loaded)
            self.log.info(f"Found {len(load_queue)} deliveries to load.")
            if not load_queue:
                self.log.info("Quitting.")
                return

            # Сохраняем объекты в базу dwh.
            for delivery in load_queue:
                self.stg.insert_delivery(conn, delivery)

            # Сохраняем прогресс.
            # Мы пользуемся тем же connection, поэтому настройка сохранится вместе с объектами,
            # либо откатятся все изменения целиком.
            wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = max([t.id for t in load_queue])
            wf_setting_json = json2str(wf_setting.workflow_settings)  # Преобразуем к строке, чтобы положить в БД.
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

            self.log.info(f"Load finished on {wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]}")
