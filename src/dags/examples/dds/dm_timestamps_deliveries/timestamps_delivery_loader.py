import datetime
from logging import Logger
from typing import List

from examples.dds import EtlSetting, DdsEtlSettingsRepository
from lib import PgConnect
from lib.dict_util import json2str, str2json
from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel


class TimestampObj(BaseModel):
    id: int
    object_id: str
    object_value: str


class TimestampsOriginRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def list_timestamps(self, user_threshold: int) -> List[TimestampObj]:
        with self._db.client().cursor(row_factory=class_row(TimestampObj)) as cur:
            cur.execute(
                """
                    SELECT id, object_id, object_value
                    FROM stg.deliverysystem_deliveries
                    WHERE id > %(threshold)s --Пропускаем те объекты, которые уже загрузили.
                    ORDER BY id ASC --Обязательна сортировка по id, т.к. id используем в качестве курсора.
                """, {
                    "threshold": user_threshold
                }
            )
            objs = cur.fetchall()
        return objs


def get_date_str(value: str) -> datetime.datetime:
    dt = str2json(value.replace("'", '"'))['delivery_ts']
    return datetime.datetime.strptime(dt.split('.')[0], '%Y-%m-%d %H:%M:%S')


class TimestampsDestRepository:

    def insert_timestamp(self, conn: Connection, timestamp: TimestampObj) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO dds.dm_timestamps_delivery(ts, year, month, day, time, date)
                    VALUES (%(ts)s, %(year)s, %(month)s, %(day)s, %(time)s, %(date)s)
                    ON CONFLICT (id) DO UPDATE
                    SET
                        ts = EXCLUDED.ts,
                        year = EXCLUDED.year,
                        month = EXCLUDED.month,
                        day = EXCLUDED.day,
                        time = EXCLUDED.time,
                        date = EXCLUDED.date;
                """,
                {
                    "ts": get_date_str(timestamp.object_value),
                    "year": get_date_str(timestamp.object_value).year,
                    "month": get_date_str(timestamp.object_value).month,
                    "day": get_date_str(timestamp.object_value).day,
                    "time": get_date_str(timestamp.object_value).time(),
                    "date": get_date_str(timestamp.object_value).date()
                },
            )


class TimestampLoader:
    WF_KEY = "timestamps_delivery_to_dds_workflow"
    LAST_LOADED_ID_KEY = "last_loaded_id"

    def __init__(self, pg_con: PgConnect, log: Logger) -> None:
        self.pg_dest = pg_con
        self.origin = TimestampsOriginRepository(pg_con)
        self.stg = TimestampsDestRepository()
        self.settings_repository = DdsEtlSettingsRepository()
        self.log = log

    def load_timestamps(self):
        # открываем транзакцию.
        # Транзакция будет закоммичена, если код в блоке with пройдет успешно (т.е. без ошибок).
        # Если возникнет ошибка, произойдет откат изменений (rollback транзакции).
        with self.pg_dest.connection() as conn:

            # Прочитываем состояние загрузки
            # Если настройки еще нет, заводим ее.
            wf_setting = self.settings_repository.get_setting(conn, self.WF_KEY)
            if not wf_setting:
                wf_setting = EtlSetting(id=0, workflow_key=self.WF_KEY,
                                        workflow_settings={self.LAST_LOADED_ID_KEY: -1})

            # Вычитываем очередную пачку объектов.
            last_loaded = wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]
            load_queue = self.origin.list_timestamps(last_loaded)
            self.log.info(f"Found {len(load_queue)} delivery timestamps to load.")
            if not load_queue:
                self.log.info("Quitting.")
                return

            # Сохраняем объекты в базу dwh.
            for restaurant in load_queue:
                self.stg.insert_timestamp(conn, restaurant)

            # Сохраняем прогресс.
            # Мы пользуемся тем же connection, поэтому настройка сохранится вместе с объектами,
            # либо откатятся все изменения целиком.
            wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = max([t.id for t in load_queue])
            wf_setting_json = json2str(wf_setting.workflow_settings)  # Преобразуем к строке, чтобы положить в БД.
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

            self.log.info(f"Load finished on {wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]}")
