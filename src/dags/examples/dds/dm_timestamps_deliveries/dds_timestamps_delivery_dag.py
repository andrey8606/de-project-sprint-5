import logging

import pendulum
from airflow.decorators import dag, task
from examples.dds.dm_timestamps_deliveries.timestamps_delivery_loader import TimestampLoader
from lib import ConnectionBuilder

log = logging.getLogger(__name__)


@dag(
    schedule_interval='0/15 * * * *',  # Задаем расписание выполнения дага - каждый 15 минут.
    start_date=pendulum.datetime(2022, 5, 5, tz="UTC"),  # Дата начала выполнения дага. Можно поставить сегодня.
    catchup=False,  # Нужно ли запускать даг за предыдущие периоды (с start_date до сегодня) - False (не нужно).
    tags=['sprint5', 'stg', 'origin', 'dds', 'timestamps', 'delivery'],  # Теги, используются для фильтрации в интерфейсе Airflow.
    is_paused_upon_creation=True  # Остановлен/запущен при появлении. Сразу запущен.
)
def sprint5_dds_timestamps_delivery_dag():
    # Создаем подключение к базе dwh.
    dwh_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")

    # Создаем подключение к базе подсистемы бонусов.
    # origin_pg_connect = ConnectionBuilder.pg_conn("PG_ORIGIN_BONUS_SYSTEM_CONNECTION")

    # Объявляем таск, который загружает данные.
    @task(task_id="timestamps_delivery_load")
    def load_timestamps_delivery():
        # создаем экземпляр класса, в котором реализована логика.
        ts_loader = TimestampLoader(dwh_pg_connect, log)
        ts_loader.load_timestamps()  # Вызываем функцию, которая перельет данные.

    # Инициализируем объявленные таски.
    timestamps_delivery_dict = load_timestamps_delivery()

    # Далее задаем последовательность выполнения тасков.
    # Т.к. таск один, просто обозначим его здесь.
    timestamps_delivery_dict  # type: ignore


dds_timestamps_delivery_dag = sprint5_dds_timestamps_delivery_dag()
