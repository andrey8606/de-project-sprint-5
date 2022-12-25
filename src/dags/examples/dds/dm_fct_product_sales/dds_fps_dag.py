import logging

import pendulum
from airflow.decorators import dag, task
from examples.dds.dm_fct_product_sales.fps_loader import FpsLoader
from lib import ConnectionBuilder

log = logging.getLogger(__name__)


@dag(
    schedule_interval='0/15 * * * *',  # Задаем расписание выполнения дага - каждый 15 минут.
    start_date=pendulum.datetime(2022, 5, 5, tz="UTC"),  # Дата начала выполнения дага. Можно поставить сегодня.
    catchup=False,  # Нужно ли запускать даг за предыдущие периоды (с start_date до сегодня) - False (не нужно).
    tags=['sprint5', 'stg', 'origin', 'dds', 'fps'],  # Теги, используются для фильтрации в интерфейсе Airflow.
    is_paused_upon_creation=True  # Остановлен/запущен при появлении. Сразу запущен.
)
def sprint5_dds_fps_dag():
    # Создаем подключение к базе dwh.
    dwh_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")

    # Создаем подключение к базе подсистемы бонусов.
    origin_pg_connect = ConnectionBuilder.pg_conn("PG_ORIGIN_BONUS_SYSTEM_CONNECTION")

    # Объявляем таск, который загружает данные.
    @task(task_id="fps_load")
    def load_fps():
        # создаем экземпляр класса, в котором реализована логика.
        fps_loader = FpsLoader(dwh_pg_connect, origin_pg_connect, log)
        fps_loader.load_fps()  # Вызываем функцию, которая перельет данные.

    # Инициализируем объявленные таски.
    fps_dict = load_fps()

    # Далее задаем последовательность выполнения тасков.
    # Т.к. таск один, просто обозначим его здесь.
    fps_dict  # type: ignore


dds_fps_dag = sprint5_dds_fps_dag()
