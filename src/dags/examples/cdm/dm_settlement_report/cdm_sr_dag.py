import logging

import pendulum
from airflow.decorators import dag, task
from lib import ConnectionBuilder
from lib import PgConnect

log = logging.getLogger(__name__)


def insert_sr(conn: PgConnect) -> None:
    with conn.client().cursor() as cur:
        cur.execute(
            """
                WITH order_sums AS (
                SELECT
                    r.restaurant_id                 AS restaurant_id,
                    r.restaurant_name               AS restaurant_name,
                    tss.date                        AS settlement_date,
                    COUNT(DISTINCT fct.order_id)    AS orders_count,
                    SUM(fct.total_sum)              AS orders_total_sum,
                    SUM(fct.bonus_payment)          AS orders_bonus_payment_sum,
                    SUM(fct.bonus_grant)            AS orders_bonus_granted_sum
                FROM dds.fct_product_sales as fct
                    INNER JOIN dds.dm_orders AS orders
                        ON fct.order_id = orders.id
                    INNER JOIN dds.dm_timestamps as tss
                        ON tss.id = orders.timestamp_id
                    INNER JOIN dds.dm_restaurants AS r
                        on r.id = orders.restaurant_id
                WHERE orders.order_status = 'CLOSED'
                GROUP BY
                    r.restaurant_id,
                    r.restaurant_name,
                    tss.date
            )
            INSERT INTO cdm.dm_settlement_report(
                restaurant_id,
                restaurant_name,
                settlement_date,
                orders_count,
                orders_total_sum,
                orders_bonus_payment_sum,
                orders_bonus_granted_sum,
                order_processing_fee,
                restaurant_reward_sum
            )
            SELECT
                restaurant_id,
                restaurant_name,
                settlement_date,
                orders_count,
                orders_total_sum,
                orders_bonus_payment_sum,
                orders_bonus_granted_sum,
                s.orders_total_sum * 0.25 AS order_processing_fee,
                s.orders_total_sum - s.orders_total_sum * 0.25 - s.orders_bonus_payment_sum AS restaurant_reward_sum
            FROM order_sums AS s
            ON CONFLICT (restaurant_id, settlement_date) DO UPDATE
            SET
                orders_count = EXCLUDED.orders_count,
                orders_total_sum = EXCLUDED.orders_total_sum,
                orders_bonus_payment_sum = EXCLUDED.orders_bonus_payment_sum,
                orders_bonus_granted_sum = EXCLUDED.orders_bonus_granted_sum,
                order_processing_fee = EXCLUDED.order_processing_fee,
                restaurant_reward_sum = EXCLUDED.restaurant_reward_sum;     
            """
        )

@dag(
    schedule_interval='0/15 * * * *',  # Задаем расписание выполнения дага - каждый 15 минут.
    start_date=pendulum.datetime(2022, 5, 5, tz="UTC"),  # Дата начала выполнения дага. Можно поставить сегодня.
    catchup=False,  # Нужно ли запускать даг за предыдущие периоды (с start_date до сегодня) - False (не нужно).
    tags=['sprint5', 'stg', 'origin', 'cdm', 'sr'],  # Теги, используются для фильтрации в интерфейсе Airflow.
    is_paused_upon_creation=True  # Остановлен/запущен при появлении. Сразу запущен.
)
def sprint5_cdm_dm_settlement_report_dag():
    # Создаем подключение к базе dwh.
    dwh_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")

    # Создаем подключение к базе подсистемы бонусов.
    # origin_pg_connect = ConnectionBuilder.pg_conn("PG_ORIGIN_BONUS_SYSTEM_CONNECTION")

    # Объявляем таск, который загружает данные.
    @task(task_id="load_sr")
    def load_sr():
        insert_sr(dwh_pg_connect)

    # Инициализируем объявленные таски.
    sr_dict = load_sr()

    # Далее задаем последовательность выполнения тасков.
    # Т.к. таск один, просто обозначим его здесь.
    sr_dict  # type: ignore


cdm_dm_settlement_report_dag = sprint5_cdm_dm_settlement_report_dag()
