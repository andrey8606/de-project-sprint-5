from datetime import datetime
from typing import Dict, List
from logging import Logger

from lib import MongoConnect


class OrderReader:
    def __init__(self, mc: MongoConnect, logger: Logger) -> None:
        self.dbs = mc.client()
        self.log = logger

    def get_orders(self, load_threshold: datetime, limit) -> List[Dict]:
        # Формируем фильтр: больше чем дата последней загрузки
        filter = {'update_ts': {'$gt': load_threshold}}

        # Формируем сортировку по update_ts. Сортировка обязательна при инкрементальной загрузке.
        sort = [('update_ts', 1)]

        # Вычитываем документы из MongoDB с применением фильтра и сортировки.
        docs = list(self.dbs.get_collection("orders").find(filter=filter, sort=sort, limit=limit))
        self.log.info(f"filter: {filter}, sort: {sort}")
        return docs
