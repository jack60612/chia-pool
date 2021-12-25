import asyncio
import logging
import os
import yaml
from typing import Optional, Dict

from pool.store.abstract import AbstractPoolStore


class PoolDB:
    def __init__(self, pool_store: Optional[AbstractPoolStore] = None):

        # We load our configurations from here
        with open(os.getcwd() + "/config.yaml") as f:
            pool_config: Dict = yaml.safe_load(f)

        # logging config
        self.log = logging.getLogger("PoolDB")
        # If you want to log to a file: use filename='example.log', encoding='utf-8'
        logging.basicConfig(level=logging.INFO)
        # select db type
        if pool_config.get("store") == "MySQLPoolStore":
            from pool.store.mysql_store import MySQLPoolStore

            self.store: AbstractPoolStore = MySQLPoolStore()
        else:
            from pool.store.sqlite_store import SqlitePoolStore

            self.store: AbstractPoolStore = pool_store or SqlitePoolStore()

    async def generate_database(self):
        await self.store.connect()
        self.log.info("Database generated.")
        await asyncio.sleep(5)  # wait 5 seconds just to be safe.
        await self.store.close()


async def start():
    pool_store = None  # easy override if needed
    database = PoolDB(pool_store=pool_store)
    await database.generate_database()


asyncio.run(start())
