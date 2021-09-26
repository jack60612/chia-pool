import asyncio
import logging
import os
import pathlib

import yaml
from typing import Dict, Optional

from chia.consensus.default_constants import DEFAULT_CONSTANTS
from chia.consensus.constants import ConsensusConstants
from chia.rpc.full_node_rpc_client import FullNodeRpcClient
from chia.rpc.wallet_rpc_client import WalletRpcClient
from chia.util.chia_logging import initialize_logging
from chia.util.default_root import DEFAULT_ROOT_PATH
from chia.util.config import load_config
from chia.util.ints import uint16

from pool.blockchain_state import StateKeeper
from pool.payment_manager.abstract import AbstractPaymentManager
from pool.payment_manager.default import DefaultPaymentManager
from pool.store.abstract import AbstractPoolStore


class PaymentServer:
    def __init__(self, config: Dict, constants: ConsensusConstants,
                 pool_store: Optional[AbstractPoolStore] = None,
                 server_name: Optional[AbstractPaymentManager] = None):
        # We load our configuration from here
        with open(os.getcwd() + "/config.yaml") as f:
            pool_config: Dict = yaml.safe_load(f)

        self.config = config
        self.constants = constants
        # setup logging
        self.log = logging
        self.log.basicConfig(level=logging.INFO)
        initialize_logging("pool", pool_config["logging"], pathlib.Path(pool_config["logging"]["log_path"]))

        if pool_config.get('store') == "MySQLPoolStore":
            from pool.store.mysql_store import MySQLPoolStore
            self.store: AbstractPoolStore = MySQLPoolStore()
        else:
            from pool.store.sqlite_store import SqlitePoolStore
            self.store: AbstractPoolStore = pool_store or SqlitePoolStore()

        # This is the wallet fingerprint and ID for the wallet spending the funds from `self.default_target_puzzle_hash`
        self.wallet_fingerprint = pool_config["wallet_fingerprint"]
        self.wallet_id = pool_config["wallet_id"]

        # rpc hostname to connect to
        self.rpc_hostname = pool_config["rpc_ip_address"]

        self.node_rpc_client: Optional[FullNodeRpcClient] = None
        self.node_rpc_port = pool_config["node_rpc_port"]
        self.wallet_rpc_client: Optional[WalletRpcClient] = None
        self.wallet_rpc_port = pool_config["wallet_rpc_port"]

        # load payment manager and state keeper
        self.payment_manager: AbstractPaymentManager = \
            server_name or DefaultPaymentManager(self.log, pool_config, constants, True)
        self.state_keeper = StateKeeper(self.log, self.wallet_fingerprint)

    async def start(self):
        await self.store.connect()
        await self.init_node_rpc()
        await self.init_wallet_rpc()

        await self.state_keeper.start(self.node_rpc_client, self.wallet_rpc_client)
        await self.payment_manager.start(self.node_rpc_client, self.wallet_rpc_client, self.store,
                                         self.state_keeper)

    async def init_node_rpc(self):
        self.node_rpc_client = await FullNodeRpcClient.create(
            self.rpc_hostname, uint16(self.node_rpc_port), DEFAULT_ROOT_PATH, self.config
        )

    async def init_wallet_rpc(self):
        self.wallet_rpc_client = await WalletRpcClient.create(
            self.rpc_hostname, uint16(self.wallet_rpc_port), DEFAULT_ROOT_PATH, self.config
        )
        res = await self.wallet_rpc_client.log_in_and_skip(fingerprint=self.wallet_fingerprint)
        if not res["success"]:
            raise ValueError(f"Error logging in: {res['error']}. Make sure your config fingerprint is correct.")
        self.log.info(f"Logging in: {res}")
        res = await self.wallet_rpc_client.get_wallet_balance(self.wallet_id)
        self.log.info(f"Obtaining balance: {res}")

    async def stop(self):
        await self.payment_manager.stop()
        await self.state_keeper.stop()
        self.wallet_rpc_client.close()
        await self.wallet_rpc_client.await_closed()
        self.node_rpc_client.close()
        await self.node_rpc_client.await_closed()
        await self.store.close()


server: Optional[PaymentServer] = None


async def start_payment_server(pool_store: Optional[AbstractPoolStore] = None,
                               server_name: Optional[AbstractPaymentManager] = None):
    global server
    config = load_config(DEFAULT_ROOT_PATH, "config.yaml")
    overrides = config["network_overrides"]["constants"][config["selected_network"]]
    constants: ConsensusConstants = DEFAULT_CONSTANTS.replace_str_to_bytes(**overrides)
    server = PaymentServer(config, constants, pool_store, server_name)
    await server.start()
    while True:
        await asyncio.sleep(2400)


async def stop():
    await server.stop()


def main():
    try:
        asyncio.run(start_payment_server())
    except KeyboardInterrupt:
        asyncio.run(stop())


if __name__ == "__main__":
    main()
