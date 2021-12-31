import os
import yaml
from typing import Optional, Set, List, Tuple, Dict

import asyncio
import aiomysql
import pymysql
from blspy import G1Element
from chia.pools.pool_wallet_info import PoolState
from chia.types.blockchain_format.sized_bytes import bytes32
from chia.types.coin_spend import CoinSpend
from chia.util.ints import uint64, uint32

from .abstract import AbstractPoolStore
from ..record import FarmerRecord
from ..util import RequestMetadata

pymysql.converters.encoders[uint64] = pymysql.converters.escape_int
pymysql.converters.encoders[uint32] = pymysql.converters.escape_int
pymysql.converters.conversions = pymysql.converters.encoders.copy()
pymysql.converters.conversions.update(pymysql.converters.decoders)


class MySQLPoolStore(AbstractPoolStore):
    """
    Pool store based on MariaDB.
    """

    async def connect(self):
        self.lock = asyncio.Lock()
        try:
            # load config
            with open(os.getcwd() + "/config.yaml") as f:
                config: Dict = yaml.safe_load(f)
            self.pool = await aiomysql.create_pool(
                minsize=10,
                maxsize=120,
                host=config["db_host"],
                port=config["db_port"],
                user=config["db_user"],
                password=config["db_password"],
                db=config["db_name"],
            )
        except pymysql.err.OperationalError as e:
            self.log.error("Error In Database Config. Check your config file! %s", e)
            raise ConnectionError("Unable to Connect to SQL Database.")
        connection = await self.pool.acquire()
        cursor = await connection.cursor()
        await cursor.execute(
            (
                "CREATE TABLE IF NOT EXISTS farmer("
                "launcher_id VARCHAR(200) PRIMARY KEY,"
                "p2_singleton_puzzle_hash VARCHAR(256),"
                "delay_time bigint,"
                "delay_puzzle_hash VARCHAR(256),"
                "authentication_public_key VARCHAR(256),"
                "singleton_tip blob,"
                "singleton_tip_state blob,"
                "points bigint,"
                "difficulty bigint,"
                "payout_instructions VARCHAR(256),"
                "is_pool_member tinyint,"
                "overall_points bigint DEFAULT 0,"
                "blocks int DEFAULT 0,"
                "xch_paid float DEFAULT 0,"
                "pps_enabled BOOLEAN DEFAULT 0,"
                "pps_change_datetime DATETIME(6),"
                "INDEX (p2_singleton_puzzle_hash))"
            )
        )

        await cursor.execute(
            "CREATE TABLE IF NOT EXISTS partial("
            "launcher_id VARCHAR(256), "
            "timestamp bigint,"
            "difficulty bigint,"
            "harvester_id VARCHAR(256), "
            "payout_instructions VARCHAR(256),"
            "accept_time DATETIME(6),"
            "pps boolean,"
            "stale boolean DEFAULT 0,"
            "invalid boolean DEFAULT 0,"
            "FOREIGN KEY (launcher_id) REFERENCES farmer(launcher_id),"
            "index (timestamp), index (launcher_id))"
        )
        await cursor.execute(
            (
                "CREATE TABLE IF NOT EXISTS payments("
                "payout_time  DATETIME(6) PRIMARY KEY,"
                "block_height int,"
                "transaction_id VARCHAR(256),"
                "launcher_id VARCHAR(256),"
                "payout_instructions VARCHAR(256),"
                "payout float,"
                "pps bool,"
                "confirmed bool,"
                "FOREIGN KEY (launcher_id) REFERENCES farmer(launcher_id),"
                "index (launcher_id))"
            )
        )
        await cursor.execute(
            (
                "CREATE TABLE IF NOT EXISTS blocks("
                "coin_id VARCHAR(256) PRIMARY KEY,"
                "created_at DATETIME(6),"
                "block_timestamp bigint,"
                "pps bool,"
                "amount float,"
                "block_height int,"
                "launcher_id VARCHAR(256),"
                "FOREIGN KEY (launcher_id) REFERENCES farmer(launcher_id))"
            )
        )
        await cursor.execute(
            (
                "CREATE TABLE IF NOT EXISTS farmer_average("
                "launcher_id VARCHAR(256),"
                "timestamp DATETIME(6),"
                "points bigint,"
                "FOREIGN KEY (launcher_id) REFERENCES farmer(launcher_id))"
            )
        )
        await cursor.execute(
            (
                "CREATE TABLE IF NOT EXISTS pool_stats_graph("
                "stats_time DATETIME(6) PRIMARY KEY,"
                "farmers int,"
                "avg_pool_space int,"
                "raw_pool_space int )"  # stored in tib
            )
        )

        await connection.commit()
        self.pool.release(connection)

    async def close(self):
        self.pool.terminate()
        await self.pool.wait_closed()

    @staticmethod
    def _row_to_farmer_record(row) -> FarmerRecord:
        return FarmerRecord(
            bytes.fromhex(row[0]),
            bytes.fromhex(row[1]),
            row[2],
            bytes.fromhex(row[3]),
            G1Element.from_bytes(bytes.fromhex(row[4])),
            CoinSpend.from_bytes(row[5]),
            PoolState.from_bytes(row[6]),
            row[7],
            row[8],
            row[9],
            row[10] == 1,
            row[14] == 1,
        )

    async def add_farmer_record(self, farmer_record: FarmerRecord, metadata: RequestMetadata):
        with (await self.pool) as connection:
            cursor = await connection.cursor()
            await cursor.execute(
                "INSERT INTO farmer (launcher_id,p2_singleton_puzzle_hash,delay_time,delay_puzzle_hash,"
                f"authentication_public_key,singleton_tip,singleton_tip_state,points,difficulty,payout_instructions,"
                f"is_pool_member) VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) "
                f"ON DUPLICATE KEY UPDATE p2_singleton_puzzle_hash=%s, delay_time=%s, delay_puzzle_hash=%s,"
                f"authentication_public_key=%s, singleton_tip=%s, singleton_tip_state=%s, payout_instructions=%s, "
                f"is_pool_member=%s",
                (
                    farmer_record.launcher_id.hex(),
                    farmer_record.p2_singleton_puzzle_hash.hex(),
                    farmer_record.delay_time,
                    farmer_record.delay_puzzle_hash.hex(),
                    bytes(farmer_record.authentication_public_key).hex(),
                    bytes(farmer_record.singleton_tip),
                    bytes(farmer_record.singleton_tip_state),
                    farmer_record.points,
                    farmer_record.difficulty,
                    farmer_record.payout_instructions,
                    int(farmer_record.is_pool_member),
                    farmer_record.p2_singleton_puzzle_hash.hex(),
                    farmer_record.delay_time,
                    farmer_record.delay_puzzle_hash.hex(),
                    bytes(farmer_record.authentication_public_key).hex(),
                    bytes(farmer_record.singleton_tip),
                    bytes(farmer_record.singleton_tip_state),
                    farmer_record.payout_instructions,
                    int(farmer_record.is_pool_member),
                ),
            )
            await connection.commit()

    async def get_farmer_record(self, launcher_id: bytes32) -> Optional[FarmerRecord]:
        # TODO(pool): use cache
        with (await self.pool) as connection:
            cursor = await connection.cursor()
            await cursor.execute("SELECT * FROM farmer WHERE launcher_id=%s", (launcher_id.hex()))
            row = await cursor.fetchone()
            if row is None:
                return None
            return self._row_to_farmer_record(row)

    async def update_difficulty(self, launcher_id: bytes32, difficulty: uint64):
        with (await self.pool) as connection:
            cursor = await connection.cursor()
            await cursor.execute(
                "UPDATE farmer SET difficulty=%s WHERE launcher_id=%s", (difficulty, launcher_id.hex())
            )
            await connection.commit()

    async def update_singleton(
        self,
        launcher_id: bytes32,
        singleton_tip: CoinSpend,
        singleton_tip_state: PoolState,
        is_pool_member: bool,
    ):
        if is_pool_member:
            entry = (bytes(singleton_tip), bytes(singleton_tip_state), 1, launcher_id.hex())
        else:
            entry = (bytes(singleton_tip), bytes(singleton_tip_state), 0, launcher_id.hex())
        with (await self.pool) as connection:
            cursor = await connection.cursor()
            await cursor.execute(
                "UPDATE farmer SET singleton_tip=%s, singleton_tip_state=%s, is_pool_member=%s WHERE launcher_id=%s",
                entry,
            )
            await connection.commit()

    async def get_pay_to_singleton_phs(self) -> Set[bytes32]:
        with (await self.pool) as connection:
            cursor = await connection.cursor()
            await cursor.execute("SELECT p2_singleton_puzzle_hash from farmer")
            rows = await cursor.fetchall()
            await cursor.close()

            all_phs: Set[bytes32] = set()
            for row in rows:
                all_phs.add(bytes32(bytes.fromhex(row[0])))
            return all_phs

    async def get_farmer_records_for_p2_singleton_phs(self, puzzle_hashes: Set[bytes32]) -> List[FarmerRecord]:
        if len(puzzle_hashes) == 0:
            return []
        puzzle_hashes_db = tuple([ph.hex() for ph in list(puzzle_hashes)])
        with (await self.pool) as connection:
            cursor = await connection.cursor()
            await cursor.execute(
                f'SELECT * from farmer WHERE p2_singleton_puzzle_hash in ({"%s," * (len(puzzle_hashes_db) - 1)}%s) ',
                puzzle_hashes_db,
            )
            rows = await cursor.fetchall()
            await cursor.close()
            return [self._row_to_farmer_record(row) for row in rows]

    async def get_farmer_points_and_payout_instructions(self, pplns_n_value: int) -> Dict[bytes32, uint64]:
        with (await self.pool) as connection:
            cursor = await connection.cursor()
            await cursor.execute(
                "SELECT partial.difficulty, farmer.payout_instructions, partial.accept_time, partial.launcher_id FROM partial "
                f"INNER JOIN farmer ON partial.launcher_id=farmer.launcher_id WHERE partial.pps=0 "
                f"AND partial.stale=0 AND partial.invalid=0 ORDER BY partial.accept_time "
                f"DESC LIMIT %s",
                (pplns_n_value),
            )
            rows = await cursor.fetchall()
            await cursor.close()
            res: dict[bytes32, uint64] = {}
            for row in rows:
                points: uint64 = uint64(row[0])
                ph: bytes32 = bytes32(bytes.fromhex(row[1]))
                if ph in res:
                    res[ph] += points
                else:
                    res[ph] = points
            return res

    async def get_pps_farmer_points_and_payout_instructions(self, min_points: int) -> List[Tuple[uint64, bytes32]]:
        with (await self.pool) as connection:
            cursor = await connection.cursor()
            await cursor.execute("SELECT points, payout_instructions FROM farmer WHERE points>=%s", (min_points))
            rows = await cursor.fetchall()
            await cursor.close()
            accumulated: Dict[bytes32, uint64] = {}
            for row in rows:
                points: uint64 = uint64(row[0])
                ph: bytes32 = bytes32(bytes.fromhex(row[1]))
                if ph in accumulated:
                    accumulated[ph] += points
                else:
                    accumulated[ph] = points

            ret: List[Tuple[uint64, bytes32]] = []
            for ph, total_points in accumulated.items():
                ret.append((total_points, ph))
            return ret

    async def clear_pps_points(self, min_points: int) -> None:
        with (await self.pool) as connection:
            cursor = await connection.cursor()
            await cursor.execute("UPDATE farmer set points=0 WHERE pps_enabled=1 AND points>=%s", (min_points))
            await connection.commit()
            await cursor.close()

    async def add_partial(
        self,
        launcher_id: bytes32,
        harvester_id: bytes32,
        timestamp: uint64,
        difficulty: uint64,
        payout_instructions: str,
        pps: int,
        stale: Optional[int] = 0,
        invalid: Optional[int] = 0,
    ):
        with (await self.pool) as connection:
            cursor = await connection.cursor()
            await cursor.execute(
                "INSERT INTO partial VALUES(%s, %s, %s, %s, %s,SYSDATE(6),%s,%s,%s)",
                (
                    launcher_id.hex(),
                    timestamp,
                    difficulty,
                    harvester_id.hex(),
                    payout_instructions,
                    pps,
                    stale,
                    invalid,
                ),
            )
            await connection.commit()
            await cursor.close()
        if stale == 0 and invalid == 0:
            cursor = await connection.cursor()
            await cursor.execute(
                "UPDATE farmer set overall_points=overall_points+%s, points=points+%s where launcher_id=%s",
                (difficulty, difficulty, launcher_id.hex()),
            )
            await connection.commit()
            await cursor.close()

    async def get_recent_partials(self, launcher_id: bytes32, count: int) -> List[Tuple[uint64, uint64]]:
        with (await self.pool) as connection:
            cursor = await connection.cursor()
            await cursor.execute(
                "SELECT timestamp, difficulty from partial WHERE launcher_id=%s AND stale=0 AND invalid=0 "
                "ORDER BY timestamp DESC LIMIT %s",
                (launcher_id.hex(), count),
            )
            rows = await cursor.fetchall()
            ret: List[Tuple[uint64, uint64]] = [
                (uint64(timestamp), uint64(difficulty)) for timestamp, difficulty in rows
            ]
            return ret

    async def add_payouts(
        self, block_confirmed: int, payment_targets: List[Dict], transaction_id: bytes32, pps: int
    ) -> None:
        with (await self.pool) as connection:
            for payment_target in payment_targets:
                payout_instructions = payment_target["puzzle_hash"].hex()
                payout: float = payment_target["amount"] / 1000000000000  # convert from mojo to chia
                cursor = await connection.cursor()
                if pps is 1:
                    await cursor.execute(
                        "SELECT launcher_id from farmer where payout_instructions=%s", (payout_instructions)
                    )
                else:
                    await cursor.execute(
                        "SELECT launcher_id from partial where payout_instructions=%s", (payout_instructions)
                    )
                row = await cursor.fetchone()
                await cursor.close()
                if row is not None:  # launcher_id not in db. Probably just the fee address.
                    launcher_id = row[0]
                    cursor = await connection.cursor()

                    await cursor.execute(
                        "INSERT INTO payments(payout_time,block_height,transaction_id,launcher_id,payout_instructions,"
                        f"payout,pps,confirmed) "
                        f"VALUES(SYSDATE(6),%s,%s,%s,%s,%s,%s,0)",
                        (block_confirmed, transaction_id.hex(), launcher_id, payout_instructions, payout, pps),
                    )
                    await connection.commit()
                    await cursor.close()
                    cursor = await connection.cursor()
                    await cursor.execute(
                        "UPDATE farmer SET xch_paid=xch_paid+%s WHERE launcher_id=%s", (payout, launcher_id)
                    )
                    await connection.commit()

    async def confirm_payouts(self, transaction_id: bytes32, block_confirmed: int) -> None:
        with (await self.pool) as connection:
            cursor = await connection.cursor()
            await cursor.execute(
                "UPDATE payments SET confirmed = 1, block_height = %s WHERE transaction_id= %s",
                (block_confirmed, transaction_id.hex()),
            )
            await connection.commit()
            await cursor.close()

    async def get_payment_system(self, launcher_id: bytes32):
        with (await self.pool) as connection:
            cursor = await connection.cursor()
            await cursor.execute(
                "SELECT pps_enabled,pps_change_datetime from farmer where launcher_id=%s", (launcher_id.hex())
            )
            row = await cursor.fetchone()
            await cursor.close()
            result = [row[0] == 1, row[1]]
            return result

    async def add_pool_block(
        self,
        coin_id: bytes32,
        pps: bool,
        amount: float,
        launcher_id: bytes32,
        block_height: int,
        block_timestamp: int,
    ):
        with (await self.pool) as connection:
            cursor = await connection.cursor()
            await cursor.execute(
                "INSERT INTO blocks(coin_id, created_at, block_timestamp, pps, amount, block_height, launcher_id)"
                "VALUES(%s, SYSDATE(6), %s, %s, %s, %s, %s) ON DUPLICATE KEY UPDATE coin_id=%s",
                (coin_id.hex(), block_timestamp, pps, amount, block_height, launcher_id.hex(), coin_id.hex()),
            )
            await connection.commit()
            await cursor.close()
