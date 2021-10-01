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
from chia.util.ints import uint64

from .abstract import AbstractPoolStore
from ..record import FarmerRecord
from ..util import RequestMetadata

pymysql.converters.encoders[uint64] = pymysql.converters.escape_int
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
            raise ConnectionError('Unable to Connect to SQL Database.')
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
                "overall_points bigint,"
                "blocks int DEFAULT 0,"
                "xch_paid float DEFAULT 0,"
                "pps_enabled BOOLEAN DEFAULT 0,"
                "pps_change_datetime DATETIME(6),"
                "INDEX (p2_singleton_puzzle_hash))"
            )
        )

        await cursor.execute(
            "CREATE TABLE IF NOT EXISTS partial(launcher_id VARCHAR(256), "
            "timestamp bigint, difficulty bigint, harvester_id VARCHAR(256), "
            "index (timestamp), index (launcher_id))"
        )

        await cursor.execute(
            (
                "CREATE TABLE IF NOT EXISTS pplns_partials("
                "accept_time  DATETIME(6) PRIMARY KEY,"
                "payout_instructions VARCHAR(256),"
                "points int,"
                "launcher_id VARCHAR(256),"
                "index (launcher_id))"

            )
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
                "index (launcher_id))"

            )
        )
        await cursor.execute(
            (
                "CREATE TABLE IF NOT EXISTS blocks("
                "timestamp DATETIME(6) PRIMARY KEY,"
                "block_height bigint,"
                "pps bool,"
                "amount float,"
                "launcher_id VARCHAR(256))"
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
            True if row[10] == 1 else False,
            True if row[11] == 1 else False,
        )

    async def add_farmer_record(self, farmer_record: FarmerRecord, metadata: RequestMetadata):
        with (await self.pool) as connection:
            cursor = await connection.cursor()
            await cursor.execute(
                f"INSERT INTO farmer (launcher_id,p2_singleton_puzzle_hash,delay_time,delay_puzzle_hash,"
                f"authentication_public_key,singleton_tip,singleton_tip_state,points,difficulty,payout_instructions,"
                f"is_pool_member) VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) "
                f"ON DUPLICATE KEY UPDATE launcher_id=%s, p2_singleton_puzzle_hash=%s, "
                f"delay_time=%s, delay_puzzle_hash=%s, authentication_public_key=%s, singleton_tip=%s, "
                f"singleton_tip_state=%s, points=%s, difficulty=%s, payout_instructions=%s,is_pool_member=%s",
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
                ),
            )
            await connection.commit()

    async def get_farmer_record(self, launcher_id: bytes32) -> Optional[FarmerRecord]:
        # TODO(pool): use cache
        with (await self.pool) as connection:
            cursor = await connection.cursor()
            await cursor.execute(
                f"SELECT * FROM farmer WHERE launcher_id=%s", (launcher_id.hex(),),
            )
            row = await cursor.fetchone()
            if row is None:
                return None
            return self._row_to_farmer_record(row)

    async def update_difficulty(self, launcher_id: bytes32, difficulty: uint64):
        with (await self.pool) as connection:
            cursor = await connection.cursor()
            await cursor.execute(
                f"UPDATE farmer SET difficulty=%s WHERE launcher_id=%s", (difficulty, launcher_id.hex())
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
                f"UPDATE farmer SET singleton_tip=%s, singleton_tip_state=%s, is_pool_member=%s WHERE launcher_id=%s",
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
                f"SELECT pplns_partials.points, farmer.payout_instructions, pplns_partials.accept_time FROM pplns_partials "
                f"JOIN farmer ON farmer.launcher_id = pplns_partials.launcher_id AND farmer.pps_enabled=0"
                f" ORDER BY pplns_partials.accept_time DESC LIMIT {pplns_n_value} ")
            rows = await cursor.fetchall()
            await cursor.close()
            ret: dict[bytes32, uint64] = {}
            for row in rows:
                if row[1] in ret:
                    ret[bytes32(bytes.fromhex(row[1]))] = ret[bytes32(bytes.fromhex(row[1]))] + uint64(row[0])
                else:
                    ret[bytes32(bytes.fromhex(row[1]))] = uint64(row[0])
            return ret

    async def get_pps_farmer_points_and_payout_instructions(self, min_points: int) -> List[Tuple[uint64, bytes]]:
        with (await self.pool) as connection:
            cursor = await connection.cursor()
            await cursor.execute(
                f"SELECT sum(pplns_partials.points) AS points,farmer.payout_instructions FROM pplns_partials,farmer "
                f"JOIN farmer ON farmer.launcher_id = pplns_partials.launcher_id AND farmer.pps_enabled=1 AND "
                f"farmer.points>=%s GROUP BY "
                f"pplns_partials.launcher_id", (min_points))
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

    async def add_partial(self, launcher_id: bytes32, harvester_id: bytes32, timestamp: uint64, difficulty: uint64):
        with (await self.pool) as connection:
            cursor = await connection.cursor()
            await cursor.execute("INSERT INTO partial VALUES(%s, %s, %s, %s)", (launcher_id.hex(), timestamp,
                                                                                difficulty, harvester_id.hex()),
                                 )
            await connection.commit()
            await cursor.close()
        with (await self.pool) as connection:
            cursor = await connection.cursor()
            await cursor.execute(
                f"UPDATE farmer set overall_points=overall_points+%s where launcher_id=%s",
                (difficulty, launcher_id.hex())
            )
            await connection.commit()
            await cursor.close()
            cursor = await connection.cursor()
            await cursor.execute(
                f"INSERT INTO pplns_partials(points,launcher_id,accept_time) VALUES(%s, %s,SYSDATE(6))",
                (difficulty, launcher_id.hex())

            )
            await connection.commit()

    async def add_pps_partial(self, launcher_id: bytes32, harvester_id: bytes32, timestamp: uint64, difficulty: uint64):
        with (await self.pool) as connection:
            cursor = await connection.cursor()
            await cursor.execute("INSERT INTO partial VALUES(%s, %s, %s, %s)", (launcher_id.hex(), timestamp,
                                                                                difficulty, harvester_id.hex()),
                                 )
            await connection.commit()
            await cursor.close()
            cursor = await connection.cursor()
            await cursor.execute(
                f"UPDATE farmer set overall_points=overall_points+%s, points=points+%s where launcher_id=%s",
                (difficulty, difficulty, launcher_id.hex()))
            await connection.commit()
            await cursor.close()

    async def get_recent_partials(self, launcher_id: bytes32, count: int) -> List[Tuple[uint64, uint64]]:
        with (await self.pool) as connection:
            cursor = await connection.cursor()
            await cursor.execute(
                "SELECT timestamp, difficulty from partial WHERE launcher_id=%s ORDER BY timestamp DESC LIMIT %s",
                (launcher_id.hex(), count),
            )
            rows = await cursor.fetchall()
            ret: List[Tuple[uint64, uint64]] = [(uint64(timestamp), uint64(difficulty)) for timestamp, difficulty in
                                                rows]
            return ret

    async def add_payouts(self, block_confirmed: int, payment_targets: List, transaction_id: bytes32) -> None:
        with (await self.pool) as connection:
            for index in range(len(payment_targets)):
                payout_instructions = payment_targets[index]["puzzle_hash"]
                payout = payment_targets[index]["amount"]
                cursor = await connection.cursor()
                await cursor.execute(f"SELECT launcher_id from farmer where payout_instructions=%s",
                                     (payout_instructions), )
                row = await cursor.fetchone()
                launcher_id = row[0]
                await cursor.close()
                cursor = await connection.cursor()
                await cursor.execute(
                    f"INSERT INTO payments(payout_time,block_height,transaction_id,launcher_id,payout_instructions,"
                    f"payout) "
                    f"VALUES(SYSDATE(6),%s,%s,%s,%s,%s)",
                    (block_confirmed, transaction_id, launcher_id.hex(), payout_instructions, payout)
                )
                await connection.commit()
                await cursor.close()
                cursor = await connection.cursor()
                await cursor.execute(f"UPDATE farmer set xch_paid=xch_paid+%s where launcher_id=%s",
                                     (payout, launcher_id.hex()))
                await connection.commit()

    async def add_block(self, launcher_id: bytes32) -> None:
        with (await self.pool) as connection:
            cursor = await connection.cursor()
            await cursor.execute(f"UPDATE farmer set blocks=blocks+1 where launcher_id=%s", (launcher_id.hex()))
            await connection.commit()
            await cursor.close()

    async def change_payment_system(self, launcher_id: bytes32, pps_enabled: int):
        with (await self.pool) as connection:
            cursor = await connection.cursor()
            await cursor.execute(f"UPDATE farmer set pps_enabled=%s, pps_change_datetime=SYSDATE(6) "
                                 f"where launcher_id=%s", (pps_enabled, launcher_id.hex()))
            await connection.commit()
            await cursor.close()

    async def get_payment_system(self, launcher_id: bytes32):
        with (await self.pool) as connection:
            cursor = await connection.cursor()
            await cursor.execute("SELECT pps_enabled,pps_change_datetime from farmer where launcher_id=%s",
                                 (launcher_id.hex()))
            row = await cursor.fetchone()
            await cursor.close()
            result = [True if row[0] == 1 else False, row[1]]
            return result

    async def add_pool_block(self, block_height: int, pps: bool, amount: float, launcher_id: bytes32):
        with (await self.pool) as connection:
            cursor = await connection.cursor()
            await cursor.execute("INSERT INTO blocks(timestamp,block_height,pps,amount,launcher_id)"
                                 "VALUES(SYSDATE(6),%s,%s,%s,%s)", (block_height, pps, amount, launcher_id.hex()))
            await connection.commit()
            await cursor.close()


