from abc import ABC, abstractmethod
from sanic.log import logger
import asyncio
from typing import Optional, Set, List, Tuple, Dict

from chia.pools.pool_wallet_info import PoolState
from chia.types.blockchain_format.sized_bytes import bytes32
from chia.types.coin_spend import CoinSpend
from chia.util.ints import uint64

from ..record import FarmerRecord
from ..util import RequestMetadata


class AbstractPoolStore(ABC):
    """
    Base class for asyncio-related pool stores.
    """

    def __init__(self):
        self.lock = None
        self.connection = None
        self.log = logger

    @abstractmethod
    async def connect(self):
        """Perform IO-related initialization"""

    @abstractmethod
    async def close(self):
        """Close any IO used by the store"""

    @abstractmethod
    async def add_farmer_record(self, farmer_record: FarmerRecord, metadata: RequestMetadata):
        """Persist a new Farmer in the store"""

    @abstractmethod
    async def get_farmer_record(self, launcher_id: bytes32) -> Optional[FarmerRecord]:
        """Fetch a farmer record for given ``launcher_id``. Returns ``None`` if no record found"""

    @abstractmethod
    async def update_difficulty(self, launcher_id: bytes32, difficulty: uint64):
        """Update difficulty for Farmer identified by ``launcher_id``"""

    @abstractmethod
    async def update_singleton(
            self,
            launcher_id: bytes32,
            singleton_tip: CoinSpend,
            singleton_tip_state: PoolState,
            is_pool_member: bool,
    ):
        """Update Farmer's singleton-related data"""

    @abstractmethod
    async def get_pay_to_singleton_phs(self) -> Set[bytes32]:
        """Fetch all puzzle hashes of Farmers in this pool, to scan the blockchain in search of them"""

    @abstractmethod
    async def get_farmer_records_for_p2_singleton_phs(self, puzzle_hashes: Set[bytes32]) -> List[FarmerRecord]:
        """Fetch Farmers matching given puzzle hashes"""

    @abstractmethod
    async def get_farmer_points_and_payout_instructions(self, pplns_n_value: int) -> Dict[bytes32, uint64]:
        """Fetch pplns farmers and their respective payout instructions"""

    @abstractmethod
    async def clear_pps_points(self, min_points: int) -> None:
        """Reset all PPS Farmers' points to 0 that are above the set limit"""
    @abstractmethod
    async def get_pps_farmer_points_and_payout_instructions(self, min_points: int) -> List[Tuple[uint64, bytes]]:
        """Fetch pps farmers and their respective payout instructions that are above the set points limit"""

    @abstractmethod
    async def add_partial(self, launcher_id: bytes32, harvester_id: bytes32, timestamp: uint64, difficulty: uint64):
        """Register new partial and update corresponding Farmer's points"""

    @abstractmethod
    async def add_pps_partial(self, launcher_id: bytes32, harvester_id: bytes32, timestamp: uint64, difficulty: uint64):
        """Register new partial and update corresponding pps Farmer's points"""

    @abstractmethod
    async def get_recent_partials(self, launcher_id: bytes32, count: int) -> List[Tuple[uint64, uint64]]:
        """Fetch last ``count`` partials for Farmer identified by ``launcher_id``"""

    @abstractmethod
    async def add_payouts(self, block_confirmed: int, payment_targets: List, transaction_id: bytes32) -> None:
        """Add new payout records for given Farmer & Register Payouts to farmers in DB"""

    @abstractmethod
    async def add_block(self, launcher_id: bytes32) -> None:
        """Add new block to given Farmer's record"""

    @abstractmethod
    async def change_payment_system(self, launcher_id: bytes32, pps_enabled: int):
        """Enable or disable pps payment system as requested by pps"""

    @abstractmethod
    async def get_payment_system(self, launcher_id: bytes32):
        """Get Current payment system status for a launcher_id"""
