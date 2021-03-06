import asyncio
import traceback
from typing import Dict, Optional, List, Tuple

from chia.consensus.block_rewards import calculate_pool_reward
from chia.types.coin_record import CoinRecord
from chia.util.bech32m import decode_puzzle_hash
from chia.util.ints import uint32, uint64
from chia.types.blockchain_format.sized_bytes import bytes32
from chia.wallet.transaction_record import TransactionRecord


from pool.payment_manager.abstract import AbstractPaymentManager
from pool.record import FarmerRecord


class PPSPaymentManager(AbstractPaymentManager):
    """
    Default payment manager
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # Don't scan anything before this height, for efficiency (for example pool start date)
        self.pps_share_price: float = 0
        self.points_for_min_payout: int = 0
        self.scan_start_height: uint32 = uint32(self._pool_config["scan_start_height"])

        # After this many confirmations, a transaction is considered final and irreversible
        self.confirmation_security_threshold = self._pool_config["confirmation_security_threshold"]

        # Interval for checking for new farmers if standalone mode is activated.
        self.standalone_search_interval = self._pool_config["standalone_search_interval"]

        # Interval for making payout transactions to pps farmers
        self.pps_payment_interval = self._pool_config["pps_payment_interval"]

        # We will not make transactions with more targets than this, to ensure our transaction gets into the blockchain
        # faster.
        self.max_additions_per_transaction = self._pool_config["max_additions_per_transaction"]

        self.pps_target_puzzle_hash: bytes32 = bytes32(decode_puzzle_hash(self._pool_config["pps_target_address"]))
        self.pps_wallet_address = self._pool_config["pps_target_address"]

        self.pps_fee = self._pool_config["pps_fee"]

        # The pool fees will be sent to this address. This MUST be on a different key than the target_puzzle_hash,
        # otherwise, the fees will be sent to the users.
        self.pool_fee_puzzle_hash: bytes32 = bytes32(decode_puzzle_hash(self._pool_config["pool_fee_address"]))

        # This is the wallet fingerprint and ID for the wallet spending the funds from `self.default_target_puzzle_hash`
        self.pps_wallet_fingerprint = self._pool_config["pps_wallet_fingerprint"]
        self.pps_wallet_id = self._pool_config["pps_wallet_id"]

        # This is the minimum payout in xch
        self.pps_min_payout = self._pool_config["pps_wallet_id"]
        # This is the list of payments that we have not sent yet, to farmers
        self.pps_pending_payments: Optional[asyncio.Queue] = None

        self.update_pps_price_loop_task: Optional[asyncio.Task] = None
        self.create_pps_payment_loop_task: Optional[asyncio.Task] = None
        self.pps_submit_payment_loop_task: Optional[asyncio.Task] = None

    async def start(self, *args, **kwargs):
        await super().start(*args, **kwargs)
        self.pps_pending_payments = asyncio.Queue()

        self.update_pps_price_loop_task = asyncio.create_task(self.update_pps_price_loop())
        self.create_pps_payment_loop_task = asyncio.create_task(self.create_pps_payment_loop())
        self.pps_submit_payment_loop_task = asyncio.create_task(self.pps_submit_payment_loop())

    async def stop(self):
        if self.update_pps_price_loop_task is not None:
            self.update_pps_price_loop_task.cancel()
        if self.create_pps_payment_loop_task is not None:
            self.create_pps_payment_loop_task.cancel()
        if self.pps_submit_payment_loop_task is not None:
            self.pps_submit_payment_loop_task.cancel()

    def register_new_farmer(self, farmer_record: FarmerRecord):
        ...  # do nothing

    async def update_pps_price_loop(self):
        while True:
            if not self._state_keeper.blockchain_state["sync"]["synced"]:
                self._logger.warning("Not synced, waiting")
                await asyncio.sleep(60)
            # convert bytes to TiB from network stats
            netspace_tib = self._state_keeper.blockchain_state["space"] / 1.1e12  # scientific notation
            # get mojo a day.
            xch_daily = 4608 * calculate_pool_reward(uint32(1))
            # get mojo per tib.
            price_tib = xch_daily / netspace_tib
            self.pps_share_price = price_tib / 100  # price per share in mojo
            xch_per_point = self.pps_share_price / 1000000000000  # price per share in XCH
            self.points_for_min_payout = int(self.pps_min_payout / xch_per_point)
            self._logger.info(f"Updated Price Per share to {price_tib / 1000000000000} XCH per TiB")
            await asyncio.sleep(240)

    async def create_pps_payment_loop(self):
        """
        Calculates the points of each pps farmer, and splits the total funds received into coins for each farmer.
        Saves the transactions that we should make, to `amount_to_distribute`.
        """
        while True:
            try:
                if not self._state_keeper.blockchain_state["sync"]["synced"]:
                    self._logger.warning("Not synced, waiting")
                    await asyncio.sleep(60)
                    continue

                if self.pps_pending_payments.qsize() != 0:
                    self._logger.warning(f"PPS: Pending payments ({self.pps_pending_payments.qsize()}), waiting")
                    await asyncio.sleep(60)
                    continue

                if self.pps_share_price == 0:
                    self._logger.warning("PPS Share price not set, waiting.")
                    await asyncio.sleep(60)
                    continue

                self._logger.info("Starting to create pps payments")
                async with self._wallet_lock:
                    coin_records: List[CoinRecord] = await self._node_rpc_client.get_coin_records_by_puzzle_hash(
                        self.pps_target_puzzle_hash,
                        include_spent_coins=False,
                        start_height=self.scan_start_height,
                    )

                if len(coin_records) == 0:
                    self._logger.info("No PPS funds to distribute.")
                    await asyncio.sleep(120)
                    continue

                total_amount_claimed = sum([c.coin.amount for c in coin_records])
                pool_coin_amount = int(total_amount_claimed * self.pps_fee)
                amount_to_distribute = total_amount_claimed - pool_coin_amount

                self._logger.info(f"PPS:Total amount claimed: {total_amount_claimed / (10 ** 12)}")
                self._logger.info(f"PPS:Pool coin amount (includes blockchain fee) {pool_coin_amount / (10 ** 12)}")
                self._logger.info(f"PPS:Total amount to distribute: {amount_to_distribute / (10 ** 12)}")

                async with self._store.lock:
                    # Get the points of each farmer, as well as payout instructions. Here a chia address is used,
                    # but other blockchain addresses can also be used.
                    min_points = self.points_for_min_payout
                    points_and_ph: List[
                        Tuple[uint64, bytes]
                    ] = await self._store.get_pps_farmer_points_and_payout_instructions(min_points)
                    total_points = sum([pt for (pt, ph) in points_and_ph])
                    if total_points > 0:
                        mojo_per_point = self.pps_share_price
                        self._logger.info(f"Paying out {mojo_per_point} mojo / point for pps")
                        if total_points * mojo_per_point > amount_to_distribute:
                            self._logger.info(
                                f"Do not have enough pps funds to distribute: {total_amount_claimed / (10 ** 12)}, "
                                "skipping payout"
                            )
                            await asyncio.sleep(self.pps_payment_interval)
                            continue
                        additions_sub_list: List[Dict] = [
                            {"puzzle_hash": self.pool_fee_puzzle_hash, "amount": pool_coin_amount}
                        ]
                        for points, ph in points_and_ph:
                            if points > 0:
                                additions_sub_list.append({"puzzle_hash": ph, "amount": int(points * mojo_per_point)})

                            if len(additions_sub_list) == self.max_additions_per_transaction:
                                await self.pps_pending_payments.put(additions_sub_list.copy())
                                self._logger.info(f"PPS:Will make payments: {additions_sub_list}")
                                additions_sub_list = []

                        if len(additions_sub_list) > 0:
                            self._logger.info(f"PPS:Will make payments: {additions_sub_list}")
                            await self.pps_pending_payments.put(additions_sub_list.copy())

                        # Subtract the points from each pps farmer
                        await self._store.clear_pps_points(min_points)
                    else:
                        self._logger.info(f"PPS: No points for any farmer. Waiting {self.pps_payment_interval}")

                await asyncio.sleep(self.pps_payment_interval)
            except asyncio.CancelledError:
                self._logger.info("Cancelled create_pps_payment_loop, closing")
                return
            except Exception as e:
                error_stack = traceback.format_exc()
                self._logger.error(f"Unexpected error in create_pps_payment_loop: {e} {error_stack}")
                await asyncio.sleep(self.pps_payment_interval)

    async def pps_submit_payment_loop(self):
        while True:
            try:
                peak_height = self._state_keeper.blockchain_state["peak"].height
                await self._wallet_rpc_client.log_in_and_skip(fingerprint=self.pps_wallet_fingerprint)
                if not self._state_keeper.blockchain_state["sync"]["synced"] or not self._state_keeper.wallet_synced:
                    self._logger.warning("Waiting for wallet sync")
                    await asyncio.sleep(60)
                    continue

                if self.pps_pending_payments.empty() is False:
                    async with self._wallet_lock:

                        payment_targets = await self.pps_pending_payments.get()
                        assert len(payment_targets) > 0

                        self._logger.info(f"Submitting a pps payment: {payment_targets}")

                        # TODO(pool): make sure you have enough to pay the blockchain fee, this will be taken out of the
                        # pool fee itself. Alternatively you can set it to 0 and wait longer
                        # blockchain_fee = 0.00001 * (10 ** 12) * len(payment_targets)
                        blockchain_fee: uint64 = uint64(0)
                        try:
                            transaction: TransactionRecord = await self._wallet_rpc_client.send_transaction_multi(
                                self.pps_wallet_id, payment_targets, fee=blockchain_fee
                            )
                        except ValueError as e:
                            self._logger.error(f"Error making pps payment: {e}")
                            await asyncio.sleep(10)
                            await self.pps_pending_payments.put(payment_targets)
                            continue

                        self._logger.info(f"pps Transaction: {transaction}")
                        try:
                            await self._store.add_payouts(0, payment_targets, transaction.name, 1)
                            self._logger.info("Successfully added pps payments to Database")
                        except Exception as e:
                            self._logger.error(f"Error adding pps payouts to database: {e}")

                        while (
                            not transaction.confirmed
                            or not (peak_height - transaction.confirmed_at_height)
                            > self.confirmation_security_threshold
                        ):
                            transaction = await self._wallet_rpc_client.get_transaction(
                                self.pps_wallet_id, transaction.name
                            )
                            peak_height = self._state_keeper.blockchain_state["peak"].height
                            self._logger.info(
                                "Waiting for pps transaction to obtain "
                                f"{self.confirmation_security_threshold} confirmations"
                            )
                            if not transaction.confirmed:
                                self._logger.info(f"Not confirmed. In mempool? {transaction.is_in_mempool()}")
                            else:
                                self._logger.info(f"Confirmations: {peak_height - transaction.confirmed_at_height}")
                            await asyncio.sleep(10)

                        self._logger.info(f"Successfully confirmed pps payments {payment_targets}")
                        # add payouts to db
                        try:
                            await self._store.confirm_payouts(transaction.name, transaction.confirmed_at_height)
                            self._logger.info("PPS Payouts were marked confirmed in the Database ")
                        except Exception as e:
                            self._logger.error(f"Error marking pps payouts confirmed in the Database: {e}")
                else:
                    await asyncio.sleep(60)

            except asyncio.CancelledError:
                self._logger.info("Cancelled pps_submit_payment_loop, closing")
                return
            except Exception as e:
                # TODO(pool): retry transaction if failed
                self._logger.error(f"Unexpected error in pps_submit_payment_loop: {e}")
                await asyncio.sleep(60)
