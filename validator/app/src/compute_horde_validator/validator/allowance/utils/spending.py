import datetime
import logging
from abc import ABC, abstractmethod
from collections import defaultdict
from typing import NamedTuple

from compute_horde_core.executor_class import ExecutorClass

from ...models import Block, BlockAllowance
from .. import settings
from ..types import (
    CannotSpend,
    ErrorWhileSpending,
    SpendingDetails,
    block_id,
    block_ids,
    ss58_address,
)

logger = logging.getLogger(__name__)

validator_ss58 = ss58_address
miner_ss58 = ss58_address
amount = float


class Triplet(NamedTuple):
    validator: validator_ss58
    miner: miner_ss58
    executor_class: ExecutorClass


class AllowanceInfo(NamedTuple):
    allowance: amount
    invalidated_at_block: block_id | None = None


class SpendingBookkeeperBase(ABC):
    """
    Base class for spending bookkeepers.
    A spending bookkeeper is used to validate a series of allowance spendings.
    """

    def spend(
        self,
        triplet: Triplet,
        spend_time: datetime.datetime,
        offered_blocks: block_ids,
        spend_amount: float,
    ) -> SpendingDetails:
        """
        Validate the spending attempt and register it if valid.

        Raises:
            CannotSpend: spending impossible due to insufficient allowance/invalidations
            ErrorWhileSpending: non-allowance-related error during spending
        """
        # Filter out blocks that are either expired or in the future relative to the spending time
        block_at_spend_time = self._get_block_at_time(spend_time)
        if block_at_spend_time is None:
            raise ErrorWhileSpending(f"Cannot find block at job submission time: {spend_time}")

        allowed_block_range = range(
            block_at_spend_time - settings.BLOCK_EXPIRY, block_at_spend_time + 1
        )
        blocks_in_range = {b for b in offered_blocks if b in allowed_block_range}

        # Find out which blocks are already spent or invalidated
        already_spent_blocks = self._check_for_spent_blocks(triplet, blocks_in_range)
        invalidated_blocks = self._check_for_invalidated_blocks(
            triplet, blocks_in_range, block_at_spend_time
        )

        # Figure out which allowances are available for spending in this case
        spendable_allowances = self._get_blocks_allowances(
            triplet, blocks_in_range - already_spent_blocks - invalidated_blocks
        )

        # Organize the data
        spending_details = SpendingDetails(
            requested_amount=spend_amount,
            offered_blocks=offered_blocks,
            spendable_amount=sum(spendable_allowances.values()),
            spent_blocks=[],  # Fill this in later if the spending is valids
            outside_range_blocks=list(set(offered_blocks) - set(blocks_in_range)),
            double_spent_blocks=list(already_spent_blocks),
            invalidated_blocks=list(invalidated_blocks),
        )

        # Finally, check if all the blocks that can be spent amount to enough allowance
        if spending_details.spendable_amount < spending_details.requested_amount:
            # Not enough allowance to cover the spending. Fail and bail.
            raise CannotSpend(spending_details)

        # We have enough allowance. Register the spending.
        spending_details.spent_blocks = list(spendable_allowances.keys())
        self._register_transaction(triplet, spending_details.spent_blocks)
        return spending_details

    @abstractmethod
    def _get_blocks_allowances(
        self,
        triplet: Triplet,
        blocks: set[block_id],
    ) -> dict[block_id, amount]:
        """Get allowance amounts for blocks for specific triplet."""
        pass

    @abstractmethod
    def _check_for_spent_blocks(
        self,
        triplet: Triplet,
        blocks: set[block_id],
    ) -> set[block_id]:
        """Get set of blocks that have already been spent for the triplet."""
        pass

    @abstractmethod
    def _check_for_invalidated_blocks(
        self,
        triplet: Triplet,
        blocks: set[block_id],
        at_block: block_id,
    ) -> set[block_id]:
        """Get set of blocks that were invalidated at or before the given block number."""
        pass

    @abstractmethod
    def _get_block_at_time(self, time: datetime.datetime) -> block_id | None:
        """Get the block ID at the given time."""
        pass

    @abstractmethod
    def _register_transaction(
        self,
        triplet: Triplet,
        blocks: block_ids,
    ) -> None:
        """Register a transaction. Does not validate the transaction."""
        pass


class InMemorySpendingBookkeeper(SpendingBookkeeperBase):
    """
    In-memory implementation of a spending bookkeeper.
    Requires prior knowledge - a "starting state" - of the allowances and blocks.
    For efficiency, only load allowance and block data that is relevant for the time range you're validating.
    Or better yet, use the for_cycle factory method.
    """

    def __init__(
        self,
        known_allowances: dict[Triplet, dict[block_id, AllowanceInfo]],
        blocks: list[Block],
    ) -> None:
        self._allowances = known_allowances
        self._blocks = blocks
        self._spendings_per_triplet: defaultdict[Triplet, set[block_id]] = defaultdict(set)

    @classmethod
    def for_block_range(cls, start_block: int, end_block: int) -> "InMemorySpendingBookkeeper":
        """
        Factory method to create a spending bookkeeper for validating spendings on jobs that finished within the given
        block range.
        The actual range of loaded data will be extended to include blocks that may be used for spending.
        End block is exclusive.
        """
        lowest_interesting_block = (
            start_block - settings.MAX_JOB_RUN_TIME_BLOCKS_APPROX - settings.BLOCK_EXPIRY
        )
        highest_interesting_block = end_block - 1

        logger.info(f"Creating spending bookkeeper for blocks {start_block} to {end_block}")
        logger.info(
            f"Loading allowance data for blocks {lowest_interesting_block} to {highest_interesting_block} inclusive"
        )

        block_allowances_qs = BlockAllowance.objects.filter(
            block_id__gte=lowest_interesting_block,
            block_id__lte=highest_interesting_block,
            allowance__gt=0,
        ).values(
            "validator_ss58",
            "miner_ss58",
            "executor_class",
            "block",
            "allowance",
            "invalidated_at_block",
        )

        allowances: defaultdict[Triplet, dict[block_id, AllowanceInfo]] = defaultdict(dict)
        for row in block_allowances_qs:
            block = row["block"]
            triplet = Triplet(
                row["validator_ss58"], row["miner_ss58"], ExecutorClass(row["executor_class"])
            )
            info = AllowanceInfo(row["allowance"], row["invalidated_at_block"])
            allowances[triplet][block] = info

        logger.info(
            f"Loading block data for blocks {lowest_interesting_block} to {highest_interesting_block + 1} inclusive"
        )
        blocks = [
            *Block.objects.filter(
                block_number__gte=lowest_interesting_block,
                # We need 1 more to know when highest_interesting_block ends
                block_number__lte=highest_interesting_block + 1,
            ).order_by("block_number")
        ]

        return InMemorySpendingBookkeeper(known_allowances=allowances, blocks=blocks)

    def _get_blocks_allowances(
        self, triplet: Triplet, blocks: set[block_id]
    ) -> dict[block_id, float]:
        triplet_allowances = self._allowances.get(triplet, {})
        return {
            block: allowance.allowance
            for block in blocks
            if (allowance := triplet_allowances.get(block)) is not None
        }

    def _check_for_spent_blocks(self, triplet: Triplet, blocks: set[block_id]) -> set[block_id]:
        spent_blocks = self._spendings_per_triplet.get(triplet, set())
        return spent_blocks & set(blocks)

    def _check_for_invalidated_blocks(
        self, triplet: Triplet, blocks: set[block_id], at_block: block_id
    ) -> set[block_id]:
        triplet_allowances = self._allowances.get(triplet, {})
        invalidated_blocks = set()
        for block in blocks:
            allowance = triplet_allowances.get(block)
            if (
                allowance
                and allowance.invalidated_at_block is not None
                and allowance.invalidated_at_block <= at_block
            ):
                invalidated_blocks.add(block)
        return invalidated_blocks

    def _register_transaction(
        self,
        triplet: Triplet,
        blocks: list[block_id],
    ) -> None:
        self._spendings_per_triplet[triplet].update(blocks)

    def _get_block_at_time(self, time: datetime.datetime) -> block_id | None:
        """
        Binary search for the block at the given time.
        Returns the block number of the latest block as of the given point in time. (not the closest one)
        Returns None if no block is found or there is not enough data to find a block.
        """
        # We need at least two blocks: one real block and a trailing bound block
        if len(self._blocks) < 2:
            return None

        # Bounds: the first block starts the searchable range; the last block is a bound
        if time < self._blocks[0].creation_timestamp:
            return None
        if time >= self._blocks[-1].creation_timestamp:
            return None

        # Search only among [0, len(_blocks) - 2]; the last element is never returned
        left = 0
        right = len(self._blocks) - 2
        result_idx: int | None = None

        # Find the greatest index i such that creation_timestamp[i] <= time
        while left <= right:
            mid = (left + right) // 2
            mid_ts = self._blocks[mid].creation_timestamp
            if mid_ts <= time:
                result_idx = mid
                left = mid + 1
            else:
                right = mid - 1

        if result_idx is None:
            return None

        return self._blocks[result_idx].block_number
