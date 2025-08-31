import datetime
from abc import abstractmethod, ABC
from collections import defaultdict
from dataclasses import dataclass
from typing import NamedTuple

from compute_horde_core.executor_class import ExecutorClass
from ..types import ss58_address, block_ids, block_id
from ...models import Block
from .. import settings

validator_ss58 = ss58_address
miner_ss58 = ss58_address
amount = float


class SpendingIssue:
    pass


@dataclass
class BlocksOutsideRange(SpendingIssue):
    allowed_range: range
    blocks_outside_range: block_ids


@dataclass
class DoubleSpentBlocks(SpendingIssue):
    double_spent_blocks: block_ids


@dataclass
class InvalidatedBlocks(SpendingIssue):
    invalidated_blocks: block_ids


@dataclass
class InsufficientAllowance(SpendingIssue):
    spendable_amount: amount
    blocks: block_ids


class NotEnoughAllowance(Exception):
    def __init__(self, issues: list[SpendingIssue]):
        self.issues = issues


class ValidatorMinerExecutor(NamedTuple):
    validator: validator_ss58
    miner: miner_ss58
    executor_class: ExecutorClass


class AllowanceInfo(NamedTuple):
    allowance: amount
    invalidated_at_block: block_id | None


class SpendingBookkeeperBase(ABC):
    def spend(
        self,
        triplet: ValidatorMinerExecutor,
        spend_time: datetime.datetime,
        payment_blocks: block_ids,
        spend_amount: float,
    ) -> list[SpendingIssue]:
        """
        Validate the next spending and register it if valid.
        Returns a list of spending issues (empty if no issues).
        Throws SpendingImpossible if spending is impossible - also includes issues.
        """
        issues: list[SpendingIssue] = []

        # Filter out blocks that are either expired or in the future relative to the spending time
        block_at_spend_time = self._get_block_at_time(spend_time)
        allowed_block_range = range(
            block_at_spend_time - settings.BLOCK_EXPIRY, block_at_spend_time + 1
        )
        blocks_in_range = {b for b in payment_blocks if b in allowed_block_range}

        # Find out which blocks are already spent or invalidated
        already_spent_blocks = self._check_for_spent_blocks(triplet, blocks_in_range)
        invalidated_blocks = self._check_for_invalidated_blocks(
            triplet, blocks_in_range, block_at_spend_time
        )

        # Figure out which allowances are available for spending in this case
        spendable_allowances = self._get_blocks_allowances(
            triplet, blocks_in_range - already_spent_blocks - invalidated_blocks
        )

        # Derive issue lists from the sets
        if blocks_outside_range := set(payment_blocks) - set(blocks_in_range):
            issues.append(BlocksOutsideRange(allowed_block_range, list(blocks_outside_range)))
        if already_spent_blocks:
            issues.append(DoubleSpentBlocks(list(already_spent_blocks)))
        if invalidated_blocks:
            issues.append(InvalidatedBlocks(list(invalidated_blocks)))

        # Finally, check if all the blocks that can be spent amount to enough allowance
        spendable_amount = sum(spendable_allowances.values())
        if spendable_amount < spend_amount:
            # Not enough allowance to cover the spending. Fail and bail.
            issues.append(
                InsufficientAllowance(spendable_amount, list(spendable_allowances.keys()))
            )
            raise NotEnoughAllowance(issues)
        else:
            # We have enough allowance. Register the spending.
            self._register_transaction(triplet, list(spendable_allowances.keys()), spend_time)
            return issues

    @abstractmethod
    def _get_blocks_allowances(
        self,
        triplet: ValidatorMinerExecutor,
        blocks: set[block_id],
    ) -> dict[block_id, amount]:
        """Get allowance amounts for multiple blocks for specific validator/miner/executor_class combination."""
        pass

    @abstractmethod
    def _check_for_spent_blocks(
        self,
        triplet: ValidatorMinerExecutor,
        blocks: set[block_id],
    ) -> set[block_id]:
        """Get set of blocks that have already been spent for the triplet."""
        pass

    @abstractmethod
    def _check_for_invalidated_blocks(
        self,
        triplet: ValidatorMinerExecutor,
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
        triplet: ValidatorMinerExecutor,
        blocks: block_ids,
        spent_at: datetime.datetime,
    ) -> None:
        """Register a transaction with common info for multiple blocks."""
        pass


class InMemorySpendingBookkeeper(SpendingBookkeeperBase):
    def __init__(
        self,
        known_allowances: dict[ValidatorMinerExecutor, dict[block_id, AllowanceInfo]],
        blocks: list[Block],
    ) -> None:
        self._allowances = known_allowances
        self._blocks = blocks
        self._spendings_per_triplet: defaultdict[ValidatorMinerExecutor, set[block_id]] = (
            defaultdict(set)
        )

    def _get_blocks_allowances(
        self, triplet: ValidatorMinerExecutor, blocks: set[block_id]
    ) -> dict[block_id, float]:
        triplet_allowances = self._allowances.get(triplet, {})
        return {
            block: allowance.allowance
            for block in blocks
            if (allowance := triplet_allowances.get(block)) is not None
        }

    def _check_for_spent_blocks(
        self, triplet: ValidatorMinerExecutor, blocks: set[block_id]
    ) -> set[block_id]:
        spent_blocks = self._spendings_per_triplet.get(triplet, set())
        return spent_blocks & set(blocks)

    def _check_for_invalidated_blocks(
        self, triplet: ValidatorMinerExecutor, blocks: set[block_id], at_block: block_id
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
        triplet: ValidatorMinerExecutor,
        blocks: list[block_id],
        spent_at: datetime.datetime,
    ) -> None:
        self._spendings_per_triplet[triplet].update(blocks)

    def _get_block_at_time(self, time: datetime.datetime) -> block_id | None:
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
