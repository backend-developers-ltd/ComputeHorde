import datetime
from abc import abstractmethod, ABC
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
class UnknownBlocks(SpendingIssue):
    unknown_blocks: block_ids


@dataclass
class InvalidatedBlocks(SpendingIssue):
    invalidated_blocks: block_ids


@dataclass
class InsufficientAllowance(SpendingIssue):
    spendable_amount: amount
    blocks: block_ids


class SpendingValidatorBase(ABC):
    @abstractmethod
    def validate_next(
        self,
        validator: validator_ss58,
        miner: miner_ss58,
        executor_class: ExecutorClass,
        spend_time: datetime.datetime,
        blocks: block_ids,
        amount: float,
    ) -> tuple[bool, list[SpendingIssue]]:
        """
        Validate the next spending.
        Returns tuple:
            - True/False based on whether valid blocks were enough to spend the given amount
            - List of spending issues (empty if no issues).
        """


class ValiMinerExeclassBlock(NamedTuple):
    validator: validator_ss58
    miner: miner_ss58
    block: block_id
    executor_class: ExecutorClass


class AmountAndInvalidation(NamedTuple):
    allowance: amount
    invalidated_at_block: block_id | None


class SpendingValidator(SpendingValidatorBase):
    def __init__(
        self,
        known_allowances: dict[ValiMinerExeclassBlock, AmountAndInvalidation],
        blocks: list[Block],
    ) -> None:
        self._allowances = known_allowances
        self._blocks: list[Block] = blocks
        self._previously_spent: set[ValiMinerExeclassBlock] = set()

    def validate_next(
        self,
        validator: validator_ss58,
        miner: miner_ss58,
        executor_class: ExecutorClass,
        spend_time: datetime.datetime,
        blocks: block_ids,
        amount: float,
    ) -> tuple[bool, list[SpendingIssue]]:
        issues: list[SpendingIssue] = []

        # Filter out blocks that are either expired or in the future relative to the spending time
        block_at_spend_time = self._get_block_at_time(spend_time)
        allowed_range = range(block_at_spend_time - settings.BLOCK_EXPIRY, block_at_spend_time + 1)
        blocks_in_range = [b for b in blocks if b in allowed_range]
        if block_outside_range := set(blocks) - set(blocks_in_range):
            issues.append(BlocksOutsideRange(allowed_range, list(block_outside_range)))

        # Check each block within range and filter out spent and invalidated blocks
        double_spent_blocks: block_ids = []
        unknown_blocks: block_ids = []
        invalidated_blocks: block_ids = []
        spendable_allowances: dict[ValiMinerExeclassBlock, AmountAndInvalidation] = {}
        keys_in_range = [
            ValiMinerExeclassBlock(validator, miner, block, executor_class)
            for block in blocks_in_range
        ]
        for key in keys_in_range:
            if key not in self._allowances:
                # unknown allowance - could have been a 0, or already spend before the validation started
                unknown_blocks.append(key.block)
                continue

            if key in self._previously_spent:
                # allowance already spent during this validation
                double_spent_blocks.append(key.block)
                continue

            allowance = self._allowances[key]

            if allowance.invalidated_at_block is not None and allowance.invalidated_at_block <= block_at_spend_time:
                invalidated_blocks.append(allowance.invalidated_at_block)
                continue

            spendable_allowances[key] = allowance

        if unknown_blocks:
            issues.append(UnknownBlocks(unknown_blocks))
        if double_spent_blocks:
            issues.append(DoubleSpentBlocks(double_spent_blocks))
        if invalidated_blocks:
            issues.append(InvalidatedBlocks(invalidated_blocks))

        spendable_amount = sum(allowance.allowance for allowance in spendable_allowances.values())
        if spendable_amount < amount:
            # Not enough allowance to cover the spending. Fail and bail.
            issues.append(InsufficientAllowance(spendable_amount, [k.block for k in spendable_allowances.keys()]))
            return False, issues

        # We have enough allowance. Register the spending.
        self._previously_spent.update(spendable_allowances.keys())
        return True, issues

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
