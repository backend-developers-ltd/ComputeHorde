# default implementation of the allowance module interface

from compute_horde_core.executor_class import ExecutorClass

from .base import AllowanceBase
from .types import Miner, Neuron, block_ids, reservation_id, ss58_address
from .utils import blocks, booking, manifests, metagraph
from .utils.spending import (
    SpendingBookkeeperBase,
    ValiMinerExeclassBlock,
    AllowanceInfo,
    InMemorySpendingBookkeeper,
)
from .utils.supertensor import supertensor
from . import settings
from ..models import BlockAllowance, Block


class Allowance(AllowanceBase):
    def __init__(self):
        self.my_ss58_address: ss58_address = supertensor().wallet().get_hotkey().ss58_address

    def reserve_allowance(
        self,
        miner: ss58_address,
        executor_class: ExecutorClass,
        allowance_seconds: float,
        job_start_block: int,
    ) -> tuple[reservation_id, block_ids]:
        return booking.reserve_allowance(
            miner=miner,
            validator=self.my_ss58_address,
            executor_class=executor_class,
            allowance_seconds=allowance_seconds,
            job_start_block=job_start_block,
        )

    def undo_allowance_reservation(self, reservation_id_: reservation_id) -> None:
        booking.undo_allowance_reservation(reservation_id_)

    def spend_allowance(self, reservation_id_: reservation_id) -> None:
        booking.spend_allowance(reservation_id_)

    def validate_foreign_receipt(self):
        raise NotImplementedError

    def get_current_block(self) -> int:
        return supertensor().get_current_block()

    def get_manifests(self) -> dict[ss58_address, dict[ExecutorClass, int]]:
        return manifests.get_current_manifests()

    def miners(self) -> list[Miner]:
        return metagraph.miners()

    def neurons(self, block: int) -> list[Neuron]:
        return metagraph.neurons(block)

    def find_miners_with_allowance(
        self, allowance_seconds: float, executor_class: ExecutorClass, job_start_block: int
    ) -> list[tuple[ss58_address, float]]:
        return blocks.find_miners_with_allowance(
            allowance_seconds=allowance_seconds,
            executor_class=executor_class,
            job_start_block=job_start_block,
            validator_ss58=self.my_ss58_address,
        )

    def get_spending_validator(self, block_start: int, block_end: int) -> SpendingBookkeeperBase:
        # TODO: Check for off-by-one errors - lower and upper
        lower_bound = block_start - settings.BLOCK_EXPIRY
        upper_bound = block_end + 1

        block_allowances_qs = (
            BlockAllowance.objects.filter(
                block_id__gte=lower_bound,
                block_id__lte=upper_bound + 1,  # So that we know when the last block ends
                allowance__gt=0,
            )
            .filter(
                Q(allowance_booking__is_spent=False) | Q(allowance_booking__isnull=True),
            )
            .values(
                "validator_ss58",
                "miner_ss58",
                "executor_class",
                "block",
                "allowance",
                "invalidated_at_block",
            )
        )

        allowances = {}

        for row in block_allowances_qs:
            allowances[
                ValiMinerExeclassBlock(
                    validator=row["validator_ss58"],
                    miner=row["miner_ss58"],
                    block=row["block"],
                    executor_class=ExecutorClass(row["executor_class"]),
                )
            ] = AllowanceInfo(
                allowance=row["allowance"],
                invalidated_at_block=row["invalidated_at_block"],
            )

        blocks = [
            block
            for block in Block.objects.filter(
                block_number__gte=lower_bound,
                block_number__lte=upper_bound,
            ).order_by("block_number")
        ]

        return InMemorySpendingBookkeeper(allowances, blocks)


_allowance_instance: Allowance | None = None


def allowance() -> Allowance:
    global _allowance_instance
    if _allowance_instance is None:
        _allowance_instance = Allowance()
    return _allowance_instance
