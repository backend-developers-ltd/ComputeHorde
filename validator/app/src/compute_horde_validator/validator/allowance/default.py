# default implementation of the allowance module interface

import time

from asgiref.sync import sync_to_async
from compute_horde.utils import (
    BAC_VALIDATOR_SS58_ADDRESS,
    MIN_VALIDATOR_STAKE,
    VALIDATORS_LIMIT,
    ValidatorInfo,
)
from compute_horde_core.executor_class import ExecutorClass

from .base import AllowanceBase
from .types import MetagraphData, Miner, Neuron, block_ids, reservation_id, ss58_address
from .utils import blocks, booking, manifests, metagraph
from .utils.spending import (
    InMemorySpendingBookkeeper,
    SpendingBookkeeperBase,
)
from .utils.supertensor import supertensor


class Allowance(AllowanceBase):
    def __init__(self):
        self.my_ss58_address: ss58_address = supertensor().wallet().get_hotkey().ss58_address
        self._metagraph_cache: tuple[float, MetagraphData] | None = None

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

    def get_metagraph(self, block: int | None = None) -> MetagraphData:
        return supertensor().get_metagraph(block)

    async def aget_metagraph(self, block: int | None = None) -> MetagraphData:
        return await sync_to_async(self.get_metagraph)(block)

    def get_serving_hotkeys(self) -> list[str]:
        return self.get_metagraph().serving_hotkeys

    def get_validator_infos(self) -> list[ValidatorInfo]:
        metagraph_data = self.get_metagraph()
        validators = [
            (uid, hotkey, stake)
            for uid, hotkey, stake in zip(
                metagraph_data.uids,
                metagraph_data.hotkeys,
                metagraph_data.total_stake,
            )
            if stake >= MIN_VALIDATOR_STAKE
        ]
        top_validators = sorted(
            validators,
            key=lambda data: (data[1] == BAC_VALIDATOR_SS58_ADDRESS, data[2]),
            reverse=True,
        )[:VALIDATORS_LIMIT]
        return [
            ValidatorInfo(uid=uid, hotkey=hotkey, stake=stake)
            for uid, hotkey, stake in top_validators
        ]

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

    def get_temporary_bookkeeper(self, start_block: int, end_block: int) -> SpendingBookkeeperBase:
        return InMemorySpendingBookkeeper.for_block_range(start_block, end_block)


_allowance_instance: Allowance | None = None


def allowance() -> Allowance:
    global _allowance_instance
    if _allowance_instance is None:
        _allowance_instance = Allowance()
    return _allowance_instance
