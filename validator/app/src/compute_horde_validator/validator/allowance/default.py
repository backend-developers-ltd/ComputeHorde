# default implementation of the allowance module interface

from compute_horde_core.executor_class import ExecutorClass

from .base import AllowanceBase
from .types import Miner, Neuron, block_ids, reservation_id, ss58_address
from .utils import blocks, booking, manifests, metagraph
from .utils.supertensor import supertensor


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

    def get_latest_finalized_block(self) -> int:
        return supertensor().get_latest_finalized_block()

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


_allowance_instance: Allowance | None = None


def allowance() -> Allowance:
    global _allowance_instance
    if _allowance_instance is None:
        _allowance_instance = Allowance()
    return _allowance_instance
