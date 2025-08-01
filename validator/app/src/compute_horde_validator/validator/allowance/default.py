# default implementation of the allowance module interface
from django.conf import settings

from compute_horde_core.executor_class import ExecutorClass
from .base import AllowanceBase
from .types import ss58_address, reservation_id, block_ids, Miner, Neuron
from .utils import blocks, metagraph
from .utils.supertensor import supertensor


class Allowance(AllowanceBase):
    def __init__(self):
        self.my_ss58_address: ss58_address = supertensor().wallet().get_hotkey().ss58_address

    def reserve_allowance(self, miner: ss58_address, executor_class: ExecutorClass, amount: float,
                          job_start_block: int) -> tuple[reservation_id, block_ids]:
        raise NotImplementedError

    def undo_allowance_reservation(self, reservation_id_: reservation_id) -> None:
        raise NotImplementedError

    def spend_allowance(self, reservation_id_: reservation_id) -> None:
        raise NotImplementedError

    def validate_foreign_receipt(self):
        raise NotImplementedError

    def get_manifests(self) -> dict[ss58_address, dict[ExecutorClass, int]]:
        raise NotImplementedError

    def miners(self) -> list[Miner]:
        return metagraph.miners()

    def neurons(self, block: int) -> list[Neuron]:
        return metagraph.neurons(block)

    def find_miners_with_allowance(self, required_allowance: float, executor_class: ExecutorClass,
                                   job_start_block: int) -> list[tuple[ss58_address, float]]:
        return blocks.find_miners_with_allowance(
            required_allowance=required_allowance,
            executor_class=executor_class,
            job_start_block=job_start_block,
            validator_ss58=self.my_ss58_address,
        )


def allowance() -> Allowance:
    if not allowance.instance:
        allowance.instance = Allowance()
    return allowance.instance

allowance.instance = None
