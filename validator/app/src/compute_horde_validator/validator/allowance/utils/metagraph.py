from dataclasses import dataclass

import turbobt

from ...models.allowance.internal import MinerAddress
from ...models.allowance.internal import Neuron as NeuronModel
from ..types import Miner, Neuron, NeuronSnapshotMissing
from ..utils.supertensor import supertensor


@dataclass(slots=True)
class MetagraphSnapshotData:
    neurons: list[turbobt.Neuron]
    subnet_state: turbobt.subnet.SubnetState
    block_number: int


def fetch_metagraph_snapshot(block_number: int | None = None) -> MetagraphSnapshotData:
    target_block = block_number if block_number is not None else supertensor().get_current_block()
    neurons = supertensor().list_neurons(target_block)
    subnet_state = supertensor().get_subnet_state(target_block)
    return MetagraphSnapshotData(
        neurons=neurons,
        subnet_state=subnet_state,
        block_number=target_block,
    )


def get_block_hash(block_number: int) -> str:
    return supertensor().get_block_hash(block_number)


def miners() -> list[Miner]:
    return [
        Miner(
            address=ma.address,
            ip_version=ma.ip_version,
            port=ma.port,
            hotkey_ss58=ma.hotkey_ss58address,
        )
        for ma in MinerAddress.objects.all()
    ]


def neurons(block: int) -> list[Neuron]:
    registered_neurons = NeuronModel.objects.filter(block=block)
    if registered_neurons.count() == 0:
        raise NeuronSnapshotMissing(f"{block=}")
    return [
        Neuron(
            hotkey_ss58=n.hotkey_ss58address,
            coldkey=n.coldkey_ss58address,
        )
        for n in NeuronModel.objects.filter(block=block)
    ]
