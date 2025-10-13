from ...models.allowance.internal import MinerAddress
from ...models.allowance.internal import Neuron as NeuronModel
from ..types import Miner, Neuron, NeuronSnapshotMissing


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
