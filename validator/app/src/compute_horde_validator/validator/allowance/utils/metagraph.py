from ...models.allowance.internal import MinerAddress
from ...models.allowance.internal import Neuron as NeuronModel
from ..types import MetagraphData, Miner, Neuron, NeuronSnapshotMissing
from ..utils.supertensor import supertensor


def fetch_metagraph_snapshot(block_number: int | None = None) -> MetagraphData:
    target_block = block_number if block_number is not None else supertensor().get_current_block()
    neurons = supertensor().list_neurons(target_block)
    subnet_state = supertensor().get_subnet_state(target_block)
    alpha_stake = subnet_state["alpha_stake"]
    tao_stake = subnet_state["tao_stake"]
    total_stake = subnet_state["total_stake"]
    hotkeys = [neuron.hotkey for neuron in neurons]
    coldkeys = [neuron.coldkey for neuron in neurons]
    uids = [neuron.uid for neuron in neurons]
    serving_hotkeys = [
        neuron.hotkey
        for neuron in neurons
        if neuron.axon_info and str(neuron.axon_info.ip) != "0.0.0.0"
    ]
    return MetagraphData(
        block=target_block,
        neurons=neurons,
        subnet_state=subnet_state,
        alpha_stake=alpha_stake,
        tao_stake=tao_stake,
        total_stake=total_stake,
        uids=uids,
        hotkeys=hotkeys,
        coldkeys=coldkeys,
        serving_hotkeys=serving_hotkeys,
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
