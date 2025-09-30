from ...models.allowance.internal import MinerAddress
from ...models.allowance.internal import Neuron as NeuronModel
from ..types import MetagraphData, Miner, Neuron, NeuronSnapshotMissing
from ..utils.supertensor import supertensor


def fetch_metagraph_snapshot(block_number: int | None = None) -> MetagraphData:
    target_block = block_number if block_number is not None else supertensor().get_current_block()
    metagraph = supertensor().get_metagraph(target_block)
    if metagraph.block != target_block:
        # Ensure consumers receive a view tagged with the requested block number
        return MetagraphData.model_construct(
            block=target_block,
            neurons=metagraph.neurons,
            subnet_state=metagraph.subnet_state,
            alpha_stake=metagraph.alpha_stake,
            tao_stake=metagraph.tao_stake,
            total_stake=metagraph.total_stake,
            uids=metagraph.uids,
            hotkeys=metagraph.hotkeys,
            coldkeys=metagraph.coldkeys,
            serving_hotkeys=metagraph.serving_hotkeys,
        )
    return metagraph


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
