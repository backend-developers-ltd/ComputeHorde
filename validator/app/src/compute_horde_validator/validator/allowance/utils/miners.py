import ipaddress
import logging

import turbobt

from compute_horde_validator.validator.models import Miner as MinerModel

logger = logging.getLogger(__name__)


def sync_miners_from_neurons(block_number: int, neurons: list[turbobt.Neuron]) -> None:
    """
    Upsert Miner records from neuron data.
    """
    current_hotkeys = [neuron.hotkey for neuron in neurons]
    miners = list(MinerModel.objects.filter(hotkey__in=current_hotkeys).all())
    existing_hotkeys = {m.hotkey for m in miners}
    new_hotkeys = set(current_hotkeys) - existing_hotkeys

    if len(new_hotkeys) > 0:
        new_miners = []
        hotkey_to_neuron = {neuron.hotkey: neuron for neuron in neurons}
        for hotkey in new_hotkeys:
            neuron = hotkey_to_neuron.get(hotkey)
            coldkey = neuron.coldkey if neuron else None
            new_miners.append(MinerModel(hotkey=hotkey, coldkey=coldkey))
        new_miners = MinerModel.objects.bulk_create(new_miners)
        miners.extend(new_miners)
        logger.info(f"Created new neurons: {new_hotkeys}")

    # update axon info of neurons
    miners_to_update = []
    hotkey_to_neuron = {
        neuron.hotkey: neuron
        for neuron in neurons
        if neuron.axon_info and str(neuron.axon_info.ip) != "0.0.0.0"
    }

    for miner in miners:
        neuron = hotkey_to_neuron.get(miner.hotkey)
        if neuron and neuron.axon_info:
            shield_address = getattr(neuron.axon_info, "shield_address", str(neuron.axon_info.ip))
            try:
                ip_version = ipaddress.ip_address(str(neuron.axon_info.ip)).version
            except ValueError:
                ip_version = 4

            if (
                miner.uid != neuron.uid
                or miner.address != shield_address
                or miner.port != neuron.axon_info.port
                or miner.ip_version != ip_version
                or miner.coldkey != neuron.coldkey
            ):
                miner.uid = neuron.uid
                miner.address = shield_address
                miner.port = neuron.axon_info.port
                miner.ip_version = ip_version
                miner.coldkey = neuron.coldkey
                miners_to_update.append(miner)

    if miners_to_update:
        MinerModel.objects.bulk_update(
            miners_to_update, fields=["uid", "address", "port", "ip_version", "coldkey"]
        )
        logger.info(f"Updated axon infos and null coldkeys for {len(miners_to_update)} miners")
