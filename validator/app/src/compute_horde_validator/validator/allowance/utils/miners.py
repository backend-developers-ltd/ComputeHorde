import logging

import turbobt

from compute_horde_validator.validator.models import Miner as MinerModel

logger = logging.getLogger(__name__)


def sync_miners_from_neurons(block_number: int, neurons: list[turbobt.Neuron]) -> None:
    """
    Upsert Miner records from neuron data.
    """
    hotkeys = [n.hotkey for n in neurons]
    miners_to_create = []
    miners_to_update = []
    existing_miners = {
        miner.hotkey: miner for miner in MinerModel.objects.filter(hotkey__in=hotkeys)
    }

    for neuron in neurons:
        miner = existing_miners.get(neuron.hotkey)
        is_serving = neuron.axon_info and str(neuron.axon_info.ip) != "0.0.0.0"

        if miner is None:
            address = "0.0.0.0"
            port = 0
            ip_version = 4

            if is_serving:
                shield_address = getattr(neuron.axon_info, "shield_address", None)
                address = shield_address if shield_address else str(neuron.axon_info.ip)
                port = neuron.axon_info.port
                ip_version = neuron.axon_info.ip.version

            miners_to_create.append(
                MinerModel(
                    hotkey=neuron.hotkey,
                    coldkey=neuron.coldkey or "",
                    uid=neuron.uid,
                    address=address,
                    port=port,
                    ip_version=ip_version,
                )
            )
        else:
            needs_update = False

            if miner.uid != neuron.uid:
                miner.uid = neuron.uid
                needs_update = True

            if miner.coldkey != neuron.coldkey and neuron.coldkey:
                miner.coldkey = neuron.coldkey
                needs_update = True

            if is_serving:
                shield_address = getattr(neuron.axon_info, "shield_address", None)
                address = shield_address if shield_address else str(neuron.axon_info.ip)
                port = neuron.axon_info.port
                ip_version = neuron.axon_info.ip.version

                if miner.address != address or miner.port != port or miner.ip_version != ip_version:
                    miner.address = address
                    miner.port = port
                    miner.ip_version = ip_version
                    needs_update = True

            if needs_update:
                miners_to_update.append(miner)

    if miners_to_create:
        MinerModel.objects.bulk_create(miners_to_create, ignore_conflicts=True)
        logger.info(f"Created {len(miners_to_create)} new Miner records for block {block_number}")

    if miners_to_update:
        MinerModel.objects.bulk_update(
            miners_to_update, fields=["uid", "coldkey", "address", "port", "ip_version"]
        )
        logger.info(f"Updated {len(miners_to_update)} Miner records for block {block_number}")
