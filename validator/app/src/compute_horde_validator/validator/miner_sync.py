from __future__ import annotations

import logging

from compute_horde_validator.celery import app
from compute_horde_validator.validator.allowance.default import allowance
from compute_horde_validator.validator.models import Miner as MinerModel
from compute_horde_validator.validator.models import SystemEvent
from compute_horde_validator.validator.models.allowance.internal import (
    MinerAddress,
)
from compute_horde_validator.validator.models.allowance.internal import (
    Neuron as NeuronSnapshot,
)

logger = logging.getLogger(__name__)


@app.task
def sync_miners(block_number: int | None = None) -> None:
    """
    Sync Miner models.
    """
    if block_number is None:
        block_number = allowance().get_current_block()

    metagraph = allowance().get_metagraph(block_number)
    uid_map = dict(zip(metagraph.hotkeys, metagraph.uids))
    neuron_map = {
        n.hotkey_ss58address: n for n in NeuronSnapshot.objects.filter(block=block_number)
    }
    address_map = {
        a.hotkey_ss58address: a
        for a in MinerAddress.objects.filter(hotkey_ss58address__in=metagraph.hotkeys)
    }

    existing_miners = list(MinerModel.objects.filter(hotkey__in=metagraph.hotkeys))
    existing_hotkeys = {m.hotkey for m in existing_miners}
    new_hotkeys = set(metagraph.hotkeys) - existing_hotkeys

    # Create new miners
    if new_hotkeys:
        new_miners = [
            MinerModel(
                hotkey=hotkey,
                uid=uid_map[hotkey],
                coldkey=neuron_map[hotkey].coldkey_ss58address if hotkey in neuron_map else None,
                address=str(address_map[hotkey].address) if hotkey in address_map else "0.0.0.0",
                port=int(address_map[hotkey].port) if hotkey in address_map else 0,
                ip_version=int(address_map[hotkey].ip_version) if hotkey in address_map else 4,
            )
            for hotkey in new_hotkeys
        ]
        MinerModel.objects.bulk_create(new_miners)
        logger.info(
            "Created %d new miners from allowance snapshot: %s", len(new_miners), new_hotkeys
        )

    # Update existing miners
    miners_to_update = []
    for miner in existing_miners:
        hotkey = miner.hotkey
        new_uid = uid_map[hotkey]
        new_coldkey = neuron_map[hotkey].coldkey_ss58address if hotkey in neuron_map else None
        new_address = str(address_map[hotkey].address) if hotkey in address_map else "0.0.0.0"
        new_port = int(address_map[hotkey].port) if hotkey in address_map else 0
        new_ip_version = int(address_map[hotkey].ip_version) if hotkey in address_map else 4

        if (
            miner.uid != new_uid
            or miner.coldkey != new_coldkey
            or miner.address != new_address
            or miner.port != new_port
            or miner.ip_version != new_ip_version
        ):
            miner.uid = new_uid
            miner.coldkey = new_coldkey
            miner.address = new_address
            miner.port = new_port
            miner.ip_version = new_ip_version
            miners_to_update.append(miner)

    if miners_to_update:
        MinerModel.objects.bulk_update(
            miners_to_update,
            fields=["uid", "coldkey", "address", "port", "ip_version"],
        )
        logger.info("Updated %d miners from allowance snapshot", len(miners_to_update))

    # Log success event
    SystemEvent.objects.create(
        type=SystemEvent.EventType.VALIDATOR_MINERS_REFRESH,
        subtype=SystemEvent.EventSubType.SUCCESS,
        data={
            "block": block_number,
            "new_neurons": len(new_hotkeys),
            "updated_axon_infos": len(miners_to_update),
        },
    )
