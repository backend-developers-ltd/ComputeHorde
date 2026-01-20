from __future__ import annotations

import logging
from ipaddress import IPv6Address

from compute_horde_validator.celery import app
from compute_horde_validator.validator.models import Miner as MinerModel
from compute_horde_validator.validator.models import SystemEvent
from compute_horde_validator.validator.models.allowance.internal import (
    MinerAddress,
)
from compute_horde_validator.validator.pylon import pylon_client
from pylon_client.v1 import Hotkey

logger = logging.getLogger(__name__)


@app.task
def sync_miners() -> None:
    """
    Sync Miner models.
    TODO: remove this task and make all places in code that rely on it use something more modularized, maybe
    allowance.sthsth or another module. and not models directly.
    """
    neurons_response = pylon_client().identity.get_recent_neurons()
    neurons = neurons_response.neurons

    existing_miners = list(MinerModel.objects.filter(hotkey__in=[n.hotkey for n in neurons.values()]))
    existing_hotkeys = {Hotkey(m.hotkey) for m in existing_miners}
    new_hotkeys = set([n.hotkey for n in neurons.values()]) - existing_hotkeys

    # Create new miners
    if new_hotkeys:
        new_miners = [
            MinerModel(
                hotkey=hotkey,
                uid=neurons[hotkey].uid,
                coldkey=neurons[hotkey].coldkey,
                address=str(neurons[hotkey].axon_info.ip),
                port=neurons[hotkey].axon_info.port,
                ip_version=6 if isinstance(neurons[hotkey].axon_info.ip, IPv6Address) else 4,
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
        new_uid = int(neurons[hotkey].uid)
        new_coldkey = str(neurons[hotkey].coldkey)
        new_address = str(neurons[hotkey].axon_info.ip)
        new_port = int(neurons[hotkey].axon_info.port)
        new_ip_version = 6 if isinstance(neurons[hotkey].axon_info.ip, IPv6Address) else 4

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
            "block": int(neurons_response.block.number),
            "new_neurons": len(new_hotkeys),
            "updated_axon_infos": len(miners_to_update),
        },
    )
