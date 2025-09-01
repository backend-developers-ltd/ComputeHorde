import logging
from decimal import Decimal

import turbobt
from asgiref.sync import async_to_sync
from django.conf import settings
from compute_horde_validator.celery import app
from compute_horde_validator.validator.tasks import bittensor_client

from compute_horde_validator.validator.models import Miner, SystemEvent
from compute_horde_validator.validator.tasks import _get_metagraph_for_sync
from .. import collateral as collateral_low_level


logger = logging.getLogger(__name__)


@app.task
@bittensor_client
def sync_collaterals(bittensor: turbobt.Bittensor) -> None:
    """
    Synchronizes miner evm addresses and collateral amounts.
    """
    try:
        neurons, subnet_state, block = async_to_sync(_get_metagraph_for_sync)(bittensor)
        if not block:
            logger.warning("Could not get current block for collateral sync")
            return

        hotkeys = [neuron.hotkey for neuron in neurons]
    except Exception as e:
        msg = f"Error getting metagraph data for collateral sync: {e}"
        logger.warning(msg)
        SystemEvent.objects.using(settings.DEFAULT_DB_ALIAS).create(
            type=SystemEvent.EventType.COLLATERAL_SYNCING,
            subtype=SystemEvent.EventSubType.FAILURE,
            long_description=msg,
            data={"error": str(e)},
        )
        return

    associations = async_to_sync(collateral_low_level.get_evm_key_associations)(
        subtensor=bittensor.subtensor,
        netuid=settings.BITTENSOR_NETUID,
        block_hash=block.hash,
    )
    miners = Miner.objects.filter(hotkey__in=hotkeys)
    w3 = collateral_low_level.get_web3_connection(network=settings.BITTENSOR_NETWORK)
    contract_address = collateral_low_level.get_collateral_contract_address()

    to_update = []
    for miner in miners:
        if not miner.uid:
            continue

        evm_address = associations.get(miner.uid)
        miner.evm_address = evm_address
        to_update.append(miner)

        if not miner.evm_address:
            continue

        if contract_address:
            try:
                collateral_wei = collateral_low_level.get_miner_collateral(
                    w3, contract_address, miner.evm_address, block.number
                )
                miner.collateral_wei = Decimal(collateral_wei)
            except Exception as e:
                msg = f"Error while fetching miner collateral: {e}"
                logger.warning(msg)
                SystemEvent.objects.using(settings.DEFAULT_DB_ALIAS).create(
                    type=SystemEvent.EventType.COLLATERAL_SYNCING,
                    subtype=SystemEvent.EventSubType.GETTING_MINER_COLLATERAL_FAILED,
                    long_description=msg,
                    data={
                        "block": block.number,
                        "miner_hotkey": miner.hotkey,
                        "evm_address": evm_address,
                    },
                )

    Miner.objects.bulk_update(to_update, fields=["evm_address", "collateral_wei"])


