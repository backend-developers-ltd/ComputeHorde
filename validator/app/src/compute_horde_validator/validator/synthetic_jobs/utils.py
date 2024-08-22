import logging
import uuid
from functools import lru_cache

import bittensor
import uvloop
from asgiref.sync import async_to_sync
from django.conf import settings

from compute_horde_validator.validator.models import Miner, SystemEvent
from compute_horde_validator.validator.synthetic_jobs.batch_run import execute_synthetic_batch_run

# new synchronized flow waits longer for job responses
SYNTHETIC_JOBS_SOFT_LIMIT = 12 * 60
SYNTHETIC_JOBS_HARD_LIMIT = SYNTHETIC_JOBS_SOFT_LIMIT + 10

logger = logging.getLogger(__name__)


@lru_cache(maxsize=100)
def batch_id_to_uuid(batch_id: int) -> uuid.UUID:
    return uuid.uuid4()


def create_and_run_synthetic_job_batch(netuid, network):
    uvloop.install()

    if settings.DEBUG_MINER_KEY:
        miners: list[Miner] = []
        axons_by_key: dict[str, bittensor.AxonInfo] = {}
        for miner_index in range(settings.DEBUG_MINER_COUNT):
            hotkey = settings.DEBUG_MINER_KEY
            if miner_index > 0:
                # fake hotkey based on miner index if there are more than one miners
                hotkey = f"5u{miner_index:03}u{hotkey[6:]}"
            miner = Miner.objects.get_or_create(hotkey=hotkey)[0]
            miners.append(miner)
            axons_by_key[hotkey] = bittensor.AxonInfo(
                version=4,
                ip=settings.DEBUG_MINER_ADDRESS,
                ip_type=4,
                port=settings.DEBUG_MINER_PORT + miner_index,
                hotkey=hotkey,
                coldkey=hotkey,  # I hope it does not matter
            )
    else:
        try:
            metagraph = bittensor.metagraph(netuid, network=network)
        except Exception as e:
            msg = f"Failed to get metagraph - will not run synthetic jobs: {e}"
            logger.warning(msg)
            SystemEvent.objects.using(settings.DEFAULT_DB_ALIAS).create(
                type=SystemEvent.EventType.MINER_SYNTHETIC_JOB_FAILURE,
                subtype=SystemEvent.EventSubType.SUBTENSOR_CONNECTIVITY_ERROR,
                long_description=msg,
                data={},
            )
            return
        axons_by_key = {n.hotkey: n.axon_info for n in metagraph.neurons}
        miners = get_miners(metagraph)
        miners = [
            miner
            for miner in miners
            if miner.hotkey in axons_by_key and axons_by_key[miner.hotkey].is_serving
        ]

    async_to_sync(execute_synthetic_batch_run)(axons_by_key, miners)


def save_receipt_event(subtype: str, long_description: str, data: dict):
    SystemEvent.objects.using(settings.DEFAULT_DB_ALIAS).create(
        type=SystemEvent.EventType.RECEIPT_FAILURE,
        subtype=subtype,
        long_description=long_description,
        data=data,
    )


def get_miners(metagraph) -> list[Miner]:
    keys = {n.hotkey for n in metagraph.neurons}
    existing = list(Miner.objects.filter(hotkey__in=keys))
    existing_keys = {m.hotkey for m in existing}
    new_miners = Miner.objects.bulk_create(
        [Miner(hotkey=n.hotkey) for n in metagraph.neurons if n.hotkey not in existing_keys]
    )
    data = {"block": metagraph.block.item()}
    if new_miners:
        data["new_miners"] = len(new_miners)
    SystemEvent.objects.using(settings.DEFAULT_DB_ALIAS).create(
        type=SystemEvent.EventType.VALIDATOR_MINERS_REFRESH,
        subtype=SystemEvent.EventSubType.SUCCESS,
        data=data,
    )
    return existing + new_miners
