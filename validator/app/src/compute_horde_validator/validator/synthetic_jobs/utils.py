import logging

import uvloop
from asgiref.sync import async_to_sync
from compute_horde.utils import (
    BAC_VALIDATOR_SS58_ADDRESS,
    MIN_STAKE,
    VALIDATORS_LIMIT,
    ValidatorInfo,
)
from django.conf import settings

from compute_horde_validator.validator.models import MetagraphSnapshot, Miner, SystemEvent
from compute_horde_validator.validator.synthetic_jobs.batch_run import execute_synthetic_batch_run

# new synchronized flow waits longer for job responses
SYNTHETIC_JOBS_SOFT_LIMIT = 20 * 60
SYNTHETIC_JOBS_HARD_LIMIT = SYNTHETIC_JOBS_SOFT_LIMIT + 10

logger = logging.getLogger(__name__)


def create_and_run_synthetic_job_batch(netuid, network, synthetic_jobs_batch_id: int) -> None:
    uvloop.install()

    if settings.DEBUG_MINER_KEY:
        miners: list[Miner] = []
        for miner_index in range(settings.DEBUG_MINER_COUNT):
            hotkey = settings.DEBUG_MINER_KEY
            if miner_index > 0:
                # fake hotkey based on miner index if there are more than one miners
                hotkey = f"5u{miner_index:03}u{hotkey[6:]}"
            miner = Miner.objects.get_or_create(
                hotkey=hotkey,
                address=settings.DEBUG_MINER_ADDRESS,
                ip_version=4,
                port=settings.DEBUG_MINER_PORT + miner_index,
            )[0]
            miners.append(miner)
        active_validators = []
    else:
        try:
            metagraph = MetagraphSnapshot.get_latest()
        except Exception as e:
            msg = f"Failed to get metagraph - will not run synthetic jobs: {e}"
            logger.warning(msg)
            SystemEvent.objects.using(settings.DEFAULT_DB_ALIAS).create(
                type=SystemEvent.EventType.VALIDATOR_SYNTHETIC_JOBS_FAILURE,
                subtype=SystemEvent.EventSubType.SUBTENSOR_CONNECTIVITY_ERROR,
                long_description=msg,
                data={},
            )
            return
        miners_hotkeys = metagraph.serving_hotkeys
        active_validators = get_validator_infos(metagraph)
        miners = list(Miner.objects.filter(hotkey__in=miners_hotkeys).all())

    async_to_sync(execute_synthetic_batch_run)(miners, active_validators, synthetic_jobs_batch_id)


def get_validator_infos(
    metagraph: MetagraphSnapshot,
) -> list[ValidatorInfo]:
    """
    Validators are top 24 neurons in terms of stake, only taking into account those that have at least 1000
    and forcibly including BAC_VALIDATOR_SS58_ADDRESS.
    The result is sorted.
    """
    validators = [
        (uid, hotkey, stake)
        for (uid, hotkey, stake) in zip(metagraph.uids, metagraph.hotkeys, metagraph.stake)
        if stake >= MIN_STAKE
    ]
    top_validators = sorted(
        validators, key=lambda data: (data[1] == BAC_VALIDATOR_SS58_ADDRESS, data[2]), reverse=True
    )[:VALIDATORS_LIMIT]
    return [
        ValidatorInfo(uid=uid, hotkey=hotkey, stake=stake)
        for (uid, hotkey, stake) in top_validators
    ]
