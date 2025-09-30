import logging

import uvloop
from asgiref.sync import async_to_sync
from django.conf import settings

from compute_horde_validator.validator.allowance.default import allowance
from compute_horde_validator.validator.models import Miner, SystemEvent
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
            metagraph = allowance().get_metagraph()
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
        active_validators = allowance().get_validator_infos()
        miners = list(Miner.objects.filter(hotkey__in=miners_hotkeys).all())

    async_to_sync(execute_synthetic_batch_run)(miners, active_validators, synthetic_jobs_batch_id)
