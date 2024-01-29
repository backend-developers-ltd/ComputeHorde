import asyncio
import time
from datetime import timedelta

import bittensor
import torch
from celery.utils.log import get_task_logger
from django.conf import settings
from django.db import transaction
from django.utils.timezone import now

from compute_horde_validator.celery import app
from compute_horde_validator.validator.models import SyntheticJobBatch
from compute_horde_validator.validator.synthetic_jobs.utils import initiate_jobs, execute_jobs

logger = get_task_logger(__name__)

JOB_WINDOW = 60 * 60

SCORING_ALGO_VERSION = 1


@app.task
def run_synthetic_jobs():
    metagraph = bittensor.metagraph(settings.BITTENSOR_NETUID, settings.BITTENSOR_NETWORK)
    my_key = settings.BITTENSOR_WALLET().get_hotkey().ss58_address
    validator_keys = sorted([n.hotkey for n in metagraph.neurons if n.validator_permit])
    my_index = validator_keys.index(my_key)  # this will throw an error if we're not a registered validator
    # and that's fine
    window_per_validator = JOB_WINDOW / (len(validator_keys) + 1)
    my_window_starts_at = window_per_validator * my_index
    logger.info(f'Sleeping for {my_window_starts_at:02f}s because I am {my_index} out of {len(validator_keys)}')
    time.sleep(my_window_starts_at)
    jobs = initiate_jobs(settings.BITTENSOR_NETUID, settings.BITTENSOR_NETWORK)  # metagraph will be refetched and
    # that's fine, after sleeping for e.g. 30 minutes we should refetch the miner list
    if not jobs:
        logger.info('Nothing to do')
        return
    asyncio.run(execute_jobs(jobs))


@app.task
def set_scores():
    subtensor = bittensor.subtensor(network=settings.BITTENSOR_NETWORK)
    metagraph = subtensor.metagraph(netuid=settings.BITTENSOR_NETUID)
    hotkey_to_uid = {n.hotkey: n.uid for n in metagraph.neurons}
    score_per_uid = {}
    with transaction.atomic():
        batches = list(SyntheticJobBatch.objects.select_related('synthetic_jobs').filter(
            scored=False, started_at__gte=now() - timedelta(days=1)))
        if not batches:
            logger.info('No batches - nothing to score')
            return

        for batch in batches:
            for job in batch.synthetic_jobs:
                uid = hotkey_to_uid.get(job.hotkey)
                if not uid:
                    continue
                score_per_uid[uid] = score_per_uid.get(uid, 0) + job.score
        uids = torch.zeros(len(score_per_uid), dtype=torch.long)
        scores = torch.zeros(len(score_per_uid), dtype=torch.float32)

        for ind, uid in enumerate(sorted(list(score_per_uid.keys()))):
            uids[ind] = uid
            scores[ind] = score_per_uid[uid]
        success = subtensor.set_weights(
            netuid=settings.BITTENSOR_NETUID,
            wallet=settings.BITTENSOR_WALLET(),
            uids=uids,
            weights=scores,
            wait_for_inclusion=True,
            wait_for_finalization=True,
            version_key=SCORING_ALGO_VERSION,
        )
        if not success:
            raise RuntimeError('Failed to set weights')
        for batch in batches:
            batch.scored = True
        SyntheticJobBatch.objects.bulk_update(batches, ['scored'])
