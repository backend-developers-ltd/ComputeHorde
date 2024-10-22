import contextlib
import json
import random
import time
import traceback
import uuid
from datetime import timedelta
from functools import cached_property
from math import ceil, floor

import billiard.exceptions
import bittensor
import celery.exceptions
import numpy as np
import requests
from asgiref.sync import async_to_sync
from bittensor.utils.weight_utils import process_weights_for_netuid
from celery import shared_task
from celery.result import allow_join_result
from celery.utils.log import get_task_logger
from compute_horde.dynamic_config import sync_dynamic_config
from compute_horde.receipts.models import JobAcceptedReceipt, JobFinishedReceipt, JobStartedReceipt
from compute_horde.receipts.schemas import (
    JobAcceptedReceiptPayload,
    JobFinishedReceiptPayload,
    JobStartedReceiptPayload,
)
from compute_horde.receipts.transfer import get_miner_receipts
from compute_horde.utils import ValidatorListError, get_validators
from constance import config
from django.conf import settings
from django.db import transaction
from django.utils.timezone import now
from pydantic import JsonValue

from compute_horde_validator.celery import app
from compute_horde_validator.validator.cross_validation.prompt_answering import answer_prompts
from compute_horde_validator.validator.cross_validation.prompt_generation import generate_prompts
from compute_horde_validator.validator.locks import Locked, LockType, get_advisory_lock
from compute_horde_validator.validator.metagraph_client import get_miner_axon_info
from compute_horde_validator.validator.models import (
    Cycle,
    OrganicJob,
    Prompt,
    PromptSample,
    PromptSeries,
    SolveWorkload,
    SyntheticJobBatch,
    SystemEvent,
    Weights,
)
from compute_horde_validator.validator.organic_jobs.miner_client import MinerClient
from compute_horde_validator.validator.organic_jobs.miner_driver import execute_organic_job
from compute_horde_validator.validator.s3 import (
    download_prompts_from_s3_url,
    generate_upload_url,
    get_public_url,
    upload_prompts_to_s3_url,
)
from compute_horde_validator.validator.synthetic_jobs.batch_run import (
    SYNTHETIC_JOBS_HARD_LIMIT,
    SYNTHETIC_JOBS_SOFT_LIMIT,
)
from compute_horde_validator.validator.synthetic_jobs.utils import (
    create_and_run_synthetic_job_batch,
)

from .models import AdminJobRequest
from .scoring import score_batches

logger = get_task_logger(__name__)

JOB_WINDOW = 2 * 60 * 60
MAX_SEED = (1 << 32) - 1

SCORING_ALGO_VERSION = 2

WEIGHT_SETTING_TTL = 60
WEIGHT_SETTING_HARD_TTL = 65
WEIGHT_SETTING_ATTEMPTS = 100
WEIGHT_SETTING_FAILURE_BACKOFF = 5


class WeightsRevealError(Exception):
    pass


class ScheduleError(Exception):
    pass


def when_to_run(subtensor_: bittensor.subtensor, current_cycle) -> int:
    """
    Select block when to run validation for a given validator.
    Validators needs to run their jobs temporarily separated from others.
    The order of validators within a cycle is random, seeded by a block
    preceding the cycle, therefore all validators should arrive at the same order.
    """

    try:
        validators = get_validators(
            netuid=settings.BITTENSOR_NETUID,
            network=settings.BITTENSOR_NETWORK,
            block=current_cycle.start,
        )
    except ValidatorListError as ex:
        raise ScheduleError() from ex

    ordered_hotkeys = [vali.hotkey for vali in validators]
    this_hotkey = get_keypair().ss58_address
    if this_hotkey not in ordered_hotkeys:
        raise ScheduleError(
            "This validator is not in a list of validators -> not scheduling synthetic jobs run"
        )

    try:
        seed = subtensor_.get_block_hash(
            current_cycle.start - config.DYNAMIC_BLOCK_FINALIZATION_NUMBER
        )
    except Exception as ex:
        raise ScheduleError("Could not get seed hash") from ex

    random.Random(seed).shuffle(ordered_hotkeys)
    index_ = ordered_hotkeys.index(this_hotkey)
    block = calculate_job_start_block(
        cycle=current_cycle,
        offset=settings.SYNTHETIC_JOBS_RUN_OFFSET,
        total=len(validators),
        index_=ordered_hotkeys.index(this_hotkey),
    )

    SystemEvent.objects.create(
        type=SystemEvent.EventType.VALIDATOR_SYNTHETIC_JOB_SCHEDULED,
        subtype=SystemEvent.EventSubType.SUCCESS,
        data={"seed": seed, "index": index_, "block": block},
    )
    return block


def calculate_job_start_block(cycle: range, total: int, index_: int, offset: int = 0) -> int:
    """
    |______________________________________________________|__________
    ^-cycle.start                                          ^-cycle.stop
    _____________|_____________|_____________|_____________|
                 ^-0           ^-1           ^-2
    |____________|_____________|
        offset       blocks
                    b/w runs
    """
    blocks_between_runs = (cycle.stop - cycle.start - offset) / total
    return cycle.start + offset + floor(blocks_between_runs * index_)


def get_epoch_containing_block(block: int, netuid: int, tempo: int = 360) -> range:
    """
    Reimplementing the logic from subtensor's Rust function:
        pub fn blocks_until_next_epoch(netuid: u16, tempo: u16, block_number: u64) -> u64
    See https://github.com/opentensor/subtensor.

    See also: https://github.com/opentensor/bittensor/pull/2168/commits/9e8745447394669c03d9445373920f251630b6b8

    If given block happens to be an end of an epoch, the resulting epoch will end with it. The beginning of an epoch
    is the first block when values like "dividends" are different (before an epoch they are constant for a full
    tempo).
    """
    assert tempo > 0

    interval = tempo + 1
    last_epoch = block - 1 - (block + netuid + 1) % interval
    next_tempo_block_start = last_epoch + interval
    return range(last_epoch, next_tempo_block_start)


def get_cycle_containing_block(block: int, netuid: int, tempo: int = 360) -> range:
    """
    A cycle contains two epochs, starts on an even one. A cycle is the basic unit of passage of time in compute horde,
    and validators testing miners are synchronised to cycles.
    """
    very_first_epoch = get_epoch_containing_block(0, netuid, tempo=tempo)
    epoch_containing_block = get_epoch_containing_block(block, netuid, tempo=tempo)

    if ((epoch_containing_block.start - very_first_epoch.start) / (tempo + 1)) % 2:
        # that's the second epoch in this cycle
        first_epoch = range(
            epoch_containing_block.start - (tempo + 1), epoch_containing_block.stop - (tempo + 1)
        )
        second_epoch = epoch_containing_block
    else:
        first_epoch = epoch_containing_block
        second_epoch = range(
            epoch_containing_block.start + (tempo + 1), epoch_containing_block.stop + (tempo + 1)
        )

    return range(first_epoch.start, second_epoch.stop)


class CommitRevealInterval:
    """
    Commit-reveal interval for a given block.

    722                                    1444                                   2166
    |______________________________________|______________________________________|
    ^-1                                    ^-2                                    ^-3

    ^                     ^                ^                                      ^
    interval start        actual commit    interval end / reveal starts           ends

    Subtensor uses the actual subnet hyperparam to determine the interval:
    https://github.com/opentensor/subtensor/blob/af585b9b8a17d27508431257052da502055477b7/pallets/subtensor/src/subnets/weights.rs#L482

    Each interval is divided into reveal and commit windows based on dynamic parameters:

    722                                                                          1443
    |______________________________________|_______________________________________|
    ^                                      ^                              ^
    |-----------------------------|--------|------------------------------|-------_|
    |       REVEAL WINDOW         | BUFFER |        COMMIT WINDOW         | BUFFER |
    |                                      |
    |            COMMIT OFFSET             |

    """

    def __init__(
        self,
        current_block: int,
        *,
        length: int | None = None,
        commit_start_offset: int | None = None,
        commit_end_buffer: int | None = None,
        reveal_end_buffer: int | None = None,
    ):
        self.current_block = current_block
        self.length = length or config.DYNAMIC_COMMIT_REVEAL_WEIGHTS_INTERVAL
        self.commit_start_offset = (
            commit_start_offset or config.DYNAMIC_COMMIT_REVEAL_COMMIT_START_OFFSET
        )
        self.commit_end_buffer = commit_end_buffer or config.DYNAMIC_COMMIT_REVEAL_COMMIT_END_BUFFER
        self.reveal_end_buffer = reveal_end_buffer or config.DYNAMIC_COMMIT_REVEAL_REVEAL_END_BUFFER

    @cached_property
    def start(self):
        """
        https://github.com/opentensor/subtensor/blob/af585b9b8a17d27508431257052da502055477b7/pallets/subtensor/src/subnets/weights.rs#L488
        """
        return self.current_block - self.current_block % self.length

    @cached_property
    def stop(self):
        return self.start + self.length

    @property
    def reveal_start(self):
        return self.start

    @cached_property
    def reveal_stop(self):
        return self.start + self.commit_start_offset - self.reveal_end_buffer

    @property
    def reveal_window(self):
        return range(self.reveal_start, self.reveal_stop)

    @cached_property
    def commit_start(self):
        return self.start + self.commit_start_offset

    @cached_property
    def commit_stop(self):
        return self.stop - self.commit_end_buffer

    @property
    def commit_window(self):
        return range(self.commit_start, self.commit_stop)


@app.task
def schedule_synthetic_jobs() -> None:
    """
    For current cycle, decide when miners' validation should happen.
    Result is a SyntheticJobBatch object in the database.
    """
    with save_event_on_error(SystemEvent.EventSubType.GENERIC_ERROR), transaction.atomic():
        try:
            get_advisory_lock(LockType.VALIDATION_SCHEDULING)
        except Locked:
            logger.debug("Another thread already scheduling validation")
            return

        bittensor.turn_console_off()
        subtensor_ = get_subtensor(network=settings.BITTENSOR_NETWORK)
        current_block = subtensor_.get_current_block()
        current_cycle = get_cycle_containing_block(
            block=current_block, netuid=settings.BITTENSOR_NETUID
        )

        batch_in_current_cycle = (
            SyntheticJobBatch.objects.filter(
                block__gte=current_cycle.start, block__lt=current_cycle.stop
            )
            .order_by("block")
            .last()
        )
        if batch_in_current_cycle:
            logger.debug(
                "Synthetic jobs are already scheduled at block %s", batch_in_current_cycle.block
            )
            return

        next_run_block = when_to_run(subtensor_, current_cycle)

        cycle, _ = Cycle.objects.get_or_create(start=current_cycle.start, stop=current_cycle.stop)
        batch = SyntheticJobBatch.objects.create(
            block=next_run_block,
            cycle=cycle,
        )
        logger.debug("Scheduled synthetic jobs run %s", batch)


@app.task(
    soft_time_limit=SYNTHETIC_JOBS_SOFT_LIMIT,
    time_limit=SYNTHETIC_JOBS_HARD_LIMIT,
)
def _run_synthetic_jobs(synthetic_jobs_batch_id: int) -> None:
    try:
        # metagraph will be refetched and that's fine, after sleeping
        # for e.g. 30 minutes we should refetch the miner list
        create_and_run_synthetic_job_batch(
            settings.BITTENSOR_NETUID,
            settings.BITTENSOR_NETWORK,
            synthetic_jobs_batch_id=synthetic_jobs_batch_id,
        )
    except billiard.exceptions.SoftTimeLimitExceeded:
        logger.info("Running synthetic jobs timed out")


@app.task()
def run_synthetic_jobs(
    wait_in_advance_blocks: int | None = None,
    poll_interval: timedelta | None = None,
) -> None:
    """
    Run synthetic jobs as scheduled by SyntheticJobBatch.
    If there is a job scheduled in near `wait_in_advance_blocks` blocks,
    wait till the block is reached, and run the jobs.

    If `settings.DEBUG_DONT_STAGGER_VALIDATORS` is set, we will run
    synthetic jobs immediately.
    """

    if not config.SERVING:
        logger.warning("Not running synthetic jobs, SERVING is disabled in constance config")
        return

    if settings.DEBUG_DONT_STAGGER_VALIDATORS:
        batch = SyntheticJobBatch.objects.create()
        _run_synthetic_jobs.apply_async(kwargs={"synthetic_jobs_batch_id": batch.id})
        return

    wait_in_advance_blocks = (
        wait_in_advance_blocks or config.DYNAMIC_SYNTHETIC_JOBS_PLANNER_WAIT_IN_ADVANCE_BLOCKS
    )
    poll_interval = poll_interval or timedelta(
        seconds=config.DYNAMIC_SYNTHETIC_JOBS_PLANNER_POLL_INTERVAL
    )

    subtensor_ = get_subtensor(network=settings.BITTENSOR_NETWORK)
    current_block = subtensor_.get_current_block()

    with transaction.atomic():
        ongoing_synthetic_job_batches = list(
            SyntheticJobBatch.objects.select_for_update(skip_locked=True)
            .filter(
                block__gte=current_block
                - config.DYNAMIC_SYNTHETIC_JOBS_PLANNER_MAX_OVERSLEEP_BLOCKS,
                block__lte=current_block + wait_in_advance_blocks,
                started_at__isnull=True,
            )
            .order_by("block")
        )
        if not ongoing_synthetic_job_batches:
            logger.debug("No ongoing scheduled synthetic jobs, current block is %s", current_block)
            return

        if len(ongoing_synthetic_job_batches) > 1:
            logger.warning(
                "More than one scheduled synthetic jobs found (%s)",
                ongoing_synthetic_job_batches,
            )

        batch = ongoing_synthetic_job_batches[0]
        target_block = batch.block
        blocks_to_wait = target_block - current_block
        if blocks_to_wait < 0:
            logger.info(
                "Overslept a batch run, but still within acceptable margin, batch_id: %s, should_run_at_block: %s, current_block: %s",
                batch.id,
                batch.block,
                current_block,
            )
            SystemEvent.objects.using(settings.DEFAULT_DB_ALIAS).create(
                type=SystemEvent.EventType.VALIDATOR_OVERSLEPT_SCHEDULED_JOB_WARNING,
                subtype=SystemEvent.EventSubType.WARNING,
                long_description="Overslept a batch run, but still within acceptable margin",
                data={
                    "batch_id": batch.id,
                    "batch_created_at": str(batch.created_at),
                    "should_run_at_block": batch.block,
                    "current_block": current_block,
                },
            )
        elif blocks_to_wait == 0:
            logger.info(
                "Woke up just in time to run batch, batch_id: %s, should_run_at_block: %s",
                batch.id,
                batch.block,
            )
        else:
            for _ in range(
                ceil(
                    blocks_to_wait
                    * settings.BITTENSOR_APPROXIMATE_BLOCK_DURATION
                    * 2
                    / poll_interval
                )
            ):
                current_block = subtensor_.get_current_block()
                if current_block >= target_block:
                    break
                logger.debug(
                    "Waiting for block %s, current block is %s, sleeping for %s",
                    target_block,
                    current_block,
                    poll_interval,
                )
                time.sleep(poll_interval.total_seconds())
            else:
                logger.error(
                    "Failed to wait for target block %s, current block is %s",
                    target_block,
                    current_block,
                )
                SystemEvent.objects.using(settings.DEFAULT_DB_ALIAS).create(
                    type=SystemEvent.EventType.VALIDATOR_SYNTHETIC_JOBS_FAILURE,
                    subtype=SystemEvent.EventSubType.FAILED_TO_WAIT,
                    long_description="Failed to await the right block to run at",
                    data={
                        "batch_id": batch.id,
                        "batch_created_at": str(batch.created_at),
                        "should_run_at_block": batch.block,
                        "current_block": current_block,
                    },
                )
                return

        batch.started_at = now()
        batch.save()

    _run_synthetic_jobs.apply_async(kwargs={"synthetic_jobs_batch_id": batch.id})


@app.task()
def check_missed_synthetic_jobs() -> None:
    """
    Check if there are any synthetic jobs that were scheduled to run, but didn't.
    """
    subtensor_ = get_subtensor(network=settings.BITTENSOR_NETWORK)
    current_block = subtensor_.get_current_block()

    with transaction.atomic():
        past_job_batches = SyntheticJobBatch.objects.select_for_update(skip_locked=True).filter(
            block__lt=current_block - config.DYNAMIC_SYNTHETIC_JOBS_PLANNER_MAX_OVERSLEEP_BLOCKS,
            started_at__isnull=True,
            is_missed=False,
        )
        for batch in past_job_batches:
            SystemEvent.objects.using(settings.DEFAULT_DB_ALIAS).create(
                type=SystemEvent.EventType.VALIDATOR_SYNTHETIC_JOBS_FAILURE,
                subtype=SystemEvent.EventSubType.OVERSLEPT,
                long_description="Failed to run synthetic jobs in time",
                data={
                    "batch_id": batch.id,
                    "created_at": str(batch.created_at),
                    "block": batch.block,
                    "current_block": current_block,
                    "current_time": str(now()),
                },
            )
        past_job_batches.update(is_missed=True)


def _normalize_weights_for_committing(weights: list[float], max_: int):
    factor = max_ / max(weights)
    return [round(w * factor) for w in weights]


@app.task()
def do_set_weights(
    netuid: int,
    uids: list[int],
    weights: list[float],
    wait_for_inclusion: bool,
    wait_for_finalization: bool,
    version_key: int,
) -> tuple[bool, str]:
    """
    Set weights. To be used in other celery tasks in order to facilitate a timeout,
     since the multiprocessing version of this doesn't work in celery.
    """
    bittensor.turn_console_off()
    subtensor_ = get_subtensor(network=settings.BITTENSOR_NETWORK)
    current_block = subtensor_.get_current_block()

    commit_reveal_weights_enabled = config.DYNAMIC_COMMIT_REVEAL_WEIGHTS_ENABLED
    max_weight = config.DYNAMIC_MAX_WEIGHT

    def _commit_weights() -> tuple[bool, str]:
        normalized_weights = _normalize_weights_for_committing(weights, max_weight)
        weights_in_db = Weights(
            uids=uids,
            weights=normalized_weights,
            block=current_block,
            version_key=version_key,
        )
        try:
            is_success, message = subtensor_.commit_weights(
                wallet=settings.BITTENSOR_WALLET(),
                netuid=netuid,
                uids=uids,
                weights=normalized_weights,
                salt=weights_in_db.salt,
                version_key=version_key,
                wait_for_inclusion=wait_for_inclusion,
                wait_for_finalization=wait_for_finalization,
                max_retries=2,
            )
        except Exception:
            is_success = False
            message = traceback.format_exc()

        if is_success:
            logger.info("Successfully committed weights!!!")
            weights_in_db.save()
            save_weight_setting_event(
                type_=SystemEvent.EventType.WEIGHT_SETTING_SUCCESS,
                subtype=SystemEvent.EventSubType.COMMIT_WEIGHTS_SUCCESS,
                long_description=f"message from chain: {message}",
                data={"weights_id": weights_in_db.id},
            )
        else:
            logger.info("Failed to commit weights due to: %s", message)
            save_weight_setting_failure(
                subtype=SystemEvent.EventSubType.COMMIT_WEIGHTS_ERROR,
                long_description=f"message from chain: {message}",
                data={
                    "weights_id": weights_in_db.id,
                    "current_block": current_block,
                },
            )
        return is_success, message

    def _set_weights() -> tuple[bool, str]:
        try:
            is_success, message = subtensor_.set_weights(
                wallet=settings.BITTENSOR_WALLET(),
                netuid=netuid,
                uids=uids,
                weights=weights,
                version_key=version_key,
                wait_for_inclusion=wait_for_inclusion,
                wait_for_finalization=wait_for_finalization,
                max_retries=2,
            )
        except Exception:
            is_success = False
            message = traceback.format_exc()
        if is_success:
            logger.info("Successfully set weights!!!")
            save_weight_setting_event(
                type_=SystemEvent.EventType.WEIGHT_SETTING_SUCCESS,
                subtype=SystemEvent.EventSubType.SET_WEIGHTS_SUCCESS,
                long_description=message,
                data={},
            )
        else:
            logger.info(f"Failed to set weights due to {message=}")
            save_weight_setting_failure(
                subtype=SystemEvent.EventSubType.SET_WEIGHTS_ERROR,
                long_description=message,
                data={},
            )
        return is_success, message

    if commit_reveal_weights_enabled:
        return _commit_weights()
    else:
        return _set_weights()


@shared_task
def trigger_run_admin_job_request(job_request_id: int):
    async_to_sync(run_admin_job_request)(job_request_id)


def get_keypair():
    return settings.BITTENSOR_WALLET().get_hotkey()


async def run_admin_job_request(job_request_id: int, callback=None):
    job_request: AdminJobRequest = await AdminJobRequest.objects.prefetch_related("miner").aget(
        id=job_request_id
    )
    try:
        miner = job_request.miner
        miner_axon_info = await get_miner_axon_info(miner.hotkey)
        job = await OrganicJob.objects.acreate(
            job_uuid=str(job_request.uuid),
            miner=miner,
            miner_address=miner_axon_info.ip,
            miner_address_ip_version=miner_axon_info.ip_type,
            miner_port=miner_axon_info.port,
            executor_class=job_request.executor_class,
            job_description="Validator Job from Admin Panel",
        )

        my_keypair = get_keypair()
        miner_client = MinerClient(
            miner_hotkey=miner.hotkey,
            miner_address=miner_axon_info.ip,
            miner_port=miner_axon_info.port,
            job_uuid=str(job.job_uuid),
            my_keypair=my_keypair,
        )

        job_request.status_message = "Job successfully triggered"
        print(job_request.status_message)
        await job_request.asave()
    except Exception as e:
        job_request.status_message = f"Job failed to trigger due to: {e}"
        print(job_request.status_message)
        await job_request.asave()
        return

    print(f"\nProcessing job request: {job_request}")
    await execute_organic_job(
        miner_client,
        job,
        job_request,
        total_job_timeout=job_request.timeout,
        notify_callback=callback,
    )


def save_receipt_event(subtype: str, long_description: str, data: JsonValue):
    SystemEvent.objects.using(settings.DEFAULT_DB_ALIAS).create(
        type=SystemEvent.EventType.RECEIPT_FAILURE,
        subtype=subtype,
        long_description=long_description,
        data=data,
    )


def save_weight_setting_event(type_: str, subtype: str, long_description: str, data: JsonValue):
    SystemEvent.objects.using(settings.DEFAULT_DB_ALIAS).create(
        type=type_,
        subtype=subtype,
        long_description=long_description,
        data=data,
    )


def save_weight_setting_failure(subtype: str, long_description: str, data: JsonValue):
    save_weight_setting_event(
        type_=SystemEvent.EventType.WEIGHT_SETTING_FAILURE,
        subtype=subtype,
        long_description=long_description,
        data=data,
    )


def save_weight_revealing_failure(subtype: str, long_description: str, data: JsonValue):
    save_weight_setting_event(
        type_=SystemEvent.EventType.WEIGHT_SETTING_FAILURE,
        subtype=subtype,
        long_description=long_description,
        data=data,
    )


@contextlib.contextmanager
def save_event_on_error(subtype):
    try:
        yield
    except Exception:
        save_weight_setting_failure(subtype, traceback.format_exc(), {})
        raise


def get_subtensor(network):
    with save_event_on_error(SystemEvent.EventSubType.SUBTENSOR_CONNECTIVITY_ERROR):
        return bittensor.subtensor(network=network)


def get_metagraph(subtensor, netuid):
    with save_event_on_error(SystemEvent.EventSubType.SUBTENSOR_CONNECTIVITY_ERROR):
        return subtensor.metagraph(netuid=netuid)


@app.task
def set_scores():
    if not config.SERVING:
        logger.warning("Not setting scores, SERVING is disabled in constance config")
        return

    commit_reveal_weights_enabled = config.DYNAMIC_COMMIT_REVEAL_WEIGHTS_ENABLED

    subtensor = get_subtensor(network=settings.BITTENSOR_NETWORK)
    current_block = subtensor.get_current_block()

    if commit_reveal_weights_enabled:
        interval = CommitRevealInterval(current_block)

        if current_block not in interval.commit_window:
            logger.debug(
                "Outside of commit window, skipping, current block: %s, window: %s",
                current_block,
                interval.commit_window,
            )
            return

    with save_event_on_error(SystemEvent.EventSubType.GENERIC_ERROR):
        with transaction.atomic():
            try:
                get_advisory_lock(LockType.WEIGHT_SETTING)
            except Locked:
                logger.debug("Another thread already setting weights")
                return

            metagraph = get_metagraph(subtensor, netuid=settings.BITTENSOR_NETUID)
            neurons = metagraph.neurons
            hotkey_to_uid = {n.hotkey: n.uid for n in neurons}
            score_per_uid = {}
            batches = list(
                SyntheticJobBatch.objects.select_related("cycle")
                .filter(
                    scored=False,
                    started_at__gte=now() - timedelta(days=1),
                    cycle__stop__lt=current_block,
                )
                .order_by("-started_at")
            )
            if not batches:
                logger.info("No batches - nothing to score")
                return
            if len(batches) > 1:
                logger.error("Unexpected number batches eligible for scoring: %s", len(batches))
                for batch in batches[:-1]:
                    batch.scored = True
                    batch.save()
                batches = [batches[-1]]

            hotkey_scores = score_batches(batches)
            for hotkey, score in hotkey_scores.items():
                uid = hotkey_to_uid.get(hotkey)
                if uid is None:
                    continue
                score_per_uid[uid] = score

            if not score_per_uid:
                logger.info("No miners on the subnet to score")
                return
            uids = np.zeros(len(neurons), dtype=np.int64)
            weights = np.zeros(len(neurons), dtype=np.float32)
            for ind, n in enumerate(neurons):
                uids[ind] = n.uid
                weights[ind] = score_per_uid.get(n.uid, 0)

            uids, weights = process_weights_for_netuid(
                uids,
                weights,
                settings.BITTENSOR_NETUID,
                subtensor,
                metagraph,
            )

            for batch in batches:
                batch.scored = True
                batch.save()

            for try_number in range(WEIGHT_SETTING_ATTEMPTS):
                logger.debug(
                    f"Setting weights (attempt #{try_number}):\nuids={uids}\nscores={weights}"
                )
                success = False

                try:
                    result = do_set_weights.apply_async(
                        kwargs=dict(
                            netuid=settings.BITTENSOR_NETUID,
                            uids=uids.tolist(),
                            weights=weights.tolist(),
                            wait_for_inclusion=True,
                            wait_for_finalization=False,
                            version_key=SCORING_ALGO_VERSION,
                        ),
                        soft_time_limit=WEIGHT_SETTING_TTL,
                        time_limit=WEIGHT_SETTING_HARD_TTL,
                    )
                    logger.info(f"Setting weights task id: {result.id}")
                    try:
                        with allow_join_result():
                            success, msg = result.get(timeout=WEIGHT_SETTING_TTL)
                    except (celery.exceptions.TimeoutError, billiard.exceptions.TimeLimitExceeded):
                        result.revoke(terminate=True)
                        logger.info(f"Setting weights timed out (attempt #{try_number})")
                        save_weight_setting_failure(
                            subtype=SystemEvent.EventSubType.WRITING_TO_CHAIN_TIMEOUT,
                            long_description=traceback.format_exc(),
                            data={"try_number": try_number, "operation": "setting/committing"},
                        )
                        continue
                except Exception:
                    logger.warning("Encountered when setting weights: ", exc_info=True)
                    save_weight_setting_failure(
                        subtype=SystemEvent.EventSubType.WRITING_TO_CHAIN_GENERIC_ERROR,
                        long_description=traceback.format_exc(),
                        data={"try_number": try_number, "operation": "setting/committing"},
                    )
                    continue
                if success:
                    break
                time.sleep(WEIGHT_SETTING_FAILURE_BACKOFF)
            else:
                msg = f"Failed to set weights after {WEIGHT_SETTING_ATTEMPTS} attempts"
                logger.warning(msg)
                save_weight_setting_failure(
                    subtype=SystemEvent.EventSubType.GIVING_UP,
                    long_description=msg,
                    data={"try_number": WEIGHT_SETTING_ATTEMPTS, "operation": "setting/committing"},
                )


@app.task()
def reveal_scores() -> None:
    """
    Select latest Weights that are older than `commit_reveal_weights_interval`
    and haven't been revealed yet, and reveal them.
    """
    last_weights = Weights.objects.order_by("-created_at").first()
    if not last_weights or last_weights.revealed_at is not None:
        logger.debug("No weights to reveal")
        return

    subtensor_ = get_subtensor(network=settings.BITTENSOR_NETWORK)
    current_block = subtensor_.get_current_block()
    interval = CommitRevealInterval(current_block)
    if current_block not in interval.reveal_window:
        logger.debug(
            "Outside of reveal window, skipping, current block: %s, window: %s",
            current_block,
            interval.reveal_window,
        )
        return

    # find the interval in which the commit occurred
    block_interval = CommitRevealInterval(last_weights.block)
    # revealing starts in the next interval
    reveal_start = block_interval.stop

    if current_block < reveal_start:
        logger.warning(
            "Too early to reveal weights weights_id: %s, reveal starts: %s, current block: %s",
            last_weights.pk,
            reveal_start,
            current_block,
        )
        return

    reveal_end = reveal_start + block_interval.length
    if current_block > reveal_end:
        logger.error(
            "Weights are too old to be revealed weights_id: %s, reveal_ended: %s, current block: %s",
            last_weights.pk,
            reveal_end,
            current_block,
        )
        return

    WEIGHT_REVEALING_TTL = config.DYNAMIC_WEIGHT_REVEALING_TTL
    WEIGHT_REVEALING_HARD_TTL = config.DYNAMIC_WEIGHT_REVEALING_HARD_TTL
    WEIGHT_REVEALING_ATTEMPTS = config.DYNAMIC_WEIGHT_REVEALING_ATTEMPTS
    WEIGHT_REVEALING_FAILURE_BACKOFF = config.DYNAMIC_WEIGHT_REVEALING_FAILURE_BACKOFF

    weights_id = last_weights.id
    with transaction.atomic():
        last_weights = (
            Weights.objects.filter(id=weights_id, revealed_at=None)
            .select_for_update(skip_locked=True)
            .first()
        )
        if not last_weights:
            logger.debug(
                "Weights have already been revealed or are being revealed at this moment: %s",
                weights_id,
            )
            return

        for try_number in range(WEIGHT_REVEALING_ATTEMPTS):
            logger.debug(f"Revealing weights (attempt #{try_number}): weights_id={weights_id}")
            success = False

            try:
                result = do_reveal_weights.apply_async(
                    kwargs=dict(
                        weights_id=last_weights.id,
                    ),
                    soft_time_limit=WEIGHT_REVEALING_TTL,
                    time_limit=WEIGHT_REVEALING_HARD_TTL,
                )
                logger.info(f"Revealing weights task id: {result.id}")
                try:
                    with allow_join_result():
                        success, msg = result.get(timeout=WEIGHT_REVEALING_TTL)
                except (celery.exceptions.TimeoutError, billiard.exceptions.TimeLimitExceeded):
                    result.revoke(terminate=True)
                    logger.info(f"Revealing weights timed out (attempt #{try_number})")
                    save_weight_setting_failure(
                        subtype=SystemEvent.EventSubType.WRITING_TO_CHAIN_TIMEOUT,
                        long_description=traceback.format_exc(),
                        data={"try_number": try_number, "operation": "revealing"},
                    )
                    continue
            except Exception:
                logger.warning("Encountered when revealing weights: ")
                save_weight_setting_failure(
                    subtype=SystemEvent.EventSubType.WRITING_TO_CHAIN_GENERIC_ERROR,
                    long_description=traceback.format_exc(),
                    data={"try_number": try_number, "operation": "revealing"},
                )
                continue
            if success:
                last_weights.revealed_at = now()
                last_weights.save()
                break
            time.sleep(WEIGHT_REVEALING_FAILURE_BACKOFF)
        else:
            msg = f"Failed to set weights after {WEIGHT_REVEALING_ATTEMPTS} attempts"
            logger.warning(msg)
            save_weight_setting_failure(
                subtype=SystemEvent.EventSubType.GIVING_UP,
                long_description=msg,
                data={"try_number": WEIGHT_REVEALING_ATTEMPTS, "operation": "revealing"},
            )


@app.task()
def do_reveal_weights(weights_id: int) -> tuple[bool, str]:
    weights = Weights.objects.filter(id=weights_id, revealed_at=None).first()
    if not weights:
        logger.debug(
            "Weights have already been revealed or are being revealed at this moment: %s",
            weights_id,
        )
        return True, "nothing_to_do"

    wallet = settings.BITTENSOR_WALLET()
    subtensor_ = get_subtensor(network=settings.BITTENSOR_NETWORK)
    try:
        is_success, message = subtensor_.reveal_weights(
            wallet=wallet,
            netuid=settings.BITTENSOR_NETUID,
            uids=weights.uids,
            weights=weights.weights,
            salt=weights.salt,
            version_key=weights.version_key,
            wait_for_inclusion=True,
            wait_for_finalization=True,
            max_retries=2,
        )
    except Exception:
        logger.warning("Encountered when setting weights: ", exc_info=True)
        is_success = False
        message = traceback.format_exc()
    if is_success:
        save_weight_setting_event(
            type_=SystemEvent.EventType.WEIGHT_SETTING_SUCCESS,
            subtype=SystemEvent.EventSubType.REVEAL_WEIGHTS_SUCCESS,
            long_description=message,
            data={"weights_id": weights.id},
        )
    else:
        try:
            current_block = subtensor_.get_current_block()
        except Exception as e:
            logger.warning("Failed to get current block: %s", e)
            current_block = "unknown"
        finally:
            save_weight_setting_failure(
                subtype=SystemEvent.EventSubType.REVEAL_WEIGHTS_ERROR,
                long_description=message,
                data={
                    "weights_id": weights.id,
                    "current_block": current_block,
                },
            )
    return is_success, message


@app.task
def fetch_receipts_from_miner(hotkey: str, ip: str, port: int):
    logger.debug(f"Fetching receipts from miner. {hotkey=} {ip} {port=}")
    try:
        receipts = get_miner_receipts(hotkey, ip, port)
    except Exception as e:
        comment = f"Failed to fetch receipts from miner {hotkey} {ip}:{port}: {e!r}"
        logger.warning(comment)
        save_receipt_event(
            subtype=SystemEvent.EventSubType.RECEIPT_FETCH_ERROR,
            long_description=comment,
            data={"miner_hotkey": hotkey, "miner_ip": ip, "miner_port": port},
        )
        return
    logger.debug(f"Miner returned {len(receipts)} receipts. {hotkey=}")

    tolerance = timedelta(hours=1)

    latest_job_started_receipt = (
        JobStartedReceipt.objects.filter(miner_hotkey=hotkey).order_by("-timestamp").first()
    )
    job_started_receipt_cutoff_time = (
        latest_job_started_receipt.timestamp - tolerance if latest_job_started_receipt else None
    )
    job_started_receipt_to_create = [
        JobStartedReceipt(
            job_uuid=receipt.payload.job_uuid,
            miner_hotkey=receipt.payload.miner_hotkey,
            validator_hotkey=receipt.payload.validator_hotkey,
            miner_signature=receipt.miner_signature,
            validator_signature=receipt.validator_signature,
            timestamp=receipt.payload.timestamp,
            executor_class=receipt.payload.executor_class,
            max_timeout=receipt.payload.max_timeout,
            ttl=receipt.payload.ttl,
        )
        for receipt in receipts
        if isinstance(receipt.payload, JobStartedReceiptPayload)
        and (
            job_started_receipt_cutoff_time is None
            or receipt.payload.timestamp > job_started_receipt_cutoff_time
        )
    ]
    logger.debug(f"Creating {len(job_started_receipt_to_create)} JobStartedReceipt. {hotkey=}")
    JobStartedReceipt.objects.bulk_create(job_started_receipt_to_create, ignore_conflicts=True)

    latest_job_accepted_receipt = (
        JobAcceptedReceipt.objects.filter(miner_hotkey=hotkey).order_by("-timestamp").first()
    )
    job_accepted_receipt_cutoff_time = (
        latest_job_accepted_receipt.timestamp - tolerance if latest_job_accepted_receipt else None
    )
    job_accepted_receipt_to_create = [
        JobAcceptedReceipt(
            job_uuid=receipt.payload.job_uuid,
            miner_hotkey=receipt.payload.miner_hotkey,
            validator_hotkey=receipt.payload.validator_hotkey,
            miner_signature=receipt.miner_signature,
            validator_signature=receipt.validator_signature,
            timestamp=receipt.payload.timestamp,
            ttl=receipt.payload.ttl,
        )
        for receipt in receipts
        if isinstance(receipt.payload, JobAcceptedReceiptPayload)
        and (
            job_accepted_receipt_cutoff_time is None
            or receipt.payload.timestamp > job_accepted_receipt_cutoff_time
        )
    ]
    logger.debug(f"Creating {len(job_accepted_receipt_to_create)} JobAcceptedReceipt. {hotkey=}")
    JobAcceptedReceipt.objects.bulk_create(job_accepted_receipt_to_create, ignore_conflicts=True)

    latest_job_finished_receipt = (
        JobFinishedReceipt.objects.filter(miner_hotkey=hotkey).order_by("-timestamp").first()
    )
    job_finished_receipt_cutoff_time = (
        latest_job_finished_receipt.timestamp - tolerance if latest_job_finished_receipt else None
    )
    job_finished_receipt_to_create = [
        JobFinishedReceipt(
            job_uuid=receipt.payload.job_uuid,
            miner_hotkey=receipt.payload.miner_hotkey,
            validator_hotkey=receipt.payload.validator_hotkey,
            miner_signature=receipt.miner_signature,
            validator_signature=receipt.validator_signature,
            timestamp=receipt.payload.timestamp,
            time_started=receipt.payload.time_started,
            time_took_us=receipt.payload.time_took_us,
            score_str=receipt.payload.score_str,
        )
        for receipt in receipts
        if isinstance(receipt.payload, JobFinishedReceiptPayload)
        and (
            job_finished_receipt_cutoff_time is None
            or receipt.payload.timestamp > job_finished_receipt_cutoff_time
        )
    ]
    logger.debug(f"Creating {len(job_finished_receipt_to_create)} JobFinishedReceipt. {hotkey=}")
    JobFinishedReceipt.objects.bulk_create(job_finished_receipt_to_create, ignore_conflicts=True)


@app.task
def fetch_receipts():
    """Fetch job receipts from the miners."""
    # Delete old receipts before fetching new ones
    JobStartedReceipt.objects.filter(timestamp__lt=now() - timedelta(days=7)).delete()
    JobAcceptedReceipt.objects.filter(timestamp__lt=now() - timedelta(days=7)).delete()
    JobFinishedReceipt.objects.filter(timestamp__lt=now() - timedelta(days=7)).delete()

    metagraph = bittensor.metagraph(
        netuid=settings.BITTENSOR_NETUID, network=settings.BITTENSOR_NETWORK
    )
    miners = [neuron for neuron in metagraph.neurons if neuron.axon_info.is_serving]
    for miner in miners:
        fetch_receipts_from_miner.delay(miner.hotkey, miner.axon_info.ip, miner.axon_info.port)


@shared_task
def send_events_to_facilitator():
    with transaction.atomic(using=settings.DEFAULT_DB_ALIAS):
        events_qs = (
            SystemEvent.objects.using(settings.DEFAULT_DB_ALIAS)
            .filter(sent=False)
            .select_for_update(skip_locked=True)
        )[:10_000]
        if events_qs.count() == 0:
            return

        if settings.STATS_COLLECTOR_URL == "":
            logger.warning("STATS_COLLECTOR_URL is not set, not sending system events")
            return

        keypair = get_keypair()
        hotkey = keypair.ss58_address
        signing_timestamp = int(time.time())
        to_sign = json.dumps(
            {"signing_timestamp": signing_timestamp, "validator_ss58_address": hotkey},
            sort_keys=True,
        )
        signature = f"0x{keypair.sign(to_sign).hex()}"
        events = list(events_qs)
        data = [event.to_dict() for event in events]
        url = settings.STATS_COLLECTOR_URL + f"validator/{hotkey}/system_events"
        response = requests.post(
            url,
            json=data,
            headers={
                "Validator-Signature": signature,
                "Validator-Signing-Timestamp": str(signing_timestamp),
            },
        )

        if response.status_code == 201:
            logger.info(f"Sent {len(data)} system events to facilitator")
            SystemEvent.objects.using(settings.DEFAULT_DB_ALIAS).filter(
                id__in=[event.id for event in events]
            ).update(sent=True)
        else:
            logger.error(f"Failed to send system events to facilitator: {response}")


@app.task
def fetch_dynamic_config() -> None:
    # if same key exists in both places, common config wins
    sync_dynamic_config(
        config_url=f"https://raw.githubusercontent.com/backend-developers-ltd/compute-horde-dynamic-config/master/validator-config-{settings.DYNAMIC_CONFIG_ENV}.json",
        namespace=config,
    )
    sync_dynamic_config(
        config_url=f"https://raw.githubusercontent.com/backend-developers-ltd/compute-horde-dynamic-config/master/common-config-{settings.DYNAMIC_CONFIG_ENV}.json",
        namespace=config,
    )


@app.task(
    soft_time_limit=4 * 60 + 50,
    time_limit=5 * 60,
)
def llm_prompt_generation():
    unprocessed_workloads = SolveWorkload.objects.filter(finished_at__isnull=True).count()
    if unprocessed_workloads > 0:
        # prevent any starvation issues
        logger.info("Unprocessed workloads found - skipping prompt generation")
        return

    num_expected_prompt_series = config.DYNAMIC_MAX_PROMPT_SERIES
    num_prompt_series = PromptSeries.objects.count()

    if num_prompt_series >= num_expected_prompt_series:
        logger.warning(
            "There are %s series in the db - skipping prompt generation",
            num_prompt_series,
        )
        return

    logger.info("There are %s series in the db, generating prompts", num_prompt_series)

    with transaction.atomic():
        try:
            get_advisory_lock(LockType.TRUSTED_MINER_LOCK)
        except Locked:
            logger.debug("Another thread already using the trusted miner")
            return

        async_to_sync(generate_prompts)()


@app.task(
    soft_time_limit=4 * 60 + 50,
    time_limit=5 * 60,
)
def llm_prompt_answering():
    started_at = now()
    unprocessed_workloads = SolveWorkload.objects.filter(finished_at__isnull=True)

    times = []
    success_count = 0
    failure_count = 0
    for workload in unprocessed_workloads:
        start = time.time()
        with transaction.atomic():
            try:
                get_advisory_lock(LockType.TRUSTED_MINER_LOCK)
            except Locked:
                logger.debug("Another thread already using the trusted miner")
                return

            success = async_to_sync(answer_prompts)(workload)
        success_count += success
        failure_count += not success_count
        times.append(time.time() - start)
        total_time = sum(times)
        avg_time = total_time / len(times)
        if total_time + avg_time > 4 * 60 + 20:
            break

    completed_at = now()
    SystemEvent.objects.create(
        type=SystemEvent.EventType.LLM_PROMPT_ANSWERING,
        subtype=SystemEvent.EventSubType.SUCCESS,
        timestamp=now(),
        long_description="",
        data={
            "started_at": started_at.isoformat(),
            "completed_at": completed_at.isoformat(),
            "task_duration": (completed_at - started_at).total_seconds(),
            "times": times,
            "success_count": success_count,
            "failure_count": failure_count,
        },
    )


def init_workload(seed: int) -> tuple[SolveWorkload, str]:
    workload_uuid = uuid.uuid4()
    # generate an s3 url to upload workload prompts to
    s3_upload_url = generate_upload_url(
        key=str(workload_uuid), bucket_name=settings.S3_BUCKET_NAME_ANSWERS
    )
    # generate an s3 url to download workload prompts to be answered
    s3_url = get_public_url(
        key=str(workload_uuid),
        bucket_name=settings.S3_BUCKET_NAME_ANSWERS,
    )
    return SolveWorkload(workload_uuid=workload_uuid, seed=seed, s3_url=s3_url), s3_upload_url


@app.task()
def llm_prompt_sampling():
    # generate new prompt samples if needed

    num_prompt_series = PromptSeries.objects.count()
    required_series_to_start_sampling = min(
        config.DYNAMIC_TARGET_NUMBER_OF_PROMPT_SAMPLES_READY * 2, config.DYNAMIC_MAX_PROMPT_SERIES
    )
    if num_prompt_series < required_series_to_start_sampling:
        logger.warning(
            "There are %s series in the db - expected %s for start sampling - skipping prompt sampling",
            num_prompt_series,
            required_series_to_start_sampling,
        )
        return
    num_unused_prompt_samples = PromptSample.objects.filter(synthetic_job__isnull=True).count()
    num_needed_prompt_samples = (
        config.DYNAMIC_TARGET_NUMBER_OF_PROMPT_SAMPLES_READY - num_unused_prompt_samples
    )

    if num_needed_prompt_samples > 0:
        logger.info(
            "There are %s prompt samples in the db, generating more",
            num_unused_prompt_samples,
        )
        create_sample_workloads(num_needed_prompt_samples)
        return
    else:
        logger.warning(
            "There are %s prompt samples - skipping prompt sampling",
            num_unused_prompt_samples,
        )


def persist_workload(
    workload: SolveWorkload, prompt_samples: list[PromptSample], prompts: list[Prompt]
):
    logger.info(f"Saving workload {workload}")
    # save the sampled prompts as unanswered in the db
    with transaction.atomic():
        workload.save()
        PromptSample.objects.bulk_create(prompt_samples)
        Prompt.objects.bulk_create(prompts)


def create_sample_workloads(num_needed_prompt_samples):
    prompts_per_sample = config.DYNAMIC_NUMBER_OF_PROMPTS_TO_SAMPLE_FROM_SERIES
    prompts_per_workload = config.DYNAMIC_NUMBER_OF_PROMPTS_PER_WORKLOAD

    # set seed for the current synthetic jobs run
    seed = random.randint(0, MAX_SEED)

    # workload we are currently sampling for
    try:
        current_workload, current_upload_url = init_workload(seed)
    except Exception as e:
        logger.error(f"Failed to create new workload: {e} - aborting prompt sampling")
        return

    # how many prompts series we sampled so far
    # for each prompt series there is one prompt sample
    num_prompt_series_sampled = 0

    current_prompt_samples = []
    current_prompts = []

    # assume we have sufficient prompt series in the db to make all the prompt_samples needed
    # take a random order of prompt series to avoid using the same series at each synthetic jobs run
    for prompt_series in PromptSeries.objects.order_by("?").all():
        # get all prompts
        lines = download_prompts_from_s3_url(prompt_series.s3_url)

        # should always have enough prompts
        if len(lines) <= prompts_per_sample:
            logger.error(f"Skipping bucket {prompt_series.s3_url}, not enough prompts")
            continue

        # sample prompts
        sampled_lines = random.sample(lines, prompts_per_sample)

        prompt_sample = PromptSample(series=prompt_series, workload=current_workload)
        current_prompt_samples += [prompt_sample]
        current_prompts += [Prompt(sample=prompt_sample, content=line) for line in sampled_lines]

        if len(current_prompts) >= prompts_per_workload:
            content = "\n".join([p.content for p in current_prompts])
            if upload_prompts_to_s3_url(current_upload_url, content):
                # save the workload in the db
                persist_workload(current_workload, current_prompt_samples, current_prompts)
                num_prompt_series_sampled += len(current_prompt_samples)
            else:
                logger.error(f"Failed to create workload {current_workload} - skipping")

            # finished creating all needed prompt samples so exit after last batch is filled
            if num_prompt_series_sampled >= num_needed_prompt_samples:
                logger.info(f"Created {num_prompt_series_sampled} new prompt samples")
                break

            # reset for next workload
            current_prompt_samples = []
            current_prompts = []
            try:
                current_workload, current_upload_url = init_workload(seed)
            except Exception as e:
                logger.error(f"Failed to create new workload: {e} - aborting prompt sampling")
                continue
