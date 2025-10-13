import asyncio
import json
import random
import time
import uuid
from datetime import timedelta
from math import ceil, floor

import billiard.exceptions
import requests
import tenacity
import turbobt
import turbobt.substrate.exceptions
from asgiref.sync import async_to_sync, sync_to_async
from celery import shared_task
from celery.result import AsyncResult
from celery.utils.log import get_task_logger
from compute_horde.dynamic_config import fetch_dynamic_configs_from_contract, sync_dynamic_config
from compute_horde.fv_protocol.facilitator_requests import OrganicJobRequest
from compute_horde.miner_client.organic import OrganicMinerClient
from compute_horde.smart_contracts.map_contract import get_dynamic_config_types_from_settings
from compute_horde.subtensor import get_cycle_containing_block
from compute_horde.utils import turbobt_get_validators
from compute_horde_core.executor_class import ExecutorClass
from constance import config
from django.conf import settings
from django.db import transaction
from django.utils.timezone import now
from pydantic import JsonValue, TypeAdapter

from compute_horde_validator.celery import app
from compute_horde_validator.validator.collateral.default import collateral
from compute_horde_validator.validator.cross_validation.prompt_answering import answer_prompts
from compute_horde_validator.validator.cross_validation.prompt_generation import generate_prompts
from compute_horde_validator.validator.locks import Locked, LockType, get_advisory_lock
from compute_horde_validator.validator.models import (
    Cycle,
    Miner,
    OrganicJob,
    Prompt,
    PromptSample,
    PromptSeries,
    SolveWorkload,
    SyntheticJobBatch,
    SystemEvent,
)
from compute_horde_validator.validator.organic_jobs.miner_client import MinerClient
from compute_horde_validator.validator.organic_jobs.miner_driver import (
    drive_organic_job,
    execute_organic_job_request,
)
from compute_horde_validator.validator.routing.types import JobRoute
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

from . import eviction
from .allowance import tasks as allowance_tasks  # noqa
from .clean_me_up import _get_metagraph_for_sync, bittensor_client, get_single_manifest
from .collateral import tasks as collateral_tasks  # noqa
from .dynamic_config import aget_config
from .models import AdminJobRequest, MetagraphSnapshot, MinerManifest
from .scoring import tasks as scoring_tasks  # noqa

if False:
    import torch  # noqa

logger = get_task_logger(__name__)

JOB_WINDOW = 2 * 60 * 60
MAX_SEED = (1 << 32) - 1

COMPUTE_TIME_OVERHEAD_SECONDS = 30  # TODO: approximate a realistic value


class ScheduleError(Exception):
    pass


async def when_to_run(
    bittensor: turbobt.Bittensor,
    current_cycle: range,
    block_finalization_number: int,
) -> int:
    """
    Select block when to run validation for a given validator.
    Validators needs to run their jobs temporarily separated from others.
    The order of validators within a cycle is random, seeded by a block
    preceding the cycle, therefore all validators should arrive at the same order.
    """

    try:
        validators = await turbobt_get_validators(
            bittensor,
            netuid=settings.BITTENSOR_NETUID,
            block=current_cycle.start,
        )
    except Exception as ex:
        raise ScheduleError() from ex

    ordered_hotkeys = [vali.hotkey for vali in validators]
    this_hotkey = get_keypair().ss58_address
    if this_hotkey not in ordered_hotkeys:
        raise ScheduleError(
            "This validator is not in a list of validators -> not scheduling synthetic jobs run"
        )

    try:
        block_number = current_cycle.start - block_finalization_number
        block = await bittensor.blocks[block_number].get()
        seed = block.hash
    except Exception as ex:
        raise ScheduleError("Could not get seed hash") from ex

    random.Random(seed).shuffle(ordered_hotkeys)
    index_ = ordered_hotkeys.index(this_hotkey)
    start_block = calculate_job_start_block(
        cycle=current_cycle,
        offset=settings.SYNTHETIC_JOBS_RUN_OFFSET,
        total=len(validators),
        index_=ordered_hotkeys.index(this_hotkey),
    )

    await SystemEvent.objects.acreate(
        type=SystemEvent.EventType.VALIDATOR_SYNTHETIC_JOB_SCHEDULED,
        subtype=SystemEvent.EventSubType.SUCCESS,
        data={"seed": seed, "index": index_, "block": start_block},
    )
    return start_block


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
@bittensor_client
def run_synthetic_jobs(
    bittensor: turbobt.Bittensor,
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

    current_block = async_to_sync(bittensor.blocks.head)()

    if settings.DEBUG_DONT_STAGGER_VALIDATORS:
        batch = SyntheticJobBatch.objects.create(
            block=current_block.number,
            cycle=Cycle.from_block(current_block.number, settings.BITTENSOR_NETUID),
        )
        _run_synthetic_jobs.apply_async(kwargs={"synthetic_jobs_batch_id": batch.id})
        return

    wait_in_advance_blocks = (
        wait_in_advance_blocks or config.DYNAMIC_SYNTHETIC_JOBS_PLANNER_WAIT_IN_ADVANCE_BLOCKS
    )
    poll_interval = poll_interval or timedelta(
        seconds=config.DYNAMIC_SYNTHETIC_JOBS_PLANNER_POLL_INTERVAL
    )

    with transaction.atomic():
        ongoing_synthetic_job_batches = list(
            SyntheticJobBatch.objects.select_for_update(skip_locked=True)
            .filter(
                block__gte=current_block.number
                - config.DYNAMIC_SYNTHETIC_JOBS_PLANNER_MAX_OVERSLEEP_BLOCKS,
                block__lte=current_block.number + wait_in_advance_blocks,
                started_at__isnull=True,
            )
            .order_by("block")
        )
        if not ongoing_synthetic_job_batches:
            logger.debug(
                "No ongoing scheduled synthetic jobs, current block is %s",
                current_block.number,
            )
            return

        if len(ongoing_synthetic_job_batches) > 1:
            logger.warning(
                "More than one scheduled synthetic jobs found (%s)",
                ongoing_synthetic_job_batches,
            )

        batch = ongoing_synthetic_job_batches[0]
        target_block = batch.block
        blocks_to_wait = target_block - current_block.number
        if blocks_to_wait < 0:
            logger.info(
                "Overslept a batch run, but still within acceptable margin, batch_id: %s, should_run_at_block: %s, current_block: %s",
                batch.id,
                batch.block,
                current_block.number,
            )
            SystemEvent.objects.using(settings.DEFAULT_DB_ALIAS).create(
                type=SystemEvent.EventType.VALIDATOR_OVERSLEPT_SCHEDULED_JOB_WARNING,
                subtype=SystemEvent.EventSubType.WARNING,
                long_description="Overslept a batch run, but still within acceptable margin",
                data={
                    "batch_id": batch.id,
                    "batch_created_at": str(batch.created_at),
                    "should_run_at_block": batch.block,
                    "current_block": current_block.number,
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
                current_block = async_to_sync(bittensor.blocks.head)()

                if current_block.number >= target_block:
                    break

                logger.debug(
                    "Waiting for block %s, current block is %s, sleeping for %s",
                    target_block,
                    current_block.number,
                    poll_interval,
                )
                time.sleep(poll_interval.total_seconds())
            else:
                logger.error(
                    "Failed to wait for target block %s, current block is %s",
                    target_block,
                    current_block.number,
                )
                SystemEvent.objects.using(settings.DEFAULT_DB_ALIAS).create(
                    type=SystemEvent.EventType.VALIDATOR_SYNTHETIC_JOBS_FAILURE,
                    subtype=SystemEvent.EventSubType.FAILED_TO_WAIT,
                    long_description="Failed to await the right block to run at",
                    data={
                        "batch_id": batch.id,
                        "batch_created_at": str(batch.created_at),
                        "should_run_at_block": batch.block,
                        "current_block": current_block.number,
                    },
                )
                return

        batch.started_at = now()
        batch.save()

    _run_synthetic_jobs.apply_async(kwargs={"synthetic_jobs_batch_id": batch.id})


@app.task()
@bittensor_client
def check_missed_synthetic_jobs(bittensor: turbobt.Bittensor) -> None:
    """
    Check if there are any synthetic jobs that were scheduled to run, but didn't.
    """
    current_block = async_to_sync(bittensor.blocks.head)()

    with transaction.atomic():
        past_job_batches = SyntheticJobBatch.objects.select_for_update(skip_locked=True).filter(
            block__lt=current_block.number
            - config.DYNAMIC_SYNTHETIC_JOBS_PLANNER_MAX_OVERSLEEP_BLOCKS,
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
                    "current_block": current_block.number,
                    "current_time": str(now()),
                },
            )
        past_job_batches.update(is_missed=True)


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

        async with turbobt.Bittensor(settings.BITTENSOR_NETWORK) as bittensor:
            current_block = await bittensor.blocks.head()

        job = await OrganicJob.objects.acreate(
            job_uuid=str(job_request.uuid),
            miner=miner,
            miner_address=miner.address,
            miner_address_ip_version=miner.ip_version,
            miner_port=miner.port,
            executor_class=job_request.executor_class,
            job_description="Validator Job from Admin Panel",
            block=current_block.number,
        )

        my_keypair = get_keypair()
        miner_client = MinerClient(
            miner_hotkey=miner.hotkey,
            miner_address=miner.address,
            miner_port=miner.port,
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
    await drive_organic_job(
        miner_client,
        job,
        job_request,
        notify_callback=callback,
    )


def save_receipt_event(subtype: str, long_description: str, data: JsonValue):
    SystemEvent.objects.using(settings.DEFAULT_DB_ALIAS).create(
        type=SystemEvent.EventType.RECEIPT_FAILURE,
        subtype=subtype,
        long_description=long_description,
        data=data,
    )


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


def save_metagraph_snapshot(
    neurons: list[turbobt.Neuron],
    subnet_state: turbobt.subnet.SubnetState,
    block: turbobt.Block,
    snapshot_type: MetagraphSnapshot.SnapshotType = MetagraphSnapshot.SnapshotType.LATEST,
) -> None:
    MetagraphSnapshot.objects.update_or_create(
        id=snapshot_type,  # current metagraph snapshot
        defaults={
            "block": block.number,
            "updated_at": now(),
            "alpha_stake": subnet_state["alpha_stake"],
            "tao_stake": subnet_state["tao_stake"],
            "stake": subnet_state["total_stake"],
            "uids": [neuron.uid for neuron in neurons],
            "hotkeys": [neuron.hotkey for neuron in neurons],
            "coldkeys": [neuron.coldkey for neuron in neurons],
            "serving_hotkeys": [
                neuron.hotkey
                for neuron in neurons
                if neuron.axon_info and str(neuron.axon_info.ip) != "0.0.0.0"
            ],
        },
    )


@app.task
@bittensor_client
def sync_metagraph(bittensor: turbobt.Bittensor) -> None:
    neurons, subnet_state, block = async_to_sync(_get_metagraph_for_sync)(bittensor)

    if not block:
        return

    # save current cycle start metagraph snapshot
    current_cycle = get_cycle_containing_block(block=block.number, netuid=settings.BITTENSOR_NETUID)

    # check metagraph sync lag
    previous_block = None
    try:
        previous_metagraph = MetagraphSnapshot.get_latest()
        if previous_metagraph:
            previous_block = previous_metagraph.block
    except Exception as e:
        logger.warning(f"Failed to fetch previous metagraph snapshot block: {e}")
    blocks_diff = block.number - previous_block if previous_block else None
    if blocks_diff is not None and blocks_diff != 1:
        if blocks_diff == 0:
            return
        else:
            msg = f"Metagraph is {blocks_diff} blocks lagging - previous: {previous_block}, current: {block.number}"
            logger.warning(msg)
            SystemEvent.objects.using(settings.DEFAULT_DB_ALIAS).create(
                type=SystemEvent.EventType.METAGRAPH_SYNCING,
                subtype=SystemEvent.EventSubType.WARNING,
                long_description=msg,
                data={
                    "blocks_diff": blocks_diff,
                    "previous_block": previous_block,
                    "block": block.number,
                },
            )

    save_metagraph_snapshot(neurons, subnet_state, block)

    # sync neurons
    current_hotkeys = [neuron.hotkey for neuron in neurons]
    miners = list(Miner.objects.filter(hotkey__in=current_hotkeys).all())
    existing_hotkeys = {m.hotkey for m in miners}
    new_hotkeys = set(current_hotkeys) - existing_hotkeys
    if len(new_hotkeys) > 0:
        new_miners = []
        hotkey_to_neuron = {neuron.hotkey: neuron for neuron in neurons}
        for hotkey in new_hotkeys:
            neuron = hotkey_to_neuron.get(hotkey)
            coldkey = neuron.coldkey if neuron else None
            new_miners.append(Miner(hotkey=hotkey, coldkey=coldkey))
        new_miners = Miner.objects.bulk_create(new_miners)
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
        if (
            neuron
            and neuron.axon_info
            and (
                miner.uid != neuron.uid
                or miner.address
                != getattr(
                    neuron.axon_info,
                    "shield_address",
                    str(neuron.axon_info.ip),
                )
                or miner.port != neuron.axon_info.port
                or miner.ip_version != neuron.axon_info.ip.version
                or miner.coldkey != neuron.coldkey
            )
        ):
            miner.uid = neuron.uid
            miner.address = getattr(
                neuron.axon_info,
                "shield_address",
                str(neuron.axon_info.ip),
            )
            miner.port = neuron.axon_info.port
            miner.ip_version = neuron.axon_info.ip.version
            miner.coldkey = neuron.coldkey
            miners_to_update.append(miner)

    if miners_to_update:
        Miner.objects.bulk_update(
            miners_to_update, fields=["uid", "address", "port", "ip_version", "coldkey"]
        )
        logger.info(f"Updated axon infos and null coldkeys for {len(miners_to_update)} miners")

    data = {
        "block": block.number,
        "new_neurons": len(new_hotkeys),
        "updated_axon_infos": len(miners_to_update),
    }
    SystemEvent.objects.using(settings.DEFAULT_DB_ALIAS).create(
        type=SystemEvent.EventType.VALIDATOR_MINERS_REFRESH,
        subtype=SystemEvent.EventSubType.SUCCESS,
        data=data,
    )

    # update cycle start metagraph snapshot if cycle has changed
    cycle_start_metagraph = None
    try:
        cycle_start_metagraph = MetagraphSnapshot.get_cycle_start()
    except Exception as e:
        logger.warning(f"Failed to fetch cycle start metagraph snapshot: {e}")
    if cycle_start_metagraph is None or cycle_start_metagraph.block != current_cycle.start:
        neurons, subnet_state, block = async_to_sync(_get_metagraph_for_sync)(
            bittensor,
            block_number=current_cycle.start,
        )

        if not block:
            return

        save_metagraph_snapshot(
            neurons,
            subnet_state,
            block,
            snapshot_type=MetagraphSnapshot.SnapshotType.CYCLE_START,
        )


async def get_manifests_from_miners(
    miners: list[Miner],
    timeout: float = 30,
) -> dict[str, dict[ExecutorClass, int]]:
    """
    Connect to multiple miner clients in parallel and retrieve their manifests.

    Args:
        miner_clients: List of OrganicMinerClient instances to connect to
        timeout: Maximum time to wait for manifest retrieval in seconds

    Returns:
        Dictionary mapping miner hotkeys to their executor manifests
    """

    miner_clients = [
        OrganicMinerClient(
            miner_hotkey=miner.hotkey,
            miner_address=miner.address,
            miner_port=miner.port,
            job_uuid="ignore",
            my_keypair=get_keypair(),
        )
        for miner in miners
    ]

    try:
        logger.info(f"Scraping manifests for {len(miner_clients)} miners")
        tasks = [
            asyncio.create_task(
                get_single_manifest(client, timeout), name=f"{client.miner_hotkey}.get_manifest"
            )
            for client in miner_clients
        ]
        results = await asyncio.gather(*tasks)

        # Process results and build the manifest dictionary
        result_manifests = {}
        for hotkey, manifest in results:
            if manifest is not None:
                result_manifests[hotkey] = manifest

        return result_manifests

    finally:
        close_tasks = [
            asyncio.create_task(client.close(), name=f"{client.miner_hotkey}.close")
            for client in miner_clients
        ]
        await asyncio.gather(*close_tasks, return_exceptions=True)


@app.task
def fetch_dynamic_config() -> None:
    with transaction.atomic():
        try:
            get_advisory_lock(LockType.DYNAMIC_CONFIG_FETCH)
        except Locked:
            logger.debug("fetch_dynamic_config: another instance is running; skipping")
            return

        _do_fetch()


@tenacity.retry(
    stop=tenacity.stop_after_attempt(5),
    wait=tenacity.wait_chain(
        tenacity.wait_fixed(5),
        tenacity.wait_fixed(30),
        tenacity.wait_fixed(60),
        tenacity.wait_fixed(180),
    ),
    reraise=True,
)
def _do_fetch() -> None:
    if settings.USE_CONTRACT_CONFIG:
        dynamic_configs = get_dynamic_config_types_from_settings()
        fetch_dynamic_configs_from_contract(
            dynamic_configs,
            settings.CONFIG_CONTRACT_ADDRESS,
            namespace=config,
        )
        return

    # if same key exists in both places, common config wins
    sync_dynamic_config(
        config_url=(
            "https://raw.githubusercontent.com/backend-developers-ltd/compute-horde-dynamic-config/"
            f"master/validator-config-{settings.DYNAMIC_CONFIG_ENV}.json"
        ),
        namespace=config,
    )
    sync_dynamic_config(
        config_url=(
            "https://raw.githubusercontent.com/backend-developers-ltd/compute-horde-dynamic-config/"
            f"master/common-config-{settings.DYNAMIC_CONFIG_ENV}.json"
        ),
        namespace=config,
    )


@app.task(
    soft_time_limit=4 * 60 + 50,
    time_limit=5 * 60,
)
def llm_prompt_generation():
    unprocessed_workloads_count = SolveWorkload.objects.filter(finished_at__isnull=True).count()
    if unprocessed_workloads_count > 0:
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
    SystemEvent.objects.create(
        type=SystemEvent.EventType.LLM_PROMPT_GENERATION,
        subtype=SystemEvent.EventSubType.PROMPT_GENERATION_STARTED,
        long_description="",
        data={
            "prompt_series_count": num_prompt_series,
            "expected_prompt_series_count": num_expected_prompt_series,
        },
    )

    with transaction.atomic():
        try:
            get_advisory_lock(LockType.TRUSTED_MINER_LOCK)
        except Locked:
            logger.debug("Another thread already using the trusted miner")
            return

        try:
            async_to_sync(generate_prompts)()
        except Exception as e:
            msg = f"Error while generating prompts: {e}"
            logger.warning(msg)
            SystemEvent.objects.create(
                type=SystemEvent.EventType.LLM_PROMPT_GENERATION,
                subtype=SystemEvent.EventSubType.FAILURE,
                long_description=msg,
                data={},
            )


@app.task(
    soft_time_limit=4 * 60 + 50,
    time_limit=5 * 60,
)
def llm_prompt_answering():
    started_at = now()
    unprocessed_workloads = SolveWorkload.objects.filter(finished_at__isnull=True)

    SystemEvent.objects.create(
        type=SystemEvent.EventType.LLM_PROMPT_ANSWERING,
        subtype=SystemEvent.EventSubType.UNPROCESSED_WORKLOADS,
        long_description="number of unprocessed workloads to be answered",
        data={
            "count": unprocessed_workloads.count(),
        },
    )

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
                break

            try:
                success = async_to_sync(answer_prompts)(workload)
            except Exception as e:
                success = False
                msg = f"Error while answering prompts: {e}"
                logger.warning(msg)
                SystemEvent.objects.create(
                    type=SystemEvent.EventType.LLM_PROMPT_ANSWERING,
                    subtype=SystemEvent.EventSubType.FAILURE,
                    long_description=msg,
                    data={},
                )

        if success:
            success_count += 1
        else:
            failure_count += 1
        times.append(time.time() - start)
        total_time = sum(times)
        avg_time = total_time / len(times)
        if total_time + avg_time > 4 * 60 + 20:
            break

    completed_at = now()
    if times:
        SystemEvent.objects.create(
            type=SystemEvent.EventType.LLM_PROMPT_ANSWERING,
            subtype=SystemEvent.EventSubType.SUCCESS,
            long_description="Finished running prompt answering jobs",
            data={
                "started_at": started_at.isoformat(),
                "completed_at": completed_at.isoformat(),
                "task_duration": (completed_at - started_at).total_seconds(),
                "times": times,
                "job_success_count": success_count,
                "job_failure_count": failure_count,
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
        SystemEvent.objects.create(
            type=SystemEvent.EventType.LLM_PROMPT_SAMPLING,
            subtype=SystemEvent.EventSubType.PROMPT_SAMPLING_SKIPPED,
            long_description="not enough prompt series in the database to start sampling",
            data={
                "prompt_series_count": num_prompt_series,
                "required_prompt_series_count_to_start_sampling": required_series_to_start_sampling,
            },
        )
        return

    num_unused_prompt_samples = PromptSample.objects.filter(synthetic_job__isnull=True).count()
    num_needed_prompt_samples = (
        config.DYNAMIC_TARGET_NUMBER_OF_PROMPT_SAMPLES_READY - num_unused_prompt_samples
    )

    if num_needed_prompt_samples <= 0:
        logger.warning(
            "There are already %s prompt samples in the db not used in synthetic jobs - skipping prompt sampling",
            num_unused_prompt_samples,
        )
        SystemEvent.objects.create(
            type=SystemEvent.EventType.LLM_PROMPT_SAMPLING,
            subtype=SystemEvent.EventSubType.PROMPT_SAMPLING_SKIPPED,
            long_description="enough prompt samples unused in synthetic jobs in the database",
            data={
                "unused_prompt_samples_count": num_unused_prompt_samples,
                "target_unused_prompt_samples_count": config.DYNAMIC_TARGET_NUMBER_OF_PROMPT_SAMPLES_READY,
            },
        )
        return

    logger.info(
        "We need %s more prompt samples in the db for synthetic jobs - generating prompt answering workloads",
        num_needed_prompt_samples,
    )

    num_workloads_created = create_sample_workloads(num_needed_prompt_samples)
    SystemEvent.objects.create(
        type=SystemEvent.EventType.LLM_PROMPT_SAMPLING,
        subtype=SystemEvent.EventSubType.NEW_WORKLOADS_CREATED,
        long_description="number of new workloads for prompt answering created",
        data={
            "new_workloads_count": num_workloads_created,
            "prompt_series_count": num_prompt_series,
            "unused_prompt_samples_count": num_unused_prompt_samples,
        },
    )


def persist_workload(
    workload: SolveWorkload, prompt_samples: list[PromptSample], prompts: list[Prompt]
):
    logger.info(f"Saving workload {workload}")
    try:
        # save the sampled prompts as unanswered in the db
        with transaction.atomic():
            workload.save()
            PromptSample.objects.bulk_create(prompt_samples)
            Prompt.objects.bulk_create(prompts)
    except Exception:
        logger.error(f"Failed to create workload {workload}")


def create_sample_workloads(num_needed_prompt_samples: int) -> int:
    """
    Creates enough workloads to cover at least `num_needed_prompt_samples` prompt samples
    Returns the number of workloads created
    """
    prompts_per_sample = config.DYNAMIC_NUMBER_OF_PROMPTS_TO_SAMPLE_FROM_SERIES
    prompts_per_workload = config.DYNAMIC_NUMBER_OF_PROMPTS_PER_WORKLOAD

    # set seed for the current synthetic jobs run
    seed = random.randint(0, MAX_SEED)

    # workload we are currently sampling for
    try:
        current_workload, current_upload_url = init_workload(seed)
    except Exception as e:
        logger.error(f"Failed to create new workload: {e} - aborting prompt sampling")
        return 0

    # how many prompts series we sampled so far
    # for each prompt series there is one prompt sample
    num_prompt_series_sampled = 0
    num_workloads_created = 0

    current_prompt_samples = []
    current_prompts = []

    # assume we have sufficient prompt series in the db to make all the prompt_samples needed
    # take a random order of prompt series to avoid using the same series at each synthetic jobs run
    for prompt_series in PromptSeries.objects.order_by("?").all():
        # get all prompts
        try:
            lines = download_prompts_from_s3_url(prompt_series.s3_url)
        except Exception as e:
            msg = f"Failed to download prompt series from {prompt_series.s3_url}: {e} - skipping"
            logger.error(msg, exc_info=True)
            SystemEvent.objects.create(
                type=SystemEvent.EventType.LLM_PROMPT_SAMPLING,
                subtype=SystemEvent.EventSubType.ERROR_DOWNLOADING_FROM_S3,
                long_description=msg,
            )
            continue

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
            try:
                upload_prompts_to_s3_url(current_upload_url, content)

                # save the workload in the db
                persist_workload(current_workload, current_prompt_samples, current_prompts)
                num_prompt_series_sampled += len(current_prompt_samples)
                num_workloads_created += 1

            except Exception as e:
                msg = f"Failed to upload prompts to s3 {current_upload_url} for workload {current_workload.workload_uuid}: {e} - skipping"
                logger.error(msg, exc_info=True)
                SystemEvent.objects.create(
                    type=SystemEvent.EventType.LLM_PROMPT_SAMPLING,
                    subtype=SystemEvent.EventSubType.ERROR_UPLOADING_TO_S3,
                    long_description=msg,
                )

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
    return num_workloads_created


@app.task
def evict_old_data():
    eviction.evict_all()


async def execute_organic_job_request_on_worker(
    job_request: OrganicJobRequest, job_route: JobRoute
) -> OrganicJob:
    """
    Sends the job request to be executed on a celery worker and waits for the result.
    Returns the OrganicJob created for the request (success or not).
    Hint: the task also sends job status updates to a django channel:
        ```
        while True:
            msg = await get_channel_layer().receive(f"job_status_updates__{job_uuid}")
        ```
    """
    timeout = await aget_config("ORGANIC_JOB_CELERY_WAIT_TIMEOUT")
    future_result: AsyncResult[None] = _execute_organic_job_on_worker.apply_async(
        args=(job_request.model_dump(), job_route.model_dump()),
        expires=timeout,
    )
    # Note - thread sensitive is essential, otherwise the wait will block the sync thread.
    # If this poses to be a problem, another approach is to asyncio.sleep then poll for the result (in a loop)
    await sync_to_async(future_result.get, thread_sensitive=False)(timeout=timeout)
    return await OrganicJob.objects.aget(job_uuid=job_request.uuid)


@app.task
def _execute_organic_job_on_worker(job_request: JsonValue, job_route: JsonValue) -> None:
    request: OrganicJobRequest = TypeAdapter(OrganicJobRequest).validate_python(job_request)
    route: JobRoute = TypeAdapter(JobRoute).validate_python(job_route)
    async_to_sync(execute_organic_job_request)(request, route)


@app.task
def slash_collateral_task(job_uuid: str) -> None:
    with transaction.atomic():
        job = OrganicJob.objects.select_related("miner").select_for_update().get(job_uuid=job_uuid)

        if job.slashed:
            logger.info(f"Already slashed for this job {job_uuid}")
            return

        try:
            async_to_sync(collateral().slash_collateral)(
                miner_hotkey=job.miner.hotkey,
                url=f"job {job_uuid} cheated",
            )
        except Exception as e:
            logger.error(f"Failed to slash collateral for job {job_uuid}: {e}")
        else:
            job.slashed = True
            job.save()


async def _get_latest_manifests(miners: list[Miner]) -> dict[str, dict[ExecutorClass, int]]:
    """
    Get manifests from periodic polling data stored in MinerManifest table.
    """
    if not miners:
        return {}

    miner_hotkeys = [miner.hotkey for miner in miners]

    latest_manifests = [
        m
        async for m in MinerManifest.objects.filter(miner__hotkey__in=miner_hotkeys)
        .distinct("miner__hotkey", "executor_class")
        .order_by("miner__hotkey", "executor_class", "-created_at")
        .values("miner__hotkey", "executor_class", "online_executor_count")
    ]

    manifests_dict: dict[str, dict[ExecutorClass, int]] = {}

    for manifest_record in latest_manifests:
        hotkey = manifest_record["miner__hotkey"]
        executor_class = ExecutorClass(manifest_record["executor_class"])
        online_count = manifest_record["online_executor_count"]

        if hotkey not in manifests_dict:
            manifests_dict[hotkey] = {}

        manifests_dict[hotkey][executor_class] = online_count

    return manifests_dict


@app.task
def poll_miner_manifests() -> None:
    """
    Poll all miners for their manifests and update the MinerManifest table.
    This runs independently of synthetic job batches.
    """
    async_to_sync(_poll_miner_manifests)()


async def _poll_miner_manifests() -> None:
    """
    Poll miners connected to this validator for their manifests and update the database.
    """
    try:
        metagraph = await MetagraphSnapshot.objects.aget(id=MetagraphSnapshot.SnapshotType.LATEST)
        serving_hotkeys = metagraph.get_serving_hotkeys()

        if not serving_hotkeys:
            logger.info("No serving miners in metagraph, skipping manifest polling")
            return

        miners = [m async for m in Miner.objects.filter(hotkey__in=serving_hotkeys)]

        if not miners:
            logger.info("No serving miners found in database, skipping manifest polling")
            return

        logger.info(f"Polling manifests from {len(miners)} serving miners")

    except MetagraphSnapshot.DoesNotExist:
        logger.warning("No metagraph snapshot found, skipping manifest polling")
        return

    manifests_dict = await get_manifests_from_miners(miners, timeout=30)

    manifest_records = []

    for miner in miners:
        manifest = manifests_dict.get(miner.hotkey, {})

        if manifest:
            for executor_class, executor_count in manifest.items():
                manifest_records.append(
                    MinerManifest(
                        miner=miner,
                        batch=None,
                        executor_class=executor_class,
                        executor_count=executor_count,
                        online_executor_count=executor_count,
                    )
                )
            logger.debug(f"Stored manifest for {miner.hotkey}: {manifest}")
        else:
            last_manifests = [
                m
                async for m in MinerManifest.objects.filter(
                    miner=miner,
                )
                .distinct("executor_class")
                .order_by("executor_class", "-created_at")
            ]

            if last_manifests:
                for last_manifest in last_manifests:
                    manifest_records.append(
                        MinerManifest(
                            miner=miner,
                            batch=None,
                            executor_class=last_manifest.executor_class,
                            executor_count=last_manifest.executor_count,
                            online_executor_count=0,
                        )
                    )
            logger.warning(f"No manifest received from {miner.hotkey}, marked as offline")

    if manifest_records:
        await MinerManifest.objects.abulk_create(manifest_records)
        logger.info(f"Stored {len(manifest_records)} manifests")
    logger.info(f"Manifest polling complete: {len(manifest_records)} manifests stored")
