import asyncio
import json
import time

import requests
import tenacity
import turbobt
import turbobt.substrate.exceptions
from asgiref.sync import async_to_sync, sync_to_async
from celery import shared_task
from celery.result import AsyncResult
from celery.utils.log import get_task_logger
from celery.utils.time import get_exponential_backoff_interval
from compute_horde.dynamic_config import fetch_dynamic_configs_from_contract, sync_dynamic_config
from compute_horde.fv_protocol.facilitator_requests import OrganicJobRequest
from compute_horde.miner_client.organic import OrganicMinerClient
from compute_horde.smart_contracts.map_contract import get_dynamic_config_types_from_settings
from compute_horde_core.executor_class import ExecutorClass
from constance import config
from django.conf import settings
from django.db import transaction
from django.utils.timezone import now
from pydantic import JsonValue, TypeAdapter
from web3.exceptions import ProviderConnectionError

from compute_horde_validator.celery import app
from compute_horde_validator.validator.collateral.default import collateral
from compute_horde_validator.validator.locks import Locked, LockType, get_advisory_lock
from compute_horde_validator.validator.models import (
    Miner,
    OrganicJob,
    SystemEvent,
)
from compute_horde_validator.validator.organic_jobs.miner_client import MinerClient
from compute_horde_validator.validator.organic_jobs.miner_driver import (
    drive_organic_job,
    execute_organic_job_request,
)
from compute_horde_validator.validator.routing.types import JobRoute

from . import (
    eviction,
    miner_sync,  # noqa
)
from .allowance import tasks as allowance_tasks  # noqa
from .allowance.default import allowance
from .clean_me_up import get_single_manifest
from .collateral import tasks as collateral_tasks  # noqa
from .collateral.types import (
    NonceTooHighCollateralException,
    NonceTooLowCollateralException,
    ReplacementUnderpricedCollateralException,
)
from .dynamic_config import aget_config
from .models import AdminJobRequest, MinerManifest
from .scoring import tasks as scoring_tasks  # noqa

if False:
    import torch  # noqa

logger = get_task_logger(__name__)

JOB_WINDOW = 2 * 60 * 60
MAX_SEED = (1 << 32) - 1

COMPUTE_TIME_OVERHEAD_SECONDS = 30  # TODO: approximate a realistic value

SLASH_COLLATERAL_TASK_MAX_RETRIES = 5
SLASH_COLLATERAL_TASK_RETRY_FACTOR_SECONDS = 2
SLASH_COLLATERAL_TASK_RETRY_MAXIMUM_SECONDS = 120
SLASH_COLLATERAL_TASK_RETRIABLE_ERRORS = (
    NonceTooLowCollateralException,
    NonceTooHighCollateralException,
    ReplacementUnderpricedCollateralException,
    ProviderConnectionError,
)


class ScheduleError(Exception):
    pass


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


@app.task
def evict_old_data():
    eviction.evict_all()


def wait_for_celery_result(task_id: str, timeout: float | None = None) -> None:
    # WARNING - The following two lines must execute on the same thread.
    # Don't use sync_to_async on future_result.get after constructing AsyncResult.
    # If needed, use sync_to_async on this whole function.
    # Moving future_result to a different thread can corrupt the redis connection state
    # (redis backends of celery are not thread safe).
    future_result: AsyncResult[None] = AsyncResult(task_id, app=app)
    return future_result.get(timeout=timeout)


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
    await sync_to_async(wait_for_celery_result, thread_sensitive=False)(
        future_result.id, timeout=timeout
    )
    return await OrganicJob.objects.aget(job_uuid=job_request.uuid)


@app.task
def _execute_organic_job_on_worker(job_request: JsonValue, job_route: JsonValue) -> None:
    request: OrganicJobRequest = TypeAdapter(OrganicJobRequest).validate_python(job_request)
    route: JobRoute = TypeAdapter(JobRoute).validate_python(job_route)
    async_to_sync(execute_organic_job_request)(request, route)


@app.task(bind=True, max_retries=SLASH_COLLATERAL_TASK_MAX_RETRIES)
def slash_collateral_task(self, job_uuid: str) -> None:
    with transaction.atomic():
        job = OrganicJob.objects.select_related("miner").select_for_update().get(job_uuid=job_uuid)

        if job.slashed:
            logger.info("Already slashed for this job %s", job_uuid)
            return

        logger.info("Slashing collateral for job %s on miner %s", job_uuid, job.miner.hotkey)

        try:
            async_to_sync(collateral().slash_collateral)(
                miner_hotkey=job.miner.hotkey,
                url=f"job {job_uuid} cheated",
            )
        except SLASH_COLLATERAL_TASK_RETRIABLE_ERRORS as e:
            countdown = get_exponential_backoff_interval(
                factor=SLASH_COLLATERAL_TASK_RETRY_FACTOR_SECONDS,
                retries=self.request.retries,
                maximum=SLASH_COLLATERAL_TASK_RETRY_MAXIMUM_SECONDS,
                full_jitter=False,
            )
            logger.warning(
                "Retriable slashing error for job %s (%s). Retry %d in %ss",
                job_uuid,
                str(e),
                self.request.retries + 1,
                countdown,
            )

            try:
                raise self.retry(exc=e, countdown=countdown)
            except SLASH_COLLATERAL_TASK_RETRIABLE_ERRORS as e:
                logger.exception("Max retries reached for job %s: %s", job_uuid, e)
        except Exception as e:
            logger.exception("Failed to slash collateral for job %s: %s", job_uuid, e)
        else:
            job.slashed = True
            job.slashed_at = now()
            job.save(update_fields=["slashed", "slashed_at"])


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
    """
    async_to_sync(_poll_miner_manifests)()


async def _poll_miner_manifests() -> None:
    """
    Poll miners connected to this validator for their manifests and update the database.
    """
    metagraph = await sync_to_async(allowance().get_metagraph, thread_sensitive=False)()

    if not metagraph.serving_hotkeys:
        logger.info("No serving miners in metagraph, skipping manifest polling")
        return

    miners = [m async for m in Miner.objects.filter(hotkey__in=metagraph.serving_hotkeys)]

    if not miners:
        logger.info("No serving miners found in database, skipping manifest polling")
        return

    logger.info(f"Polling manifests from {len(miners)} serving miners")

    manifests_dict = await get_manifests_from_miners(miners, timeout=30)

    manifest_records = []

    for miner in miners:
        manifest = manifests_dict.get(miner.hotkey, {})

        if manifest:
            for executor_class, executor_count in manifest.items():
                manifest_records.append(
                    MinerManifest(
                        miner=miner,
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
