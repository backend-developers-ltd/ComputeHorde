import logging
from datetime import timedelta
from typing import assert_never

import bittensor
from compute_horde.executor_class import EXECUTOR_CLASS
from compute_horde.fv_protocol.facilitator_requests import (
    OrganicJobRequest,
    V2JobRequest,
)
from compute_horde.receipts.models import JobFinishedReceipt, JobStartedReceipt
from compute_horde.subtensor import get_cycle_containing_block
from compute_horde.utils import async_synchronized
from django.conf import settings
from django.utils import timezone

from compute_horde_validator.validator.dynamic_config import aget_config
from compute_horde_validator.validator.models import (
    ComputeTimeAllowance,
    MetagraphSnapshot,
    Miner,
    MinerBlacklist,
    MinerManifest,
    MinerPreliminaryReservation,
    OrganicJob,
    SystemEvent,
)
from compute_horde_validator.validator.utils import TRUSTED_MINER_FAKE_KEY

logger = logging.getLogger(__name__)


class JobRoutingException(Exception):
    pass


class NoMinerForExecutorType(JobRoutingException):
    pass


class AllMinersBusy(JobRoutingException):
    pass


class MinerIsBlacklisted(JobRoutingException):
    pass


class NotEnoughTimeInCycle(JobRoutingException):
    pass


class NoMinerWithEnoughAllowance(JobRoutingException):
    pass


async def pick_miner_for_job_request(request: OrganicJobRequest) -> Miner:
    if settings.DEBUG_MINER_KEY:
        miner, _ = await Miner.objects.aget_or_create(hotkey=settings.DEBUG_MINER_KEY)
        return miner

    if isinstance(request, V2JobRequest):
        return await pick_miner_for_job_v2(request)

    assert_never(request)


@async_synchronized
async def pick_miner_for_job_v2(request: V2JobRequest) -> Miner:
    """
    Goes through all miners with recent manifests and online executors of the given executor class.
    Returns a random miner that may have a non-busy executor based on known receipts.
    """

    executor_class = request.executor_class
    logger.info(f"Picking a miner for job {request.uuid} with executor class {executor_class}")

    if request.on_trusted_miner:
        logger.debug(f"Using trusted miner for job {request.uuid}")
        miner, _ = await Miner.objects.aget_or_create(hotkey=TRUSTED_MINER_FAKE_KEY)
        return miner

    executor_seconds = (
        request.download_time_limit + request.execution_time_limit + request.upload_time_limit
    )

    block: int | None = None
    try:
        block = (await MetagraphSnapshot.aget_latest()).block
    except Exception as exc:
        logger.warning(f"Failed to get latest metagraph snapshot: {exc}")
        async with bittensor.AsyncSubtensor(network=settings.BITTENSOR_NETWORK) as subtensor:
            block = await subtensor.get_current_block()
    assert block is not None, "Failed to get current block from cache or subtensor."

    cycle = get_cycle_containing_block(block, netuid=settings.BITTENSOR_NETUID)
    time_remaining_in_cycle = (cycle.stop - block) * settings.BITTENSOR_APPROXIMATE_BLOCK_DURATION

    time_required = (
        executor_seconds
        + await aget_config("DYNAMIC_EXECUTOR_RESERVATION_TIME_LIMIT")
        + await aget_config("DYNAMIC_EXECUTOR_STARTUP_TIME_LIMIT")
        + EXECUTOR_CLASS[executor_class].spin_up_time
    )

    if time_remaining_in_cycle < timedelta(seconds=time_required):
        raise NotEnoughTimeInCycle()

    manifests_qs = (
        MinerManifest.objects.select_related("miner")
        .filter(
            miner__in=Miner.objects.non_blacklisted(),
            executor_class=str(executor_class),
            created_at__gte=timezone.now() - timedelta(hours=4),
        )
        .order_by("created_at")
    )
    manifests = [manifest async for manifest in manifests_qs.all()]

    # Only accept the latest manifest for each miner
    latest_miner_manifest: dict[str, MinerManifest] = {}
    for manifest in manifests:
        latest_miner_manifest[manifest.miner.hotkey] = manifest

    for manifest in latest_miner_manifest.values():
        logger.debug(
            f"Latest {manifest.miner.hotkey} manifest has {manifest.online_executor_count} executors of type {executor_class}"
        )

    # Discard manifests that explicitly say there are no executors of this type
    latest_miner_manifest = {
        hotkey: manifest
        for hotkey, manifest in latest_miner_manifest.items()
        if manifest.online_executor_count > 0
    }

    if not latest_miner_manifest:
        logger.error(f"Failed to find a miner with available executors of type {executor_class}")
        raise NoMinerForExecutorType()

    # filter/sort miners based on available allowance
    allowance_qs = ComputeTimeAllowance.objects.select_related("miner").filter(
        cycle__start__lte=block,
        cycle__stop__gt=block,
        miner__hotkey__in=latest_miner_manifest.keys(),
        validator__hotkey=settings.BITTENSOR_WALLET().hotkey.ss58_address,
        remaining_allowance__gte=executor_seconds,
    )
    allowances = [allowance async for allowance in allowance_qs.all()]

    if not allowances:
        raise NoMinerWithEnoughAllowance()

    allowances.sort(
        key=lambda allowance: (
            # percentage of remaining allowance
            allowance.remaining_allowance / allowance.initial_allowance,
            # miner collateral as a tiebreaker
            allowance.miner.collateral_wei,
        ),
        reverse=True,
    )

    minimum_collateral = await aget_config("DYNAMIC_MINIMUM_COLLATERAL_AMOUNT_WEI")

    for allowance in allowances:
        miner = allowance.miner
        manifest = latest_miner_manifest[miner.hotkey]
        if settings.COLLATERAL_CONTRACT_ADDRESS and int(miner.collateral_wei) < minimum_collateral:
            logger.warning(
                f"Miner {manifest.miner.hotkey} has {int(miner.collateral_wei)} collateral, but required minimum is {minimum_collateral}"
            )
            continue

        preliminary_reservation_jobs: set[str] = {
            str(job_uuid)
            async for job_uuid in MinerPreliminaryReservation.objects.active()
            .filter(
                miner=miner,
                executor_class=str(executor_class),
            )
            .values_list("job_uuid", flat=True)
        }

        known_started_jobs: set[str] = {
            str(job_uuid)
            async for job_uuid in JobStartedReceipt.objects.valid_at(timezone.now())
            .filter(miner_hotkey=miner.hotkey)
            .values_list("job_uuid", flat=True)
        }

        known_finished_jobs: set[str] = {
            str(job_uuid)
            async for job_uuid in JobFinishedReceipt.objects.filter(
                job_uuid__in=known_started_jobs | preliminary_reservation_jobs,
                miner_hotkey=miner.hotkey,
            ).values_list("job_uuid", flat=True)
        }

        maybe_ongoing_jobs = (
            preliminary_reservation_jobs | known_started_jobs
        ) - known_finished_jobs

        logger.debug(
            f"Miner {manifest.miner.hotkey} has "
            f"{len(preliminary_reservation_jobs)} preliminary reservations, "
            f"{len(known_started_jobs)} known started, "
            f"{len(known_finished_jobs)} known finished, "
            f"{len(maybe_ongoing_jobs)} maybe ongoing jobs"
        )

        if len(maybe_ongoing_jobs) < manifest.online_executor_count:
            reservation_time = await aget_config(
                "DYNAMIC_ROUTING_PRELIMINARY_RESERVATION_TIME_SECONDS"
            )
            await MinerPreliminaryReservation.objects.acreate(
                miner=miner,
                executor_class=executor_class,
                job_uuid=request.uuid,
                expires_at=timezone.now() + timedelta(seconds=reservation_time),
            )
            logger.info(f"Picked miner {manifest.miner.hotkey}")
            return miner

    logger.error("All miners are busy")
    raise AllMinersBusy()


async def report_miner_failed_job(job: OrganicJob) -> None:
    if job.status != OrganicJob.Status.FAILED:
        logger.info(
            f"Not blacklisting miner: job {job.job_uuid} is not failed (status={job.status})"
        )
        return
    if job.on_trusted_miner:
        return

    blacklist_time = await aget_config("DYNAMIC_JOB_FAILURE_BLACKLIST_TIME_SECONDS")
    await blacklist_miner(job, MinerBlacklist.BlacklistReason.JOB_FAILED, blacklist_time)


async def blacklist_miner(
    job: OrganicJob, reason: MinerBlacklist.BlacklistReason, blacklist_time: int
) -> None:
    now = timezone.now()
    blacklist_until = now + timedelta(seconds=blacklist_time)
    miner = await Miner.objects.aget(id=job.miner_id)

    msg = (
        f"Blacklisting miner {miner.hotkey} "
        f"until {blacklist_until.isoformat()} "
        f"for failed job {job.job_uuid} "
        f"({job.comment})"
    )

    await MinerBlacklist.objects.acreate(
        miner=miner,
        expires_at=blacklist_until,
        reason=reason,
        reason_details=job.comment,
    )

    await SystemEvent.objects.using(settings.DEFAULT_DB_ALIAS).acreate(
        type=SystemEvent.EventType.MINER_ORGANIC_JOB_INFO,
        subtype=SystemEvent.EventSubType.MINER_BLACKLISTED,
        long_description=msg,
        data={
            "job_uuid": str(job.job_uuid),
            "miner_hotkey": miner.hotkey,
            "reason": reason,
            "start_ts": now.isoformat(),
            "end_ts": blacklist_until.isoformat(),
        },
    )
