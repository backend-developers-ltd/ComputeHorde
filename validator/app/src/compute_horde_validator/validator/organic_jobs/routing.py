import logging
import random
from datetime import timedelta
from typing import assert_never

from compute_horde.fv_protocol.facilitator_requests import (
    JobRequest,
    V0JobRequest,
    V1JobRequest,
    V2JobRequest,
)
from compute_horde.receipts.models import JobFinishedReceipt, JobStartedReceipt
from compute_horde.utils import async_synchronized
from django.conf import settings
from django.utils import timezone

from compute_horde_validator.validator.dynamic_config import aget_config
from compute_horde_validator.validator.models import (
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


async def pick_miner_for_job_request(request: JobRequest) -> Miner:
    if settings.DEBUG_MINER_KEY:
        miner, _ = await Miner.objects.aget_or_create(hotkey=settings.DEBUG_MINER_KEY)
        return miner

    if isinstance(request, V0JobRequest | V1JobRequest):
        return await pick_miner_for_job_v0_v1(request)

    if isinstance(request, V2JobRequest):
        return await pick_miner_for_job_v2(request)

    assert_never(request)


@async_synchronized
async def pick_miner_for_job_v2(request: V2JobRequest) -> Miner:
    """
    Goes through all miners with recent manifests and online executors of the given executor class.
    Returns a random miner that may have a non-busy executor based on known receipts.
    """
    if request.on_trusted_miner:
        miner, _ = await Miner.objects.aget_or_create(hotkey=TRUSTED_MINER_FAKE_KEY)
        return miner

    executor_class = request.executor_class

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
    manifests = list(latest_miner_manifest.values())

    # Discard manifests that explicitly say there are no executors of this type
    manifests = [manifest for manifest in manifests if manifest.online_executor_count > 0]

    if not manifests:
        raise NoMinerForExecutorType()

    random.shuffle(manifests)

    for manifest in manifests:
        miner = manifest.miner

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
            return miner

    raise AllMinersBusy()


async def pick_miner_for_job_v0_v1(request: V0JobRequest | V1JobRequest) -> Miner:
    """
    V0 and V1 requests contain miner selected by facilitator - so just return that.
    """
    if await MinerBlacklist.objects.active().filter(miner__hotkey=request.miner_hotkey).aexists():
        raise MinerIsBlacklisted()

    miner, _ = await Miner.objects.aget_or_create(hotkey=request.miner_hotkey)

    return miner


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
