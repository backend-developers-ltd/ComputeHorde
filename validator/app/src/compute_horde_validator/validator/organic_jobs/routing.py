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
from django.utils import timezone

from compute_horde_validator.validator.models import Miner, MinerManifest


class JobRoutingException(Exception):
    pass


class NoMinerForExecutorType(JobRoutingException):
    pass


class AllMinersBusy(JobRoutingException):
    pass


async def pick_miner_for_job(request: JobRequest) -> Miner:
    if isinstance(request, V0JobRequest | V1JobRequest):
        return await pick_miner_for_job_v0_v1(request)

    if isinstance(request, V2JobRequest):
        return await pick_miner_for_job_v2(request)

    assert_never(request)


async def pick_miner_for_job_v2(request: V2JobRequest) -> Miner:
    """
    Goes through all miners with recent manifests and online executors of the given executor class.
    Returns a random miner that may have a non-busy executor based on known receipts.
    """
    executor_class = request.executor_class

    manifests_qs = MinerManifest.objects.select_related("miner").filter(
        executor_class=str(executor_class),
        online_executor_count__gt=0,
        created_at__gte=timezone.now() - timedelta(hours=4),
    )
    manifests = [manifest async for manifest in manifests_qs.all()]
    if not manifests:
        raise NoMinerForExecutorType()

    random.shuffle(manifests)

    for manifest in manifests:
        miner = manifest.miner

        known_started_jobs: set[str] = {
            job_uuid async for job_uuid in JobStartedReceipt.objects
            .valid_at(timezone.now())
            .filter(miner_hotkey=miner.hotkey)
            .values_list("job_uuid", flat=True)
        }

        known_finished_jobs: set[str] = {
            job_uuid async for job_uuid in JobFinishedReceipt.objects
            .filter(
                job_uuid__in=known_started_jobs,
                miner_hotkey=miner.hotkey,
            )
            .values_list("job_uuid", flat=True)
        }

        maybe_ongoing_jobs = known_started_jobs - known_finished_jobs

        if len(maybe_ongoing_jobs) < manifest.online_executor_count:
            return miner

    raise AllMinersBusy()


async def pick_miner_for_job_v0_v1(request: V0JobRequest | V1JobRequest) -> Miner:
    """
    V0 and V1 specify the miner in the job request itself - so just return that.
    """
    miner, _ = await Miner.objects.aget_or_create(hotkey=request.miner_hotkey)
    return miner
