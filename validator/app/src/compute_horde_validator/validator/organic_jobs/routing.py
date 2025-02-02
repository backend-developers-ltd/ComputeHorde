import random
from datetime import timedelta
from typing import assert_never

from compute_horde.fv_protocol.facilitator_requests import (
    JobRequest,
    V0JobRequest,
    V1JobRequest,
    V2JobRequest,
)
from compute_horde.receipts.models import JobStartedReceipt
from django.db.models import Count
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
    now = timezone.now()

    manifests_qs = MinerManifest.objects.select_related("miner").filter(
        executor_class=str(executor_class),
        online_executor_count__gt=0,
        created_at__gte=timezone.now() - timedelta(hours=4),
    )
    manifests = [manifest async for manifest in manifests_qs.all()]
    if not manifests:
        raise NoMinerForExecutorType()

    running_miner_jobs_counts_qs = (
        JobStartedReceipt.objects.valid_at(now)
        .filter(executor_class=executor_class)
        .values("miner_hotkey")
        .annotate(count=Count("*"))
        .values_list("miner_hotkey", "count")
    )
    running_miner_jobs_counts: dict[str, int] = {
        hotkey: count async for hotkey, count in running_miner_jobs_counts_qs
    }
    manifests = [
        manifest
        for manifest in manifests
        if manifest.online_executor_count > running_miner_jobs_counts.get(manifest.miner.hotkey, 0)
    ]
    if not manifests:
        raise AllMinersBusy()

    selected = random.choice(manifests)
    return selected.miner


async def pick_miner_for_job_v0_v1(request: V0JobRequest | V1JobRequest) -> Miner:
    """
    V0 and V1 specify the miner in the job request itself - so just return that.
    """
    miner, _ = await Miner.objects.aget_or_create(hotkey=request.miner_hotkey)
    return miner
