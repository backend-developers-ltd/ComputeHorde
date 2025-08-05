import logging
from datetime import timedelta

from django.conf import settings
from django.utils import timezone

from compute_horde_validator.validator.dynamic_config import aget_config
from compute_horde_validator.validator.models import (
    Miner,
    MinerBlacklist,
    OrganicJob,
    SystemEvent,
)

logger = logging.getLogger(__name__)


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
