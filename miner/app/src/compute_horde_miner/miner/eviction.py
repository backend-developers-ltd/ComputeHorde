import logging
from datetime import timedelta

from compute_horde.receipts.models import JobAcceptedReceipt, JobFinishedReceipt, JobStartedReceipt
from django.utils.timezone import now

from compute_horde_miner.miner.receipts import current_store

from .models import AcceptedJob

JOBS_RETENTION_PERIOD = timedelta(days=2)
RECEIPTS_RETENTION_PERIOD = timedelta(days=2)

logger = logging.getLogger(__name__)


async def evict_all():
    await evict_jobs()
    await evict_receipts()


async def evict_jobs() -> None:
    logger.info("Evicting old accepted jobs")
    cutoff = now() - JOBS_RETENTION_PERIOD
    await AcceptedJob.objects.filter(created_at__lt=cutoff).adelete()


async def evict_receipts() -> None:
    logger.info("Evicting old receipts")
    cutoff = now() - RECEIPTS_RETENTION_PERIOD
    await JobStartedReceipt.objects.filter(timestamp__lt=cutoff).adelete()
    await JobAcceptedReceipt.objects.filter(timestamp__lt=cutoff).adelete()
    await JobFinishedReceipt.objects.filter(timestamp__lt=cutoff).adelete()

    logger.info("Evicting old receipt pages")
    (await current_store()).evict(cutoff=cutoff)
