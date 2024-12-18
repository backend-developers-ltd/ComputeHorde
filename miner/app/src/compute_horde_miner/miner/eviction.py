import logging
from datetime import timedelta

from compute_horde.receipts.models import JobAcceptedReceipt, JobFinishedReceipt, JobStartedReceipt
from django.utils.timezone import now

from compute_horde_miner.miner.receipts import current_store

from .models import AcceptedJob

JOBS_RETENTION_PERIOD = timedelta(days=2)
RECEIPTS_RETENTION_PERIOD = timedelta(days=2)

logger = logging.getLogger(__name__)


def evict_all():
    evict_jobs()
    evict_receipts()


def evict_jobs() -> None:
    logger.info("Evicting old accepted jobs")
    cutoff = now() - JOBS_RETENTION_PERIOD
    AcceptedJob.objects.filter(created_at__lt=cutoff).delete()


def evict_receipts() -> None:
    logger.info("Evicting old receipts")
    cutoff = now() - RECEIPTS_RETENTION_PERIOD
    JobStartedReceipt.objects.filter(timestamp__lt=cutoff).delete()
    JobAcceptedReceipt.objects.filter(timestamp__lt=cutoff).delete()
    JobFinishedReceipt.objects.filter(timestamp__lt=cutoff).delete()

    logger.info("Evicting old receipt pages")
    current_store().evict(cutoff=cutoff)
