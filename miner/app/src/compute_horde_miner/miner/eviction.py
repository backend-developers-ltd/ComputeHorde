import logging
from datetime import timedelta

from compute_horde.receipts.models import JobAcceptedReceipt, JobFinishedReceipt, JobStartedReceipt
from compute_horde.receipts.store.local import LocalFilesystemPagedReceiptStore
from django.utils.timezone import now

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
    store = LocalFilesystemPagedReceiptStore()
    store.delete_pages_older_than(cutoff)
