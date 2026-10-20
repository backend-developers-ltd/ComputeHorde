import logging
from datetime import timedelta

from compute_horde.receipts.models import JobAcceptedReceipt, JobFinishedReceipt, JobStartedReceipt
from django.conf import settings
from django.db.models import Q
from django.utils.timezone import now

from .models import (
    MinerBlacklist,
    MinerPreliminaryReservation,
    OrganicJob,
    SystemEvent,
)

RECEIPTS_RETENTION_PERIOD = timedelta(days=2)
ORGANIC_JOBS_RETENTION_PERIOD = timedelta(days=2)
SYSTEM_EVENTS_RETENTION_PERIOD = timedelta(days=2)
MINER_BLACKLIST_RETENTION_PERIOD = timedelta(days=2)
MINER_PRELIMINARY_RESERVATION_RETENTION_PERIOD = timedelta(days=2)
COMPUTE_TIME_ALLOWANCE_RETENTION_PERIOD = timedelta(days=2)

logger = logging.getLogger(__name__)


def evict_all() -> None:
    evict_organic_jobs()
    evict_receipts()
    evict_malformed_receipts()
    evict_system_events()
    evict_miner_blacklist()
    evict_miner_preliminary_reservations()


def evict_system_events() -> None:
    logger.info("Evicting old system events")
    cutoff = now() - SYSTEM_EVENTS_RETENTION_PERIOD
    SystemEvent.objects.using(settings.DEFAULT_DB_ALIAS).filter(timestamp__lt=cutoff).delete()


def evict_organic_jobs() -> None:
    logger.info("Evicting old organic jobs")
    cutoff = now() - ORGANIC_JOBS_RETENTION_PERIOD
    OrganicJob.objects.filter(updated_at__lt=cutoff).delete()


def evict_receipts() -> None:
    logger.info("Evicting receipts outside the retention window")
    current_time = now()
    # Receipts are dated by the issuing validator; a transferred receipt dated further ahead than
    # the retention period would otherwise outlive it.
    outside_retention_window = Q(timestamp__lt=current_time - RECEIPTS_RETENTION_PERIOD) | Q(
        timestamp__gt=current_time + RECEIPTS_RETENTION_PERIOD
    )
    for receipt_model in (JobStartedReceipt, JobAcceptedReceipt, JobFinishedReceipt):
        receipt_model.objects.filter(outside_retention_window).delete()


def evict_malformed_receipts() -> None:
    """
    Deletes job finished receipts whose score cannot be read.

    Such rows were transferred from miners before receipt fields were validated; scoring skips
    them, so they only take up space and produce warnings.
    """
    logger.info("Evicting job finished receipts with a malformed score")
    JobFinishedReceipt.with_malformed_score().delete()


def evict_miner_blacklist() -> None:
    logger.info("Evicting expired miner blacklists")
    cutoff = now() - MINER_BLACKLIST_RETENTION_PERIOD
    MinerBlacklist.objects.filter(expires_at__lt=cutoff).delete()


def evict_miner_preliminary_reservations():
    logger.info("Evicting old expired miner preliminary reservations")
    cutoff = now() - MINER_PRELIMINARY_RESERVATION_RETENTION_PERIOD
    MinerPreliminaryReservation.objects.filter(expires_at__lt=cutoff).delete()
