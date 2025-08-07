from celery.utils.log import get_task_logger
from django.conf import settings
from django.db import transaction

from compute_horde_validator.celery import app
from compute_horde_validator.validator.locks import Lock, Locked, LockType
from compute_horde_validator.validator.models import SystemEvent, AllowanceMinerManifest
from compute_horde_validator.validator.allowance.utils import blocks, manifests

logger = get_task_logger(__name__)


@app.task(
    time_limit=blocks.MAX_RUN_TIME + 30,
)
def scan_blocks_and_calculate_allowance():
    # TODO: write tests and add to celery beat config
    if not AllowanceMinerManifest.objects.exists():
        logger.warning("No miner manifests found, skipping allowance calculation")
        return
    with transaction.atomic(using=settings.DEFAULT_DB_ALIAS):
        try:
            with Lock(LockType.ALLOWANCE_FETCHING, 5.0, settings.DEFAULT_DB_ALIAS):
                blocks.scan_blocks_and_calculate_allowance(report_allowance_to_system_events.delay)
        except Locked:
            logger.debug("Another thread already fetching blocks")
        except blocks.TimesUpError:
            scan_blocks_and_calculate_allowance.delay()


@app.task()
def report_allowance_to_system_events(block_number_lt: int, block_number_gte: int):
    return
    blocks.report_checkpoint(block_number_lt, block_number_gte)


@app.task()
def sync_manifests():
    # TODO: write tests and add to celery beat config
    try:
        manifests.sync_manifests()
    except Exception as e:
        msg = f"Failed to sync manifests: {e}"
        logger.error(msg, exc_info=True)
        SystemEvent.objects.create(
            type=SystemEvent.EventType.COMPUTE_TIME_ALLOWANCE,
            subtype=SystemEvent.EventSubType.FAILURE,
            data={
                "error": str(e),
            },
        )


# clean up old allowance blocks
# clean up old reservations - report system events if there are any expired ones (that were not undone)
