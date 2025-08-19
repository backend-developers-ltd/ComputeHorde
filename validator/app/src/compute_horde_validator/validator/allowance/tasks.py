from celery.utils.log import get_task_logger
from django.conf import settings
from django.db import transaction
from django.db.models import Q, Subquery

from compute_horde_validator.celery import app
from compute_horde_validator.validator.allowance.utils import blocks, manifests
from compute_horde_validator.validator.allowance.utils.supertensor import (
    PrecachingSuperTensor,
    SuperTensor,
    supertensor,
)
from compute_horde_validator.validator.allowance.utils.supertensor_django_cache import DjangoCache
from compute_horde_validator.validator.locks import Lock, Locked, LockType, get_advisory_lock
from compute_horde_validator.validator.models import (
    AllowanceBooking,
    AllowanceMinerManifest,
    Block,
    BlockAllowance,
    SystemEvent,
)

from . import settings as allowance_settings

logger = get_task_logger(__name__)


LOCK_WAIT_TIMEOUT = 5.0


@app.task(
    time_limit=blocks.MAX_RUN_TIME + 30,
)
def scan_blocks_and_calculate_allowance(
    backfilling_supertensor: SuperTensor | None = None,
    keep_running: bool = True,
):
    if not AllowanceMinerManifest.objects.exists():
        logger.warning("No miner manifests found, skipping allowance calculation")
        return
    with transaction.atomic(using=settings.DEFAULT_DB_ALIAS):
        try:
            with Lock(LockType.ALLOWANCE_FETCHING, LOCK_WAIT_TIMEOUT, settings.DEFAULT_DB_ALIAS):
                blocks.scan_blocks_and_calculate_allowance(
                    report_allowance_checkpoint.delay,
                    backfilling_supertensor or PrecachingSuperTensor(cache=DjangoCache()),
                    supertensor(),
                )
        except Locked:
            logger.debug("Another thread already fetching blocks")
        except blocks.TimesUpError:
            if keep_running:
                scan_blocks_and_calculate_allowance.delay()


@app.task()
def report_allowance_checkpoint(block_number_lt: int, block_number_gte: int):
    """
    Purely for monitoring purposes. No business logic here.
    """
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


@app.task()
def evict_old_data():
    with transaction.atomic(using=settings.DEFAULT_DB_ALIAS):
        try:
            get_advisory_lock(LockType.ALLOWANCE_EVICTING)
        except Locked:
            logger.debug("Another thread already evicting")
            return
        current_block = supertensor().get_current_block()
        block_number = current_block - allowance_settings.BLOCK_EVICTION_THRESHOLD
        logger.info(
            f"Evicting data older than {block_number=} ({current_block}, "
            f"{allowance_settings.BLOCK_EVICTION_THRESHOLD=})"
        )

        removed, _ = BlockAllowance.objects.filter(block_id__lte=block_number).delete()
        logger.info(f"Removed {removed} BlockAllowances")
        removed, _ = AllowanceMinerManifest.objects.filter(block_number__lte=block_number).delete()
        logger.info(f"Removed {removed} AllowanceMinerManifests")
        removed, _ = Block.objects.filter(block_number__lte=block_number).delete()
        logger.info(f"Removed {removed} Blocks")
        removed, _ = AllowanceBooking.objects.filter(
            ~Q(
                id__in=Subquery(
                    BlockAllowance.objects.filter(allowance_booking__isnull=False).values(
                        "allowance_booking_id"
                    )
                )
            )
        ).delete()

        logger.info(f"Removed {removed} AllowanceBookings")
