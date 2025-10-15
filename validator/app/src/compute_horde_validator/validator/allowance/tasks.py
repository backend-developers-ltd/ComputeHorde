import contextlib
import time
from collections.abc import Callable
from typing import Any

from celery.utils.log import get_task_logger
from django.conf import settings
from django.db import transaction
from django.db.models import Q, Subquery

from compute_horde_validator.celery import app
from compute_horde_validator.validator.allowance.utils import blocks, manifests
from compute_horde_validator.validator.allowance.utils.supertensor import (
    ArchiveSubtensorNotConfigured,
    PrecachingSuperTensor,
    SuperTensor,
    SuperTensorError,
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


MAX_RUN_TIME = 90


def _run_block_scan_with_lock(
    *,
    lock_type: LockType,
    max_run_time: float,
    backfilling_supertensor: SuperTensor | None,
    scan: Callable[[int, SuperTensor, float, float], None],
    task_name: str,
    keep_running: bool,
    reschedule_func: Callable[..., Any],
) -> None:
    """
    Wrapper for block scanning tasks.

    Args:
        lock_type: Type of lock to acquire
        max_run_time: Maximum runtime for the scan
        backfilling_supertensor: Optional pre-configured SuperTensor instance
        scan: Function to call with (current_block, supertensor, start_time, max_run_time) to perform the actual scanning
        task_name: Name of the task for logging
        keep_running: Whether to reschedule on timeout
        reschedule_func: Function to call to reschedule the task
    """
    if not AllowanceMinerManifest.objects.exists():
        logger.warning(f"{task_name.capitalize()} skipped: no miner manifests available")
        return
    current_block = supertensor().get_current_block()
    with transaction.atomic(using=settings.DEFAULT_DB_ALIAS):
        try:
            with Lock(lock_type, LOCK_WAIT_TIMEOUT, settings.DEFAULT_DB_ALIAS):
                start_time = time.time()

                cm: contextlib.AbstractContextManager[SuperTensor]
                if backfilling_supertensor is None:
                    cm = PrecachingSuperTensor(cache=DjangoCache(), enable_workers=True)
                else:
                    cm = contextlib.nullcontext(backfilling_supertensor)

                with cm as st:
                    scan(current_block, st, start_time, max_run_time)

        except Locked:
            logger.debug(f"Another thread already running {task_name}")
        except blocks.TimesUpError:
            if keep_running:
                logger.info(f"{task_name} timed out, rescheduling")
                reschedule_func()


@app.task(
    time_limit=MAX_RUN_TIME + 60,
)
def scan_blocks_and_calculate_allowance(
    backfilling_supertensor: SuperTensor | None = None,
    keep_running: bool = True,
):
    def _scan_live_blocks(
        current_block: int, st: SuperTensor, start_time: float, max_run_time: float
    ) -> None:
        blocks.backfill_blocks_if_necessary(
            current_block,
            max_run_time,
            report_allowance_checkpoint.delay,
            st,
        )
        time_left = max_run_time - (time.time() - start_time)
        if time_left < 0:
            raise blocks.TimesUpError

        blocks.livefill_blocks(
            current_block,
            time_left,
            report_allowance_checkpoint.delay,
        )

    _run_block_scan_with_lock(
        lock_type=LockType.ALLOWANCE_FETCHING,
        max_run_time=MAX_RUN_TIME,
        backfilling_supertensor=backfilling_supertensor,
        scan=_scan_live_blocks,
        task_name="live block scan",
        keep_running=keep_running,
        reschedule_func=scan_blocks_and_calculate_allowance.delay,
    )


@app.task()
def report_allowance_checkpoint(block_number_lt: int, block_number_gte: int):
    """
    Purely for monitoring purposes. No business logic here.
    """
    blocks.report_checkpoint(block_number_lt, block_number_gte)


@app.task()
def sync_manifests():
    try:
        manifests.sync_manifests()
    except Exception as e:
        msg = f"Failed to sync manifests: {e}"
        if isinstance(e, SuperTensorError):
            logger.info(msg)
        else:
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
    with transaction.atomic():
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


@app.task(
    time_limit=allowance_settings.ARCHIVE_SCAN_MAX_RUN_TIME + 60,
)
def scan_archive_blocks_and_calculate_allowance(
    backfilling_supertensor: SuperTensor | None = None,
    keep_running: bool = True,
):
    """
    Scan and calculate allowances for historical blocks in the archive range.

    Args:
        backfilling_supertensor: Optional pre-configured SuperTensor instance
        keep_running: Whether to reschedule itself if it times out or has more work
    """

    def _scan_archive_blocks(
        current_block: int, st: SuperTensor, start_time: float, max_run_time: float
    ) -> None:
        blocks_processed = 0

        start_block = current_block - allowance_settings.ARCHIVE_MAX_LOOKBACK
        end_block = current_block - allowance_settings.ARCHIVE_START_OFFSET

        oldest = st.oldest_reachable_block()
        if oldest is None or oldest == float("-inf"):
            oldest = start_block
        start_block = max(start_block, int(oldest))

        if start_block > end_block:
            logger.warning("Archive range is empty, nothing to process")
            return

        existing_blocks = set(
            Block.objects.filter(
                block_number__gte=start_block,
                block_number__lte=end_block,
            ).values_list("block_number", flat=True)
        )

        missing_blocks = [
            block_number
            for block_number in range(end_block, start_block - 1, -1)
            if block_number not in existing_blocks
        ]

        if not missing_blocks:
            logger.warning("Archive block sync skipped: archive window already filled")
            return

        logger.warning(
            f"Found {len(missing_blocks)} missing blocks in archive range "
            f"[{start_block}, {end_block}], processing newest to oldest"
        )

        for next_block in missing_blocks:
            if time.time() - start_time >= max_run_time:
                logger.warning(
                    f"Archive scan reached time limit after processing {blocks_processed} blocks, "
                    f"{len(missing_blocks) - blocks_processed} blocks remaining"
                )
                raise blocks.TimesUpError

            try:
                blocks.process_block_allowance_with_reporting(
                    next_block,
                    st,
                    blocks_behind=current_block - next_block,
                    raise_on_error=True,
                )
                blocks_processed += 1

                if blocks_processed % allowance_settings.ARCHIVE_SCAN_BATCH_SIZE == 0:
                    report_allowance_checkpoint.delay(
                        next_block + allowance_settings.ARCHIVE_SCAN_BATCH_SIZE,
                        next_block,
                    )

            except ArchiveSubtensorNotConfigured:
                logger.warning("Archive subtensor not configured, stopping archive scan")
                return
            except Exception as e:
                logger.warning(
                    f"Unexpected error processing archive block {next_block}: {e}, will retry in next run",
                    exc_info=True,
                )
                SystemEvent.objects.create(
                    type=SystemEvent.EventType.COMPUTE_TIME_ALLOWANCE,
                    subtype=SystemEvent.EventSubType.FAILURE,
                    data={
                        "block_number": next_block,
                        "error": str(e),
                        "context": "archive_scan",
                    },
                )
                logger.warning(
                    f"Stopping archive scan to avoid gaps. Processed {blocks_processed} blocks. "
                    f"Will retry block {next_block} in next run."
                )
                raise blocks.TimesUpError

        logger.warning(f"Archive scan complete - processed all {blocks_processed} missing blocks")

    _run_block_scan_with_lock(
        lock_type=LockType.ALLOWANCE_ARCHIVE_FETCHING,
        max_run_time=allowance_settings.ARCHIVE_SCAN_MAX_RUN_TIME,
        backfilling_supertensor=backfilling_supertensor,
        scan=_scan_archive_blocks,
        task_name="archive block scan",
        keep_running=keep_running,
        reschedule_func=scan_archive_blocks_and_calculate_allowance.delay,
    )
