import asyncio
import datetime
import logging
import time
from collections import defaultdict
from collections.abc import Awaitable, Callable, Sequence
from dataclasses import dataclass

import aiohttp
from compute_horde.receipts.models import (
    JobFinishedReceipt,
    JobStartedReceipt,
)
from compute_horde.receipts.schemas import JobFinishedReceiptPayload, JobStartedReceiptPayload
from compute_horde.receipts.store.local import N_ACTIVE_PAGES, LocalFilesystemPagedReceiptStore
from compute_horde.receipts.transfer import MinerInfo, ReceiptsTransfer, TransferResult
from compute_horde.utils import sign_blob
from compute_horde_core.executor_class import ExecutorClass
from django.conf import settings
from django.db.models import Count, Exists, OuterRef, Subquery
from django.utils import timezone
from prometheus_client import Counter, Gauge, Histogram
from typing_extensions import deprecated

from compute_horde_validator.validator.allowance.utils.supertensor import supertensor
from compute_horde_validator.validator.dynamic_config import aget_config
from compute_horde_validator.validator.models import MetagraphSnapshot, Miner
from compute_horde_validator.validator.models.allowance.internal import Block

from .base import ReceiptsBase
from .types import JobSpendingInfo, TransferIsDisabled

logger = logging.getLogger(__name__)


@dataclass
class _Metrics:
    receipts: Counter
    miners: Gauge
    successful_transfers: Counter
    line_errors: Counter
    transfer_errors: Counter
    transfer_duration: Histogram
    catchup_pages_left: Gauge


class Receipts(ReceiptsBase):
    """
    Default implementation of receipts manager.
    """

    def __init__(self):
        self.metrics = _Metrics(
            receipts=Counter(
                "receipttransfer_receipts_total", documentation="Number of transferred receipts"
            ),
            miners=Gauge(
                "receipttransfer_miners", documentation="Number of miners to transfer from"
            ),
            successful_transfers=Counter(
                "receipttransfer_successful_transfers_total",
                documentation="Number of transfers that didn't explicitly fail. (this includes 404s though)",
            ),
            line_errors=Counter(
                "receipttransfer_line_errors_total",
                labelnames=["exc_type"],
                documentation="Number of invalid lines in received pages",
            ),
            transfer_errors=Counter(
                "receipttransfer_transfer_errors_total",
                labelnames=["exc_type"],
                documentation="Number of completely failed page transfers",
            ),
            transfer_duration=Histogram(
                "receipttransfer_transfer_duration",
                documentation="Total time to transfer latest page deltas from all miners",
            ),
            catchup_pages_left=Gauge(
                "receipttransfer_catchup_pages_left",
                documentation="Pages waiting for catch-up",
            ),
        )

    async def run_receipts_transfer(
        self,
        daemon: bool,
        debug_miner_hotkey: str | None,
        debug_miner_ip: str | None,
        debug_miner_port: int | None,
    ) -> None:
        if (debug_miner_hotkey, debug_miner_ip, debug_miner_port) != (None, None, None):
            # 1st, use explicitly specified miner if available
            if None in {debug_miner_hotkey, debug_miner_ip, debug_miner_port}:
                raise ValueError("Either none or all of explicit miner details must be provided")
            miner = [debug_miner_hotkey, debug_miner_ip, debug_miner_port]
            logger.info(f"Will fetch receipts from explicit miner: {miner}")

            async def miners():
                return [miner]

        elif settings.DEBUG_FETCH_RECEIPTS_FROM_MINERS:
            # 2nd, if debug miners are specified, they take precedence.
            debug_miners = settings.DEBUG_FETCH_RECEIPTS_FROM_MINERS
            logger.info(f"Will fetch receipts from {len(debug_miners)} debug miners")

            async def miners():
                return debug_miners

        else:
            # 3rd, if no specific miners were specified, get from metagraph snapshot.
            logger.info("Will fetch receipts from metagraph snapshot miners")

            async def miners():
                snapshot = await MetagraphSnapshot.aget_latest()
                serving_hotkeys = snapshot.serving_hotkeys
                serving_miners = [m async for m in Miner.objects.filter(hotkey__in=serving_hotkeys)]
                return [(m.hotkey, m.address, m.port) for m in serving_miners]

        # IMPORTANT: This encompasses at least the current and the previous cycle.
        cutoff = timezone.now() - datetime.timedelta(hours=5)

        """
        General considerations:
        - higher concurrency:
            - higher bandwidth use
            - more parallel CPU-heavy signature check tasks -> steal CPU time from asyncio thread (GIL) 
        - lower concurrency:
            - slows down the process due to higher influence of network latency 
        - higher allowed request timeout:
            - one slow miner may stall the whole process for longer
            - less timeouts due to CPU time being stolen by CPU heavy tasks
        """

        if daemon:
            while True:
                try:
                    await self._run_in_loop(cutoff, miners)
                except TransferIsDisabled:
                    # Sleep instead of exiting in case the transfer gets dynamically re-enabled.
                    logger.info("Transfer is currently disabled. Sleeping for a minute.")
                    await asyncio.sleep(60)
        else:
            await self._run_once(cutoff, miners)

    def create_job_finished_receipt(
        self,
        job_uuid: str,
        miner_hotkey: str,
        validator_hotkey: str,
        time_started: datetime.datetime,
        time_took_us: int,
        score_str: str,
        block_numbers: list[int] | None = None,
    ) -> JobFinishedReceipt:
        if block_numbers is None:
            block_numbers = []
        payload = JobFinishedReceiptPayload(
            job_uuid=job_uuid,
            miner_hotkey=miner_hotkey,
            validator_hotkey=validator_hotkey,
            timestamp=datetime.datetime.now(datetime.UTC),
            time_started=time_started,
            time_took_us=time_took_us,
            score_str=score_str,
            block_numbers=block_numbers,
        )

        validator_kp = settings.BITTENSOR_WALLET().get_hotkey()
        validator_signature = sign_blob(validator_kp, payload.blob_for_signing())

        return JobFinishedReceipt(
            job_uuid=job_uuid,
            miner_hotkey=miner_hotkey,
            validator_hotkey=validator_hotkey,
            validator_signature=validator_signature,
            timestamp=payload.timestamp,
            time_started=time_started,
            time_took_us=time_took_us,
            score_str=score_str,
            block_numbers=block_numbers,
        )

    def create_job_started_receipt(
        self,
        job_uuid: str,
        miner_hotkey: str,
        validator_hotkey: str,
        executor_class: ExecutorClass,
        is_organic: bool,
        ttl: int,
    ) -> tuple[JobStartedReceiptPayload, str]:
        payload = JobStartedReceiptPayload(
            job_uuid=job_uuid,
            miner_hotkey=miner_hotkey,
            validator_hotkey=validator_hotkey,
            timestamp=datetime.datetime.now(datetime.UTC),
            executor_class=str(executor_class),
            is_organic=is_organic,
            ttl=ttl,
        )

        validator_kp = settings.BITTENSOR_WALLET().get_hotkey()
        validator_signature = sign_blob(validator_kp, payload.blob_for_signing())

        return payload, validator_signature

    @deprecated("Use database queries instead")
    async def get_job_started_receipt_by_uuid(self, job_uuid: str) -> JobStartedReceipt:
        return await JobStartedReceipt.objects.aget(job_uuid=job_uuid)

    def get_finished_jobs_for_block_range(
        self, start_block: int, end_block: int, executor_class: ExecutorClass
    ) -> list[JobSpendingInfo]:
        """
        Returns the alleged job spendings as reported by miners via receipt transfer.
        These spendings may be bogus and require validation.
        """
        if start_block >= end_block:
            logger.warning(
                "Invalid block range provided: start_block (%s) >= end_block (%s)",
                start_block,
                end_block,
            )
            return []

        start_timestamp = self._get_block_timestamp(start_block)
        end_timestamp = self._get_block_timestamp(end_block)

        finished_qs = JobFinishedReceipt.objects.filter(
            timestamp__gte=start_timestamp,
            timestamp__lt=end_timestamp,
        )

        # Filter for executor class - which is present on the job started receipt
        starts = JobStartedReceipt.objects.filter(
            job_uuid=OuterRef("job_uuid"), executor_class=str(executor_class)
        )
        finished_qs = finished_qs.filter(Exists(starts))

        # TODO(new scoring): this could be inefficient
        finished_qs = finished_qs.annotate(started_at=Subquery(starts.values("timestamp")[:1]))

        return [
            JobSpendingInfo(
                job_uuid=str(r.job_uuid),
                validator_hotkey=r.validator_hotkey,
                miner_hotkey=r.miner_hotkey,
                executor_class=executor_class,
                executor_seconds_cost=r.score(),
                paid_with_blocks=r.block_numbers,
                started_at=r.started_at,
            )
            for r in finished_qs
        ]

    async def get_busy_executor_count(
        self, executor_class: ExecutorClass, at_time: datetime.datetime
    ) -> dict[str, int]:
        starts_qs = JobStartedReceipt.objects.valid_at(at_time).filter(
            executor_class=str(executor_class)
        )

        finishes = JobFinishedReceipt.objects.filter(
            job_uuid=OuterRef("job_uuid"), timestamp__lte=at_time
        )

        ongoing = starts_qs.annotate(has_finished=Exists(finishes)).filter(has_finished=False)

        rows = [
            row
            async for row in ongoing.values("miner_hotkey")
            .annotate(n=Count("id"))
            .values("miner_hotkey", "n")
        ]
        return {row["miner_hotkey"]: int(row["n"]) for row in rows}

    async def _catch_up(
        self,
        pages: Sequence[int],
        miners: Callable[[], Awaitable[list[MinerInfo]]],
        session: aiohttp.ClientSession,
        semaphore: asyncio.Semaphore,
    ) -> None:
        """
        Fetches new receipts on given pages one by one.
        """
        for idx, page in enumerate(pages):
            await self._throw_if_disabled()

            self.metrics.catchup_pages_left.set(len(pages) - idx)
            start_time = time.monotonic()
            current_loop_miners = await miners()
            result = await ReceiptsTransfer.transfer(
                miners=current_loop_miners,
                pages=[page],
                session=session,
                semaphore=semaphore,
                # We may need to download a lot of full pages, so the timeout is higher.
                request_timeout=3.0,
            )
            elapsed = time.monotonic() - start_time

            logger.info(
                f"Catching up: "
                f"{page=} ({idx + 1}/{len(pages)}) "
                f"receipts={result.n_receipts} "
                f"{elapsed=:.3f} "
                f"successful_transfers={result.n_successful_transfers} "
                f"transfer_errors={len(result.transfer_errors)} "
                f"line_errors={len(result.line_errors)} "
            )

            for line_error in result.line_errors:
                logger.debug(" - line error: %s", line_error)

            self._push_common_metrics(result)
        self.metrics.catchup_pages_left.set(0)

    async def _keep_up(
        self,
        miners: Callable[[], Awaitable[list[MinerInfo]]],
        session: aiohttp.ClientSession,
        semaphore: asyncio.Semaphore,
    ) -> None:
        """
        Runs indefinitely and polls for changes in active pages every `interval`.
        """
        while True:
            await self._throw_if_disabled()
            interval: int = await aget_config("DYNAMIC_RECEIPT_TRANSFER_INTERVAL")

            start_time = time.monotonic()
            current_page = LocalFilesystemPagedReceiptStore.current_page()
            pages = list(reversed(range(current_page - N_ACTIVE_PAGES + 1, current_page + 1)))
            current_loop_miners = await miners()
            result = await ReceiptsTransfer.transfer(
                miners=current_loop_miners,
                pages=pages,
                session=session,
                semaphore=semaphore,
                request_timeout=1.0,
            )
            elapsed = time.monotonic() - start_time

            logger.info(
                f"Keeping up: "
                f"{pages=} "
                f"receipts={result.n_receipts} "
                f"{elapsed=:.3f} "
                f"successful_transfers={result.n_successful_transfers} "
                f"transfer_errors={len(result.transfer_errors)} "
                f"line_errors={len(result.line_errors)} "
            )

            self._push_common_metrics(result)
            self.metrics.miners.set(len(current_loop_miners))
            self.metrics.transfer_duration.observe(elapsed)

            # Sleep for the remainder of the time if any
            if elapsed < interval:
                time.sleep(interval - elapsed)

    async def _run_once(
        self,
        cutoff_ts: datetime.datetime,
        miners: Callable[[], Awaitable[list[MinerInfo]]],
    ) -> None:
        catchup_cutoff_page = LocalFilesystemPagedReceiptStore.current_page_at(cutoff_ts)
        current_page = LocalFilesystemPagedReceiptStore.current_page()
        async with aiohttp.ClientSession() as session:
            await self._catch_up(
                pages=list(reversed(range(catchup_cutoff_page, current_page + 1))),
                miners=miners,
                session=session,
                semaphore=asyncio.Semaphore(50),
            )

    async def _run_in_loop(
        self,
        cutoff_ts: datetime.datetime,
        miners: Callable[[], Awaitable[list[MinerInfo]]],
    ) -> None:
        catchup_cutoff_page = LocalFilesystemPagedReceiptStore.current_page_at(cutoff_ts)
        current_page = LocalFilesystemPagedReceiptStore.current_page()
        async with aiohttp.ClientSession() as session:
            await self._catch_up(
                pages=list(reversed(range(current_page - N_ACTIVE_PAGES + 1, current_page + 1))),
                miners=miners,
                session=session,
                semaphore=asyncio.Semaphore(50),
            )
            await asyncio.gather(
                self._catch_up(
                    pages=list(
                        reversed(range(catchup_cutoff_page, current_page - N_ACTIVE_PAGES + 1))
                    ),
                    miners=miners,
                    session=session,
                    semaphore=asyncio.Semaphore(10),
                ),
                self._keep_up(
                    miners=miners,
                    session=session,
                    semaphore=asyncio.Semaphore(50),
                ),
            )

    def _push_common_metrics(self, result: TransferResult) -> None:
        # Push line error counts grouped by the exception type
        n_line_errors: defaultdict[type[Exception], int] = defaultdict(int)
        for line_error in result.line_errors:
            n_line_errors[type(line_error)] += 1
        for exc_type, exc_count in n_line_errors.items():
            self.metrics.line_errors.labels(exc_type=exc_type.__name__).inc(exc_count)

        # Push transfer error counts grouped by the exception type
        n_transfer_errors: defaultdict[type[Exception], int] = defaultdict(int)
        for transfer_error in result.transfer_errors:
            n_transfer_errors[type(transfer_error)] += 1
        for exc_type, exc_count in n_transfer_errors.items():
            self.metrics.transfer_errors.labels(exc_type=exc_type.__name__).inc(exc_count)

        self.metrics.receipts.inc(result.n_receipts)
        self.metrics.successful_transfers.inc(result.n_successful_transfers)

    async def _throw_if_disabled(self) -> None:
        try:
            if await aget_config("DYNAMIC_RECEIPT_TRANSFER_ENABLED"):
                return
        except KeyError:
            logger.warning("DYNAMIC_RECEIPT_TRANSFER_ENABLED dynamic config is not set up!")
        raise TransferIsDisabled

    def _get_block_timestamp(self, block_number: int) -> datetime.datetime:
        try:
            block = Block.objects.get(block_number=block_number)
            return block.creation_timestamp
        except Block.DoesNotExist:
            return supertensor().get_block_timestamp(block_number)


_receipts_instance: Receipts | None = None


def receipts() -> Receipts:
    global _receipts_instance
    if _receipts_instance is None:
        _receipts_instance = Receipts()
    return _receipts_instance
