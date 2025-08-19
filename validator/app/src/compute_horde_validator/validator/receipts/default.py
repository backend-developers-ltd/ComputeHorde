import asyncio
import datetime
import logging
import time
from dataclasses import dataclass

import aiohttp
from asgiref.sync import sync_to_async
from compute_horde.receipts import Receipt
from compute_horde.receipts.models import (
    JobFinishedReceipt,
    JobStartedReceipt,
)
from compute_horde.receipts.schemas import JobFinishedReceiptPayload, JobStartedReceiptPayload
from compute_horde.receipts.store.local import N_ACTIVE_PAGES, LocalFilesystemPagedReceiptStore
from compute_horde.receipts.transfer import ReceiptsTransfer, TransferResult
from compute_horde.utils import sign_blob
from django.conf import settings
from django.utils import timezone
from prometheus_client import Counter, Gauge, Histogram

from compute_horde_validator.validator.allowance.utils.supertensor import supertensor
from compute_horde_validator.validator.dynamic_config import aget_config
from compute_horde_validator.validator.models import MetagraphSnapshot, Miner
from compute_horde_validator.validator.models.allowance.internal import Block
from compute_horde_validator.validator.receipts.base import ReceiptsBase
from compute_horde_validator.validator.receipts.types import (
    ReceiptsGenerationError,
)

logger = logging.getLogger(__name__)


class _TransferIsDisabled(Exception):
    pass


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

    async def run_receipts_transfer(
        self,
        daemon: bool,
        debug_miner_hotkey: str | None,
        debug_miner_ip: str | None,
        debug_miner_port: int | None,
    ) -> None:
        metrics = _Metrics(
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

        mode, explicit_miner = await self._determine_miners_mode(
            debug_miner_hotkey, debug_miner_ip, debug_miner_port
        )
        cutoff = timezone.now() - datetime.timedelta(hours=5)

        if daemon:
            while True:
                try:
                    await self._run_in_loop(cutoff, mode, explicit_miner, metrics)
                except _TransferIsDisabled:
                    logger.info("Transfer is currently disabled. Sleeping for a minute.")
                    await asyncio.sleep(60)
        else:
            await self._run_once(cutoff, mode, explicit_miner, metrics)

    async def _determine_miners_mode(
        self,
        debug_miner_hotkey: str | None,
        debug_miner_ip: str | None,
        debug_miner_port: int | None,
    ) -> tuple[str, tuple[str, str, int] | None]:
        if (debug_miner_hotkey, debug_miner_ip, debug_miner_port) != (None, None, None):
            if None in {debug_miner_hotkey, debug_miner_ip, debug_miner_port}:
                raise ValueError("Either none or all of explicit miner details must be provided")
            # All values are guaranteed non-None here due to the check above.
            assert debug_miner_hotkey is not None
            assert debug_miner_ip is not None
            assert debug_miner_port is not None
            miner: tuple[str, str, int] = (
                debug_miner_hotkey,
                debug_miner_ip,
                int(debug_miner_port),
            )
            logger.info(f"Will fetch receipts from explicit miner: {list(miner)}")
            return "explicit", miner
        if settings.DEBUG_FETCH_RECEIPTS_FROM_MINERS:
            debug_miners = settings.DEBUG_FETCH_RECEIPTS_FROM_MINERS
            logger.info(f"Will fetch receipts from {len(debug_miners)} debug miners")
            return "debug_settings", None
        logger.info("Will fetch receipts from metagraph snapshot miners")
        return "metagraph", None

    async def _list_miners(
        self, mode: str, explicit_miner: tuple[str, str, int] | None
    ) -> list[tuple[str, str, int]]:
        if mode == "explicit":
            assert explicit_miner is not None
            return [explicit_miner]
        if mode == "debug_settings":
            return settings.DEBUG_FETCH_RECEIPTS_FROM_MINERS
        # metagraph mode
        snapshot = await MetagraphSnapshot.aget_latest()
        serving_hotkeys = snapshot.serving_hotkeys
        serving_miners = [m async for m in Miner.objects.filter(hotkey__in=serving_hotkeys)]
        return [(m.hotkey, m.address, m.port) for m in serving_miners]

    async def _throw_if_disabled(self) -> None:
        try:
            if await aget_config("DYNAMIC_RECEIPT_TRANSFER_ENABLED"):
                return
        except KeyError:
            logger.warning("DYNAMIC_RECEIPT_TRANSFER_ENABLED dynamic config is not set up!")
        raise _TransferIsDisabled

    def _push_common_metrics(self, result: TransferResult, metrics: _Metrics) -> None:
        from collections import defaultdict

        n_line_errors: defaultdict[type[Exception], int] = defaultdict(int)
        for line_error in result.line_errors:
            n_line_errors[type(line_error)] += 1
        for exc_type, exc_count in n_line_errors.items():
            metrics.line_errors.labels(exc_type=exc_type.__name__).inc(exc_count)

        n_transfer_errors: defaultdict[type[Exception], int] = defaultdict(int)
        for transfer_error in result.transfer_errors:
            n_transfer_errors[type(transfer_error)] += 1
        for exc_type, exc_count in n_transfer_errors.items():
            metrics.transfer_errors.labels(exc_type=exc_type.__name__).inc(exc_count)

        metrics.receipts.inc(result.n_receipts)
        metrics.successful_transfers.inc(result.n_successful_transfers)

    async def _catch_up(
        self,
        pages: list[int],
        mode: str,
        explicit_miner: tuple[str, str, int] | None,
        session: aiohttp.ClientSession,
        semaphore: asyncio.Semaphore,
        metrics: _Metrics,
    ) -> None:
        for idx, page in enumerate(pages):
            await self._throw_if_disabled()

            metrics.catchup_pages_left.set(len(pages) - idx)
            start_time = time.monotonic()
            current_loop_miners = await self._list_miners(mode, explicit_miner)
            result = await ReceiptsTransfer.transfer(
                miners=current_loop_miners,
                pages=[page],
                session=session,
                semaphore=semaphore,
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

            self._push_common_metrics(result, metrics)
        metrics.catchup_pages_left.set(0)

    async def _keep_up(
        self,
        mode: str,
        explicit_miner: tuple[str, str, int] | None,
        session: aiohttp.ClientSession,
        semaphore: asyncio.Semaphore,
        metrics: _Metrics,
    ) -> None:
        while True:
            await self._throw_if_disabled()
            interval: int = await aget_config("DYNAMIC_RECEIPT_TRANSFER_INTERVAL")

            start_time = time.monotonic()
            current_page = LocalFilesystemPagedReceiptStore.current_page()
            pages = list(reversed(range(current_page - N_ACTIVE_PAGES + 1, current_page + 1)))
            current_loop_miners = await self._list_miners(mode, explicit_miner)
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

            self._push_common_metrics(result, metrics)
            metrics.miners.set(len(current_loop_miners))
            metrics.transfer_duration.observe(elapsed)

            if elapsed < interval:
                time.sleep(interval - elapsed)

    async def _run_once(
        self,
        cutoff_ts: datetime.datetime,
        mode: str,
        explicit_miner: tuple[str, str, int] | None,
        metrics: _Metrics,
    ) -> None:
        catchup_cutoff_page = LocalFilesystemPagedReceiptStore.current_page_at(cutoff_ts)
        current_page = LocalFilesystemPagedReceiptStore.current_page()
        async with aiohttp.ClientSession() as session:
            await self._catch_up(
                pages=list(reversed(range(catchup_cutoff_page, current_page + 1))),
                mode=mode,
                explicit_miner=explicit_miner,
                session=session,
                semaphore=asyncio.Semaphore(50),
                metrics=metrics,
            )

    async def _run_in_loop(
        self,
        cutoff_ts: datetime.datetime,
        mode: str,
        explicit_miner: tuple[str, str, int] | None,
        metrics: _Metrics,
    ) -> None:
        catchup_cutoff_page = LocalFilesystemPagedReceiptStore.current_page_at(cutoff_ts)
        current_page = LocalFilesystemPagedReceiptStore.current_page()
        async with aiohttp.ClientSession() as session:
            await self._catch_up(
                pages=list(reversed(range(current_page - N_ACTIVE_PAGES + 1, current_page + 1))),
                mode=mode,
                explicit_miner=explicit_miner,
                session=session,
                semaphore=asyncio.Semaphore(50),
                metrics=metrics,
            )
            await asyncio.gather(
                self._catch_up(
                    pages=list(
                        reversed(range(catchup_cutoff_page, current_page - N_ACTIVE_PAGES + 1))
                    ),
                    mode=mode,
                    explicit_miner=explicit_miner,
                    session=session,
                    semaphore=asyncio.Semaphore(10),
                    metrics=metrics,
                ),
                self._keep_up(
                    mode=mode,
                    explicit_miner=explicit_miner,
                    session=session,
                    semaphore=asyncio.Semaphore(50),
                    metrics=metrics,
                ),
            )

    async def _transfer_receipts_from_miners(
        self,
        miner_hotkeys: list[str],
        pages: list[int],
        semaphore_limit: int = 50,
        request_timeout: float = 3.0,
    ) -> TransferResult:
        if not miner_hotkeys or not pages:
            return TransferResult(0, 0, [], [])

        miners = await self._fetch_miners(miner_hotkeys)
        miner_infos: list[tuple[str, str, int]] = [
            (m[0], m[1], m[2]) for m in miners if m[1] and m[2] and m[1] != "0.0.0.0"
        ]
        if not miner_infos:
            return TransferResult(0, 0, [], [])

        semaphore = asyncio.Semaphore(semaphore_limit)
        async with aiohttp.ClientSession() as session:
            return await ReceiptsTransfer.transfer(
                miners=miner_infos,
                pages=pages,
                session=session,
                semaphore=semaphore,
                request_timeout=request_timeout,
            )

    async def _run_full_transfer_cycle(
        self,
        miner_hotkeys: list[str],
        cutoff_hours: int = 5,
        n_active_pages: int = 2,
        active_semaphore_limit: int = 50,
        catchup_semaphore_limit: int = 10,
        active_timeout: float = 1.0,
        catchup_timeout: float = 3.0,
    ) -> tuple[TransferResult, TransferResult]:
        # Compute page windows
        cutoff_ts = timezone.now() - datetime.timedelta(hours=cutoff_hours)
        catchup_cutoff_page = LocalFilesystemPagedReceiptStore.current_page_at(cutoff_ts)
        current_page = LocalFilesystemPagedReceiptStore.current_page()

        active_pages = list(reversed(range(current_page - n_active_pages + 1, current_page + 1)))
        catchup_pages = list(
            reversed(
                range(
                    catchup_cutoff_page, max(catchup_cutoff_page, current_page - n_active_pages + 1)
                )
            )
        )

        miners = await self._fetch_miners(miner_hotkeys)
        miner_infos: list[tuple[str, str, int]] = [
            (m[0], m[1], m[2]) for m in miners if m[1] and m[2] and m[1] != "0.0.0.0"
        ]
        if not miner_infos:
            return TransferResult(0, 0, [], []), TransferResult(0, 0, [], [])

        async with aiohttp.ClientSession() as session:
            active_result = await ReceiptsTransfer.transfer(
                miners=miner_infos,
                pages=active_pages,
                session=session,
                semaphore=asyncio.Semaphore(active_semaphore_limit),
                request_timeout=active_timeout,
            )
            catchup_result = await ReceiptsTransfer.transfer(
                miners=miner_infos,
                pages=catchup_pages,
                session=session,
                semaphore=asyncio.Semaphore(catchup_semaphore_limit),
                request_timeout=catchup_timeout,
            )

        return active_result, catchup_result

    def create_job_finished_receipt(
        self,
        job_uuid: str,
        miner_hotkey: str,
        validator_hotkey: str,
        time_started: datetime.datetime,
        time_took_us: int,
        score_str: str,
    ) -> JobFinishedReceipt:
        payload = JobFinishedReceiptPayload(
            job_uuid=job_uuid,
            miner_hotkey=miner_hotkey,
            validator_hotkey=validator_hotkey,
            timestamp=datetime.datetime.now(datetime.UTC),
            time_started=time_started,
            time_took_us=time_took_us,
            score_str=score_str,
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
        )

    def create_job_started_receipt(
        self,
        job_uuid: str,
        miner_hotkey: str,
        validator_hotkey: str,
        executor_class: str,
        is_organic: bool,
        ttl: int,
    ) -> tuple[JobStartedReceiptPayload, str]:
        try:
            payload = JobStartedReceiptPayload(
                job_uuid=job_uuid,
                miner_hotkey=miner_hotkey,
                validator_hotkey=validator_hotkey,
                timestamp=datetime.datetime.now(datetime.UTC),
                executor_class=executor_class,
                is_organic=is_organic,
                ttl=ttl,
            )

            validator_kp = settings.BITTENSOR_WALLET().get_hotkey()
            validator_signature = sign_blob(validator_kp, payload.blob_for_signing())

            logger.debug(
                "Created JobStartedReceipt payload for job %s (miner: %s, validator: %s, organic: %s)",
                job_uuid,
                miner_hotkey,
                validator_hotkey,
                is_organic,
            )

            return payload, validator_signature

        except Exception as e:
            raise ReceiptsGenerationError(f"Failed to create job started receipt: {e}") from e

    async def get_valid_job_started_receipts_for_miner(
        self, miner_hotkey: str, at_time: datetime.datetime
    ) -> list[JobStartedReceipt]:
        try:

            def _query() -> list[JobStartedReceipt]:
                qs = JobStartedReceipt.objects.valid_at(at_time).filter(miner_hotkey=miner_hotkey)
                return list(qs.all())

            receipts: list[JobStartedReceipt] = await sync_to_async(_query, thread_sensitive=True)()

            logger.debug(
                "Retrieved %s valid job started receipts for miner %s at %s",
                len(receipts),
                miner_hotkey,
                at_time,
            )

            return receipts

        except Exception as e:
            logger.error("Failed to get valid job started receipts for miner: %s", e)
            return []

    async def get_job_finished_receipts_for_miner(
        self, miner_hotkey: str, job_uuids: list[str]
    ) -> list[JobFinishedReceipt]:
        try:
            if not job_uuids:
                return []

            def _query() -> list[JobFinishedReceipt]:
                qs = JobFinishedReceipt.objects.filter(
                    miner_hotkey=miner_hotkey, job_uuid__in=job_uuids
                )
                return list(qs.all())

            receipts: list[JobFinishedReceipt] = await sync_to_async(
                _query, thread_sensitive=True
            )()

            logger.debug(
                "Retrieved %s job finished receipts for miner %s (jobs: %s)",
                len(receipts),
                miner_hotkey,
                len(job_uuids),
            )

            return receipts

        except Exception as e:
            logger.error("Failed to get job finished receipts for miner: %s", e)
            return []

    async def get_job_started_receipt_by_uuid(self, job_uuid: str) -> JobStartedReceipt | None:
        try:
            django_receipt = await sync_to_async(
                JobStartedReceipt.objects.get, thread_sensitive=True
            )(job_uuid=job_uuid)
            logger.debug(
                "Retrieved JobStartedReceipt for job %s (miner: %s, validator: %s)",
                job_uuid,
                django_receipt.miner_hotkey,
                django_receipt.validator_hotkey,
            )
            return django_receipt
        except JobStartedReceipt.DoesNotExist:
            logger.debug("No JobStartedReceipt found for job %s", job_uuid)
            return None
        except Exception as e:
            logger.error("Failed to get JobStartedReceipt for job %s: %s", job_uuid, e)
            return None

    async def get_completed_job_receipts_for_block_range(
        self, start_block: int, end_block: int
    ) -> list[Receipt]:
        if start_block >= end_block:
            logger.warning(
                "Invalid block range provided: start_block (%s) >= end_block (%s)",
                start_block,
                end_block,
            )
            return []

        try:
            start_timestamp = await self._get_block_timestamp(start_block)
            end_timestamp = await self._get_block_timestamp(end_block)

            finished_receipts_qs = JobFinishedReceipt.objects.filter(
                timestamp__gte=start_timestamp,
                timestamp__lt=end_timestamp,
            )
            receipts: list[Receipt] = []
            async for django_receipt in finished_receipts_qs:
                receipts.append(django_receipt.to_receipt())

            logger.info(
                "Found %s completed job receipts for blocks %s-%s",
                len(receipts),
                start_block,
                end_block,
            )
            return receipts
        except Exception as ex:
            logger.error(
                "Failed to list receipts for block range %s-%s: %s",
                start_block,
                end_block,
                ex,
            )
            return []

    async def _fetch_miners(self, hotkeys: list[str]) -> list[tuple[str, str, int]]:
        """Fetch miner endpoints (hotkey, address, port) for given hotkeys."""

        def _query() -> list[tuple[str, str, int]]:
            return list(
                Miner.objects.filter(hotkey__in=hotkeys).values_list("hotkey", "address", "port")
            )

        return await sync_to_async(_query, thread_sensitive=True)()

    async def _fetch_receipts_for_range(self, start_block: int, end_block: int) -> list[Receipt]:
        """Fetch JobFinished receipts for blocks in [start_block, end_block)."""

        start_ts = await self._get_block_timestamp(start_block)
        end_ts = await self._get_block_timestamp(end_block)

        receipts_qs = JobFinishedReceipt.objects.filter(
            timestamp__gte=start_ts,
            timestamp__lt=end_ts,
        )
        receipts: list[Receipt] = []
        async for receipt_data in receipts_qs:
            receipts.append(receipt_data.to_receipt())
        return receipts

    async def _get_block_timestamp(self, block_number: int) -> datetime.datetime:
        try:
            block = await Block.objects.aget(block_number=block_number)
            return block.creation_timestamp
        except Exception as db_ex:
            logger.debug(
                "Block %s not found in DB or DB error occurred: %s",
                block_number,
                db_ex,
            )

        try:
            ts = await supertensor().get_block_timestamp(block_number)
            if isinstance(ts, datetime.datetime):
                return ts
            else:
                raise ValueError(f"Expected datetime, got {type(ts)}")
        except Exception as chain_ex:  # noqa: BLE001 - broad to surface upstream
            logger.warning(
                "Failed to resolve timestamp for block %s via chain: %s",
                block_number,
                chain_ex,
            )
            raise
