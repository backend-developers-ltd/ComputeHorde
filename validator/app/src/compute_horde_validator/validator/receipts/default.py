import asyncio
import datetime
import logging

import aiohttp
from asgiref.sync import sync_to_async
from compute_horde.receipts import Receipt
from compute_horde.receipts.models import (
    JobFinishedReceipt,
    JobStartedReceipt,
)
from compute_horde.receipts.schemas import JobFinishedReceiptPayload, JobStartedReceiptPayload
from compute_horde.receipts.store.local import LocalFilesystemPagedReceiptStore
from compute_horde.receipts.transfer import ReceiptsTransfer
from compute_horde.utils import sign_blob
from django.conf import settings

from compute_horde_validator.validator.allowance.utils.supertensor import supertensor
from compute_horde_validator.validator.models import Miner
from compute_horde_validator.validator.models.allowance.internal import Block
from compute_horde_validator.validator.receipts.base import ReceiptsBase
from compute_horde_validator.validator.receipts.types import (
    ReceiptsGenerationError,
)

logger = logging.getLogger(__name__)


class Receipts(ReceiptsBase):
    """
    Default implementation of receipts manager.
    """

    async def scrape_receipts_from_miners(
        self, miner_hotkeys: list[str], start_block: int, end_block: int
    ) -> list[Receipt]:
        if not miner_hotkeys:
            logger.info("No miner hotkeys provided for scraping")
            return []
        if start_block >= end_block:
            logger.warning(
                "Invalid block range provided: start_block (%s) >= end_block (%s)",
                start_block,
                end_block,
            )
            return []

        try:
            start_ts = await self._get_block_timestamp(start_block)
            end_ts = await self._get_block_timestamp(end_block)

            start_page = LocalFilesystemPagedReceiptStore.current_page_at(start_ts)
            end_page = LocalFilesystemPagedReceiptStore.current_page_at(end_ts)
            if end_page < start_page:
                logger.warning(
                    "Computed page range is empty: start_page=%s end_page=%s",
                    start_page,
                    end_page,
                )
                return []
            pages = list(range(start_page, end_page + 1))

            miners = await self._fetch_miners(miner_hotkeys)
            miner_infos: list[tuple[str, str, int]] = [
                (m[0], m[1], m[2]) for m in miners if m[1] and m[2] and m[1] != "0.0.0.0"
            ]
            if not miner_infos:
                logger.info("No valid miner endpoints resolved for scraping")
                return []

            semaphore = asyncio.Semaphore(25)
            async with aiohttp.ClientSession() as session:
                result = await ReceiptsTransfer.transfer(
                    miners=miner_infos,
                    pages=pages,
                    session=session,
                    semaphore=semaphore,
                    request_timeout=3.0,
                )
                logger.info(
                    "Scrape finished: receipts=%s successful_transfers=%s transfer_errors=%s line_errors=%s",
                    result.n_receipts,
                    result.n_successful_transfers,
                    len(result.transfer_errors),
                    len(result.line_errors),
                )

            receipts = await self._fetch_receipts_for_range(start_ts, end_ts, miner_hotkeys)
            return receipts
        except Exception as ex:
            logger.error(
                "Failed to scrape receipts for block range %s-%s: %s",
                start_block,
                end_block,
                ex,
            )
            return []

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

    def get_valid_job_started_receipts_for_miner(
        self, miner_hotkey: str, at_time: datetime.datetime
    ) -> list[JobStartedReceipt]:
        try:
            qs = JobStartedReceipt.objects.valid_at(at_time).filter(miner_hotkey=miner_hotkey)
            receipts: list[JobStartedReceipt] = [r for r in qs.all()]

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

    def get_job_finished_receipts_for_miner(
        self, miner_hotkey: str, job_uuids: list[str]
    ) -> list[JobFinishedReceipt]:
        try:
            if not job_uuids:
                return []
            qs = JobFinishedReceipt.objects.filter(
                miner_hotkey=miner_hotkey, job_uuid__in=job_uuids
            )
            receipts: list[JobFinishedReceipt] = [r for r in qs.all()]

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

    def get_job_started_receipt_by_uuid(self, job_uuid: str) -> JobStartedReceipt | None:
        try:
            django_receipt = JobStartedReceipt.objects.get(job_uuid=job_uuid)
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

    async def _fetch_receipts_for_range(
        self, start_ts: datetime.datetime, end_ts: datetime.datetime, hotkeys: list[str]
    ) -> list[Receipt]:
        """Fetch JobFinished receipts in [start_ts, end_ts) for given miner hotkeys and convert to Receipt objects."""

        receipts_qs = JobFinishedReceipt.objects.filter(
            timestamp__gte=start_ts,
            timestamp__lt=end_ts,
            miner_hotkey__in=hotkeys,
        )
        receipts = []
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
