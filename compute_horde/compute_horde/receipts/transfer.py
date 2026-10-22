import asyncio
import datetime
import logging
from collections import defaultdict
from collections.abc import Sequence
from dataclasses import dataclass
from io import BytesIO
from typing import TypeAlias

import aiohttp
import pydantic
from aiohttp import ClientTimeout
from asgiref.sync import sync_to_async
from django.core.exceptions import ImproperlyConfigured
from django.db import DataError, IntegrityError

from compute_horde.receipts.models import ReceiptModel, receipt_to_django_model
from compute_horde.receipts.schemas import (
    BadMinerReceiptSignature,
    BadValidatorReceiptSignature,
    Receipt,
)
from compute_horde.receipts.transfer_checkpoints import checkpoint_backend

logger = logging.getLogger(__name__)

MinerInfo: TypeAlias = tuple[str, str, int]

# Errors a single receipt row can cause while being written. Connection-level database errors are
# deliberately not here: retrying rows one by one would not help and the transfer must fail loudly.
_ROW_STORAGE_ERRORS = (DataError, IntegrityError, ValueError, TypeError, OverflowError)

_STORAGE_BATCH_SIZE = 1000

# Receipts age out by their own timestamp, so a future-dated receipt outlives the retention period.
# The skew tolerates the issuing validator's clock running ahead of the local one.
MAX_RECEIPT_TIMESTAMP_SKEW = datetime.timedelta(hours=1)


class FutureDatedReceipt(Exception):
    """
    The receipt's timestamp is more than MAX_RECEIPT_TIMESTAMP_SKEW ahead of the local clock.
    """

    def __init__(self, receipt: Receipt):
        super().__init__(
            f"Receipt {receipt.payload.job_uuid} is dated {receipt.payload.timestamp.isoformat()}, "
            f"more than {MAX_RECEIPT_TIMESTAMP_SKEW} ahead of the local clock"
        )
        self.receipt = receipt


class TransferException(Exception):
    """
    Transfer exception means the request for receipts completely failed - timed out, got an error response etc.
    """

    def __init__(self, miner: MinerInfo, page: int):
        super().__init__(f"Transfer failed: {miner=} {page=}")
        self.miner: MinerInfo = miner
        self.page: int = page


class LineException(Exception):
    """
    A line exception means that, while processing a correct response, one of the receipt lines
    could not be processed: it failed validation, signature verification or storage.
    """

    def __init__(self, cause: Exception):
        super().__init__(f"Transferred line skipped: {type(cause).__name__}: {cause}")
        self.__cause__ = self.cause = cause


@dataclass
class TransferResult:
    n_receipts: int
    n_successful_transfers: int
    transfer_errors: list[TransferException]
    line_errors: list[LineException]


class ReceiptsTransfer:
    """
    HTTP client for fetching receipts from a target based on an HTTP URL.
    Uses HTTP range requests to only retrieve receipts added since last run.
    """

    @classmethod
    async def transfer(
        cls,
        miners: Sequence[MinerInfo],
        pages: Sequence[int],
        session: aiohttp.ClientSession,
        semaphore: asyncio.Semaphore,
        request_timeout: float,
    ) -> TransferResult:
        """
        Efficiently transfer receipts from multiple miners at the same time, storing them in local database.
        Still, the number of pages transferred at the same time should be limited to 1-2.
        """

        async def rate_limited_transfer(
            transfer: ReceiptsTransfer, page: int, session: aiohttp.ClientSession
        ) -> tuple[list[Receipt], list[LineException]]:
            try:
                async with semaphore:
                    # This both fetches and verifies the receipts.
                    return await transfer.get_new_receipts_on_page(
                        page=page,
                        session=session,
                        timeout=request_timeout,
                    )
            except (RuntimeError, ImproperlyConfigured):
                # Don't catch this as it may block the script from exiting.
                raise
            except (TimeoutError, Exception) as e:
                # Wrap other exceptions as transfer exceptions
                raise TransferException(miner, page) from e

        # Create transfer tasks, one transfer task = one page from one miner
        transfer_tasks = []
        for page in pages:
            for miner in miners:
                hotkey, ip, port = miner
                transfer = cls(f"http://{ip}:{port}/receipts")
                transfer_tasks.append(
                    asyncio.create_task(rate_limited_transfer(transfer, page, session))
                )

        # Place received receipts into buckets based on receipt model type
        receipts_by_type: defaultdict[type[ReceiptModel], list[ReceiptModel]] = defaultdict(list)
        total_receipts = 0
        successful_transfers = 0
        transfer_errors: list[TransferException] = []
        line_errors: list[LineException] = []

        async def store_bucket(model_type: type[ReceiptModel]) -> None:
            nonlocal total_receipts
            bucket = receipts_by_type[model_type]
            n_stored, storage_errors = await _store_receipts(model_type, bucket)
            total_receipts += n_stored
            line_errors.extend(storage_errors)
            logger.info(f"Stored {n_stored} {model_type.__name__} receipts")
            bucket.clear()

        # Wait for transfer tasks in parallel, handle a batch of receipts as soon as any is available
        for transfer_task in asyncio.as_completed(transfer_tasks):
            try:
                transferred_batch, batch_line_errors = await transfer_task
                line_errors.extend(batch_line_errors)
                successful_transfers += 1
                for receipt in transferred_batch:
                    model = receipt_to_django_model(receipt)
                    receipts_by_type[type(model)].append(model)
                    if len(receipts_by_type[type(model)]) >= _STORAGE_BATCH_SIZE:
                        await store_bucket(type(model))
            except TransferException as e:
                transfer_errors.append(e)
                continue

        # Insert the remainder of the receipts
        for model_type in receipts_by_type:
            await store_bucket(model_type)

        return TransferResult(total_receipts, successful_transfers, transfer_errors, line_errors)

    def __init__(self, server_url: str):
        self._receipts_url = server_url.rstrip("/")

    async def get_new_receipts_on_page(
        self, page: int, session: aiohttp.ClientSession, timeout: float
    ) -> tuple[list[Receipt], list[LineException]]:
        """
        Fetch a batch of receipts from remote server.
        Will start from last checkpoint for this page, if available.
        """
        page_url = self.page_url(page)
        checkpoints = checkpoint_backend()

        checkpoint_key = page_url
        checkpoint = await checkpoints.get(checkpoint_key)
        use_range_request = checkpoint > 0

        if use_range_request:
            # We're re-requesting the page starting from a known offset.
            # Ask for file content that was added after the page was last checked.
            # Also, range request and gzip won't work together.
            # (the range relates to compressed bytes then, which are meaningless here.)
            headers = {
                "Accept-Encoding": "",
                "Range": f"bytes={checkpoint}-",
            }
        else:
            # This is the first time we're requesting this page.
            # This will only receive a gzipped page if it's available.
            # If it's not, this will still work but the raw page file will be sent.
            # aiohttp inflates the file automatically.
            headers = {
                "Accept-Encoding": "gzip",
            }

        # As the request should be as fast as possible, don't allow redirecting - clients must respond immediately.
        response = await session.get(
            page_url, headers=headers, allow_redirects=False, timeout=ClientTimeout(total=timeout)
        )
        if response.status in {404, 416}:
            # 404 - miner doesn't have the page (yet / anymore / at all)
            # 416 - no new receipts on page
            logger.debug(f"Nothing to fetch from {page_url}: {response.status}")
            return [], []
        if response.status not in {200, 206}:
            raise Exception(f"Request failed for {page_url}: {response.status}")
        jsonl_content = await response.read()

        # Put this on a worker thread, otherwise other HTTP requests are more likely to time out waiting for this.
        receipts, line_errors = await sync_to_async(
            self._to_valid_receipts, thread_sensitive=False
        )(jsonl_content)

        # Save the checkpoint
        if use_range_request:
            # Range requests return a "content-range" header that contains full size of the file.
            # We ask the server not to gzip range requests - so this is the total bytes we got so far.
            range_header = response.headers["content-range"]
            assert range_header.lower().startswith("bytes ")
            _, total_str = range_header.split("/")
            await checkpoints.set(checkpoint_key, int(total_str))
        else:
            # We got the complete page file, use its size for next checkpoint.
            await checkpoints.set(checkpoint_key, len(jsonl_content))

        return receipts, line_errors

    def page_url(self, page: int) -> str:
        return f"{self._receipts_url}/{page}.jsonl"

    def _to_valid_receipts(
        self, received_bytes: bytes
    ) -> tuple[list[Receipt], list[LineException]]:
        """
        Converts a received JSONL chunk into valid, signed receipts - one line at a time.
        Skips anything else.
        """
        receipts: list[Receipt] = []
        line_errors: list[LineException] = []
        latest_accepted_timestamp = datetime.datetime.now(datetime.UTC) + MAX_RECEIPT_TIMESTAMP_SKEW
        for line in BytesIO(received_bytes):
            try:
                receipt = Receipt.model_validate_json(line)
                if receipt.payload.timestamp > latest_accepted_timestamp:
                    raise FutureDatedReceipt(receipt)
                receipt.verify_miner_signature(throw=True)
                receipt.verify_validator_signature(throw=True)
                receipts.append(receipt)
            except pydantic.ValidationError as e:
                # This is potentially a serious issue.
                # A receipt line that doesn't validate could be a schema mismatch or an incomplete receipt line.
                # Schema incompatibility is a showstopper for syncing a miner.
                # Incomplete receipts will get lost in transfer and the cause of it must be investigated.
                logger.info(
                    "skipping line: failed validation: %s%s",
                    line[:100],
                    " (...)" if len(line) > 100 else "",
                )
                line_errors.append(LineException(e))
                continue
            except BadMinerReceiptSignature as e:
                logger.info(
                    "Skipping %s with bad miner signature: %s",
                    e.receipt.payload.receipt_type,
                    e.receipt.payload.job_uuid,
                )
                line_errors.append(LineException(e))
                continue
            except BadValidatorReceiptSignature as e:
                logger.info(
                    "Skipping %s with bad validator signature: %s",
                    e.receipt.payload.receipt_type,
                    e.receipt.payload.job_uuid,
                )
                line_errors.append(LineException(e))
                continue
            except FutureDatedReceipt as e:
                logger.info(
                    "Skipping %s dated %s, beyond the accepted clock skew: %s",
                    e.receipt.payload.receipt_type,
                    e.receipt.payload.timestamp.isoformat(),
                    e.receipt.payload.job_uuid,
                )
                line_errors.append(LineException(e))
                continue
        return receipts, line_errors


async def _store_receipts(
    model_type: type[ReceiptModel], receipts: list[ReceiptModel]
) -> tuple[int, list[LineException]]:
    """
    Insert receipts in one batch, skipping the ones already stored.

    Validated receipts fit the receipt tables, so the batch insert is expected to succeed. If the
    database still rejects it, the rows are retried one by one so that only the offending receipt
    is lost: receipts of all miners share a batch and one miner must not be able to drop the others.
    Returns the number of rows handed to the database and the errors of the skipped rows.
    """
    try:
        await model_type.objects.abulk_create(receipts, ignore_conflicts=True)  # type: ignore
        return len(receipts), []
    except _ROW_STORAGE_ERRORS as e:
        logger.warning(
            "Batch insert of %d %s receipts rejected by the database, retrying one by one: %s",
            len(receipts),
            model_type.__name__,
            e,
        )

    n_stored = 0
    line_errors: list[LineException] = []
    for receipt in receipts:
        try:
            await model_type.objects.abulk_create([receipt], ignore_conflicts=True)  # type: ignore
            n_stored += 1
        except _ROW_STORAGE_ERRORS as e:
            logger.info(
                "Skipping %s receipt %s rejected by the database: %s",
                model_type.__name__,
                receipt.job_uuid,
                e,
            )
            line_errors.append(LineException(e))
    return n_stored, line_errors
