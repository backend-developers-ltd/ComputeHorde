import asyncio
import logging
from collections import defaultdict
from collections.abc import Sequence
from io import BytesIO
from typing import TypeAlias

import aiohttp
import pydantic
from aiohttp import ClientTimeout
from asgiref.sync import sync_to_async
from django.core.exceptions import ImproperlyConfigured

from compute_horde.receipts.models import ReceiptModel, receipt_to_django_model
from compute_horde.receipts.schemas import (
    BadMinerReceiptSignature,
    BadValidatorReceiptSignature,
    Receipt,
)
from compute_horde.receipts.transfer_checkpoints import checkpoint_backend

logger = logging.getLogger(__name__)

MinerInfo: TypeAlias = tuple[str, str, int]


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
    ) -> tuple[int, int]:
        """
        Efficiently transfer receipts from multiple miners at the same time, storing them in local database.
        Still, the number of pages transferred at the same time should be limited to 1-2.
        """

        async def rate_limited_transfer(
            transfer: ReceiptsTransfer, page: int, session: aiohttp.ClientSession
        ):
            try:
                async with semaphore:
                    # This both fetches and verifies the receipts.
                    return await transfer.get_new_receipts_on_page(
                        page=page,
                        session=session,
                        timeout=request_timeout,
                    )
            except (TimeoutError, Exception) as e:
                logger.error(
                    f"Transfer failed: " f"{miner=} " f"{page=} " f"{type(e).__name__} " f"{e}",
                )
                raise

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
        failures = 0

        # Wait for transfer tasks in parallel, handle a batch of receipts as soon as any is available
        for transfer_task in asyncio.as_completed(transfer_tasks):
            try:
                transferred_batch = await transfer_task
                for receipt in transferred_batch:
                    model = receipt_to_django_model(receipt)
                    model_type = type(model)
                    bucket = receipts_by_type[model_type]
                    bucket.append(model)
                    if len(bucket) >= 1000:
                        await model_type.objects.abulk_create(bucket, ignore_conflicts=True)  # type: ignore
                        total_receipts += len(bucket)
                        logger.info(f"Stored {len(bucket)} {model_type.__name__} receipts")
                        bucket.clear()
            except (RuntimeError, ImproperlyConfigured):
                # Don't catch this as it may block the script from exiting.
                raise
            except (TimeoutError, Exception):
                failures += 1
                continue

        # Insert the remainder of the receipts
        for model_type, bucket in receipts_by_type.items():
            await model_type.objects.abulk_create(bucket, ignore_conflicts=True)  # type: ignore
            total_receipts += len(bucket)
            logger.info(f"Stored {len(bucket)} {model_type.__name__} receipts")

        return total_receipts, failures

    def __init__(self, server_url: str):
        self._receipts_url = server_url.rstrip("/")

    async def get_new_receipts_on_page(
        self, page: int, session: aiohttp.ClientSession, timeout: float
    ) -> list[Receipt]:
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
            return []
        if response.status not in {200, 206}:
            raise Exception(f"Request failed for {page_url}: {response.status}")
        jsonl_content = await response.read()

        # Put this on a worker thread, otherwise other HTTP requests are more likely to time out waiting for this.
        receipts = await sync_to_async(self._to_valid_receipts, thread_sensitive=False)(
            jsonl_content
        )

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

        return receipts

    def page_url(self, page: int) -> str:
        return f"{self._receipts_url}/{page}.jsonl"

    def _to_valid_receipts(self, received_bytes: bytes) -> list[Receipt]:
        """
        Converts a received JSONL chunk into valid, signed receipts - one line at a time.
        Skips anything else.
        """
        receipts = []
        for line in BytesIO(received_bytes):
            try:
                receipt = Receipt.model_validate_json(line)
                receipt.verify_miner_signature(throw=True)
                receipt.verify_validator_signature(throw=True)
                receipts.append(receipt)
            except pydantic.ValidationError:
                # This is potentially a serious issue.
                # A receipt line that doesn't validate could be a schema mismatch or an incomplete receipt line.
                # Schema incompatibility is a showstopper for syncing a miner.
                # Incomplete receipts will get lost in transfer and the cause of it must be investigated.
                logger.error(
                    "skipping line: failed validation: %s%s",
                    line[:100],
                    " (...)" if len(line) > 100 else "",
                )
                continue
            except BadMinerReceiptSignature as e:
                logger.warning(
                    "Skipping %s with bad miner signature: %s",
                    e.receipt.payload.receipt_type,
                    e.receipt.payload.job_uuid,
                )
                continue
            except BadValidatorReceiptSignature as e:
                logger.warning(
                    "Skipping %s with bad validator signature: %s",
                    e.receipt.payload.receipt_type,
                    e.receipt.payload.job_uuid,
                )
                continue
        return receipts
