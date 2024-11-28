import asyncio
import logging
from collections import defaultdict
from collections.abc import Sequence
from io import BytesIO
from typing import Protocol, TypeAlias

import aiohttp
import pydantic
from django.core.cache import caches

from compute_horde.receipts.models import ReceiptModel, receipt_to_django_model
from compute_horde.receipts.schemas import (
    Receipt, BadMinerReceiptSignature, BadValidatorReceiptSignature,
)
from compute_horde.receipts.store.local import LocalFilesystemPagedReceiptStore

logger = logging.getLogger(__name__)

Offset = int
MinerInfo: TypeAlias = tuple[str, str, int]


class CheckpointBackend(Protocol):
    async def get(self, key: str) -> int: ...

    async def set(self, key: str, checkpoint: int) -> None: ...


class DjangoCacheCheckpointBackend:
    def __init__(self, cache: str = "default"):
        self.cache = caches[cache]

    async def get(self, key: str) -> int:
        try:
            return int(await self.cache.aget(key, 0))
        except TypeError:
            logger.warning(f"Django cache contained non-integer checkpoint value for {key}")
            return 0

    async def set(self, key: str, checkpoint: int) -> None:
        await self.cache.aset(key, str(checkpoint))


class ReceiptsTransfer:
    """
    HTTP client for fetching receipts from a target based on an HTTP URL.
    Uses HTTP range requests to only retrieve receipts added since last run.
    """

    def __init__(self, server_url: str, checkpoint_backend: CheckpointBackend):
        self._receipts_url = server_url.rstrip("/")
        self._checkpoints = checkpoint_backend

    @classmethod
    async def transfer(
        cls,
        miners: Sequence[MinerInfo],
        session: aiohttp.ClientSession,
        concurrency: int,
        max_time_per_miner_page: float,
        batch_insert_size: int,
    ) -> tuple[int, int]:
        """
        Efficiently transfer receipts from multiple miners at the same time, storing
        them in local database.
        """
        # TODO: Be smart about catching up when we have no receipts at all?
        latest_page = LocalFilesystemPagedReceiptStore.active_page_id()
        pages = [latest_page - 1, latest_page]

        checkpoint_backend = DjangoCacheCheckpointBackend("receipts_checkpoints")

        # More concurrency = more load from switching asyncio tasks
        # Less concurrency = higher latency miners will slow down the process
        semaphore = asyncio.Semaphore(concurrency)

        async def transfer_page_from_miner(miner: MinerInfo, page: int):
            try:
                async with semaphore:
                    hotkey, ip, port = miner
                    transfer = cls(
                        server_url=f"http://{ip}:{port}/receipts",
                        checkpoint_backend=checkpoint_backend,
                    )
                    return await transfer.get_new_receipts_on_page(page, session, timeout=max_time_per_miner_page)
            except Exception as e:
                logger.error(f"Got exception from {miner}: {e.__class__.__name__} {e}", exc_info=False)
                raise

        transfer_tasks = [
            asyncio.create_task(transfer_page_from_miner(miner, page))
            for miner in miners
            for page in pages
        ]

        # Place received receipts into buckets based on receipt model type
        receipts_by_type: defaultdict[type[ReceiptModel], list[ReceiptModel]] = defaultdict(list)
        total_receipts = 0
        failures = 0

        # Wait for all transfer tasks and handle them as soon as they finish
        for transfer_task in asyncio.as_completed(transfer_tasks):
            try:
                for receipt in await transfer_task:
                    model = receipt_to_django_model(receipt)
                    model_type = model.__class__
                    bucket = receipts_by_type[model_type]
                    bucket.append(model)
                    if len(bucket) >= batch_insert_size:
                        await model_type.objects.abulk_create(bucket, ignore_conflicts=True)
                        total_receipts += len(bucket)
                        logger.info(f"Transferred {len(bucket)} {model_type.__name__} receipts")
                        bucket.clear()
            except Exception:
                # TODO: This catch may be too broad
                failures += 1
                continue

        # Batch insert
        for model_type, bucket in receipts_by_type.items():
            await model_type.objects.abulk_create(bucket, ignore_conflicts=True)
            total_receipts += len(bucket)
            logger.info(f"Transferred {len(bucket)} {model_type.__name__} receipts")

        return total_receipts, failures

    async def get_new_receipts_on_page(self, page: int, session: aiohttp.ClientSession, timeout: float) -> list[
        Receipt]:
        """
        Fetch a batch of receipts from remote server.
        Will start from last checkpoint for this page, if available.
        Checkpoint will be saved when this context manager exits.
        """
        page_url = f"{self._receipts_url}/{page}.jsonl"

        checkpoint_key = page_url
        checkpoint = await self._checkpoints.get(checkpoint_key)
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

        # TODO: short timeout
        # As the request should be as fast as possible, don't allow redirecting - clients must respond immediately.
        response = await session.get(page_url, headers=headers, allow_redirects=False, timeout=timeout)
        if response.status in {404, 416}:
            logger.debug(f"Nothing to fetch from %s - %s", page_url, response.status)
            return []
        if response.status not in {200, 206}:
            logger.warning(f"Request failed for page {page}: {response.status}")
            return []
        jsonl_content = await response.read()

        receipts = []
        for line in BytesIO(jsonl_content):
            try:
                receipt = Receipt.model_validate_json(line)
                receipt.verify_miner_signature(throw=True)
                receipt.verify_validator_signature(throw=True)
                receipts.append(receipt)
            except pydantic.ValidationError:
                logger.warning(
                    f"skipping invalid line: %s%s",
                    line[:100],
                    ' (...)' if len(line) > 100 else '',
                )
            except BadMinerReceiptSignature as e:
                logger.warning(
                    "Skipping receipt with bad miner signature: %s",
                    e.receipt.payload.job_uuid,
                )
            except BadValidatorReceiptSignature as e:
                logger.warning(
                    "Skipping receipt with bad validator signature: %s",
                    e.receipt.payload.job_uuid,
                )

        # Save the total page size so that next time we request the page we know what range to request.
        if use_range_request:
            # Range requests return a "content-range" header that contains full size of the file.
            # We ask the server not to gzip range requests - so this is the real total size.
            range_header = response.headers["content-range"]
            assert range_header.lower().startswith("bytes ")
            _, total_str = range_header.split("/")
            await self._checkpoints.set(checkpoint_key, int(total_str))
        else:
            # We got the complete page file, use its size for next checkpoint.
            await self._checkpoints.set(checkpoint_key, response.content.total_bytes)

        return receipts
