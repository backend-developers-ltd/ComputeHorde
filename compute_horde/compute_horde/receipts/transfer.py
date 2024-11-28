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


class ReceiptFetchError(Exception):
    pass


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


async def _merged_generators(*gens):
    tasks = [asyncio.create_task(gen) for gen in gens]
    while tasks:
        done, _ = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
        for task in done:
            try:
                yield task.result()
                tasks.remove(task)
                tasks.append(asyncio.create_task(next(gen)))
            except StopAsyncIteration:
                tasks.remove(task)


class ReceiptsTransfer:
    """
    HTTP client for fetching receipts from a target based on an HTTP URL.
    Uses HTTP range requests to only retrieve receipts added since last run.
    """

    def __init__(self, server_url: str, checkpoint_backend: CheckpointBackend):
        self._receipts_url = server_url.rstrip("/")
        self._checkpoints = checkpoint_backend

    @classmethod
    async def transfer(cls, miners: Sequence[MinerInfo], session: aiohttp.ClientSession) -> int:
        """
        Efficiently transfer receipts from multiple miners at the same time, storing
        them in local database.
        """
        # TODO: Be smart about catching up when we have no receipts at all?
        latest_page = LocalFilesystemPagedReceiptStore.active_page_id()
        pages = [latest_page - 1, latest_page]

        # TODO: cache name should be coming from settings
        # TODO: inject it automatically
        checkpoint_backend = DjangoCacheCheckpointBackend("receipts_checkpoints")

        # More concurrency = more load from switching asyncio tasks
        # Less concurrency = higher latency miners will slow down the process
        # TODO: Configurable, but default to 100 or so
        semaphore = asyncio.Semaphore(70)

        async def transfer_page_from_miner(miner: MinerInfo, page: int):
            try:
                async with semaphore:
                    hotkey, ip, port = miner
                    transfer = cls(
                        server_url=f"http://{ip}:{port}/receipts",
                        checkpoint_backend=checkpoint_backend,
                    )
                    return await transfer.get_new_receipts_on_page(page, session)
            except Exception as e:
                logger.error(f"Got exception from {miner}: {e.__class__.__name__} {e}", exc_info=False)
                return []

        transfer_tasks = [
            asyncio.create_task(transfer_page_from_miner(miner, page))
            for miner in miners
            for page in pages
        ]

        total = 0

        # Place received receipts into buckets based on receipt model type
        receipts_by_type: defaultdict[type[ReceiptModel], list[ReceiptModel]] = defaultdict(list)
        for completed_task in asyncio.as_completed(transfer_tasks):
            for receipt in await completed_task:
                model = receipt_to_django_model(receipt)
                model_type = model.__class__
                bucket = receipts_by_type[model_type]
                bucket.append(model)
                if len(bucket) >= 1000:
                    await model_type.objects.abulk_create(bucket, ignore_conflicts=True)
                    total += len(bucket)
                    logger.info(f"Transferred {len(bucket)} {model_type.__name__} receipts")
                    bucket.clear()

        # Batch insert
        for model_type, bucket in receipts_by_type.items():
            await model_type.objects.abulk_create(bucket, ignore_conflicts=True)
            total += len(bucket)
            logger.info(f"Transferred {len(bucket)} {model_type.__name__} receipts")

        return total


    async def get_new_receipts_on_page(self, page: int, session: aiohttp.ClientSession) -> list[Receipt]:
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
        response = await session.get(page_url, headers=headers, allow_redirects=False)
        if response.status in {404, 416}:
            logger.debug(f"Nothing to fetch from %s - %s", page_url, response.status)
            return []
        if response.status not in {200, 206}:
            logger.warning(f"Request failed for page {page}: {response.status}")
            return []
        jsonl_content = await response.read()

        # This iterates over lines without splitting/copying the string in memory
        receipts = []
        for line in BytesIO(jsonl_content):
            try:
                receipt = Receipt.model_validate_json(line)
                # TODO: throw=True
                receipt.verify_miner_signature(throw=False)
                receipt.verify_validator_signature(throw=False)
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
