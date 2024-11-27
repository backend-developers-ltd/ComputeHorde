import asyncio
import logging
from collections import defaultdict
from collections.abc import AsyncIterable, Sequence
from typing import Protocol, TypeAlias

import aiohttp
import pydantic
from django.core.cache import caches
from more_itertools import chunked

from compute_horde.receipts.models import ReceiptModel, receipt_to_django_model
from compute_horde.receipts.schemas import (
    Receipt,
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


async def _aiter_aiohttp_lines(response: aiohttp.ClientResponse):
    """
    Iterate over lines within an aiohttp response.
    """
    while True:
        line = await response.content.readline()
        if line == b'':
            break
        yield line[:-1]  # strip newline


class ReceiptsTransfer:
    """
    HTTP client for fetching receipts from a target based on an HTTP URL.
    Uses HTTP range requests to only retrieve receipts added since last run.
    """

    def __init__(self, server_url: str, checkpoint_backend: CheckpointBackend):
        self._receipts_url = server_url.rstrip("/")
        self._checkpoints = checkpoint_backend

    @classmethod
    async def transfer_from(cls, miners: Sequence[MinerInfo]):
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

        # TODO: Configurable, but default to 100 or so
        semaphore = asyncio.Semaphore(100)
        transferred_receipts = []

        async def transfer_page_from_miner(miner: MinerInfo, page: int):
            hotkey, ip, port = miner
            transfer = ReceiptsTransfer(
                server_url=f"http://{ip}:{port}/receipts",
                checkpoint_backend=checkpoint_backend,
            )
            async with semaphore:
                async for receipt in transfer._get_new_receipts(page):
                    transferred_receipts.append(receipt)

        await asyncio.gather(*(
            asyncio.create_task(transfer_page_from_miner(miner, page))
            for page in pages
            for miner in miners
        ))

        await cls._store_receipts(transferred_receipts)

    async def _get_new_receipts(self, page: int) -> AsyncIterable[Receipt]:
        """
        Iterate over receipts present on given page of the remote server.
        Will start from last checkpoint for this page, if available.
        Will also save a checkpoint for future runs.
        TODO: Verify signatures
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
            # httpx inflates the file automatically.
            headers = {
                "Accept-Encoding": "gzip",
            }

        async with aiohttp.ClientSession() as http:
            # TODO: short timeout
            # As the request should be as fast as possible, don't allow redirecting - clients must respond immediately.
            response = await http.get(page_url, headers=headers, allow_redirects=False)

            if response.status in {404, 416}:
                logger.debug(f"Nothing to fetch from %s - %s", page_url, response.status)
                return

            if response.status not in {200, 206}:
                logger.warning(f"Request failed for page {page}: {response.status}")
                return

            async for line in _aiter_aiohttp_lines(response):
                try:
                    yield Receipt.model_validate_json(line)
                except pydantic.ValidationError:
                    logger.warning(
                        f"skipping invalid line: %s%s",
                        line[:100],
                        ' (...)' if len(line) > 100 else ''
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

    @staticmethod
    async def _store_receipts(receipts: list[Receipt]):
        """
        Convert receipts to django models and batch-save them
        """
        receipts_by_type: defaultdict[type[ReceiptModel], list[ReceiptModel]] = defaultdict(list)
        for receipt in receipts:
            model = receipt_to_django_model(receipt)
            receipts_by_type[model.__class__].append(model)

        for receipt_model, receipts in receipts_by_type.items():
            for receipts_chunk in chunked(receipts, 1000):
                await receipt_model.objects.abulk_create(receipts_chunk, ignore_conflicts=True)
