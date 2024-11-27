import logging
from collections import defaultdict
from collections.abc import AsyncIterable, Sequence
from typing import Protocol

import aiohttp
import pydantic
from django.core.cache import caches
from more_itertools import chunked

from compute_horde.receipts.models import ReceiptModel, receipt_to_django_model
from compute_horde.receipts.schemas import (
    Receipt,
)

logger = logging.getLogger(__name__)


class ReceiptFetchError(Exception):
    pass


Offset = int


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

    async def transfer_new_receipts(
        self,
        pages: Sequence[int],
        # TODO: signed_by_miner: bittensor.wallet,
    ) -> dict[type[ReceiptModel], list[ReceiptModel]]:
        """
        Fetch and _store_ new receipts by looking at last N pages.
        """
        # Retrieve the receipts the pages and group by receipt model type.
        receipts_by_type: defaultdict[type[ReceiptModel], list[ReceiptModel]] = defaultdict(list)
        for page in pages:
            async for receipt in self.transfer_new_receipts_on_page(page):
                model = receipt_to_django_model(receipt)
                receipts_by_type[model.__class__].append(model)

        # Bach insert in chunks.
        for receipt_model, receipts in receipts_by_type.items():
            for receipts_chunk in chunked(receipts, 1000):
                await receipt_model.objects.abulk_create(receipts_chunk, ignore_conflicts=True)

        return dict(receipts_by_type)

    async def transfer_new_receipts_on_page(self, page: int) -> AsyncIterable[Receipt]:
        """
        Iterate over receipts present on given page of the remote server.
        If use_checkpoint=False, all receipts from the page will be returned.
        The checkpoint will be written in any case.
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
                        f"skipping invalid line: {line[:100]}{' (...)' if len(line) > 100 else ''}"
                    )
                    continue

        # Save the total page size so that next time we request the page we know what range to request.
        if use_range_request:
            # Range requests return a "content-range" header that contains full size of the file.
            # We ask the server not to gzip range requests - so this is the real total size.
            range_header = response.headers["content-range"]
            assert range_header.lower().startswith("bytes ")
            _, total_str = range_header.split("/")
            await self._checkpoints.set(checkpoint_key, int(total_str))
        else:
            await self._checkpoints.set(checkpoint_key, response.content.total_bytes)
