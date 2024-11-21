import logging
from collections import defaultdict
from collections.abc import AsyncIterable, MutableMapping, Sequence
from contextlib import AsyncExitStack, asynccontextmanager

import httpx
import pydantic
from more_itertools import chunked

from compute_horde.receipts.models import ReceiptModel, receipt_to_django_model
from compute_horde.receipts.schemas import (
    Receipt,
)

logger = logging.getLogger(__name__)


class ReceiptFetchError(Exception):
    pass


Offset = int
CheckpointBackend = MutableMapping[str, int]


class ReceiptsTransfer:
    """
    HTTP client for fetching receipts from a target based on an HTTP URL.
    Uses HTTP range requests to only retrieve receipts added since last run.
    """

    def __init__(self, server_url: str, checkpoint_backend: CheckpointBackend):
        self._receipts_url = server_url.rstrip("/")
        self._checkpoints = checkpoint_backend
        self._client: httpx.Client | None = None

    @asynccontextmanager
    async def session(self):
        """
        When running multiple subsequent requests to the same host within a session context, the underlying TCP
        connection will be reused, reducing TCP handshake roundtrips.
        """
        with httpx.AsyncClient() as client:
            self._client = client

    async def new_receipts(
        self,
        pages: Sequence[int],
        # TODO: signed_by_miner: bittensor.wallet,
    ) -> dict[type[ReceiptModel], list[ReceiptModel]]:
        """
        Fetch and store updated receipts by looking at last N pages.
        """

        # Retrieve the receipts the pages and group by receipt model type.
        receipts_by_type: defaultdict[type[ReceiptModel], list[ReceiptModel]] = defaultdict(list)
        for page in pages:
            async for receipt in self._receipts_on_page(page):
                model = receipt_to_django_model(receipt)
                receipts_by_type[model.__class__].append(model)

        # Bach insert in chunks.
        for receipt_model, receipts in receipts_by_type.items():
            for receipts_chunk in chunked(receipts, 1000):
                await receipt_model.objects.abulk_create(receipts_chunk, ignore_conflicts=True)

        return dict(receipts_by_type)

    async def _receipts_on_page(
        self, page: int, use_checkpoint: bool = True
    ) -> AsyncIterable[Receipt]:
        """
        Iterate over receipts present on given page of the remote server.
        If use_checkpoint=False, all receipts from the page will be returned.
        The checkpoint will be written in any case.
        """
        page_url = f"{self._receipts_url}/{page}.jsonl"

        checkpoint_key = page_url
        checkpoint = self._checkpoints[checkpoint_key] if use_checkpoint else 0
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

        async with AsyncExitStack() as request_context:
            # Reuse the client if we're within a session()
            client = self._client or await request_context.enter_async_context(httpx.AsyncClient())
            response = await client.get(page_url, headers=headers)

        if response.status_code not in {200, 206}:
            logger.warning(f"Request failed for page {page}: {response.status_code}")
            return

        for line in response.iter_lines():
            try:
                receipt = Receipt.model_validate_json(line)
            except pydantic.ValidationError:
                logger.warning(
                    f"skipping invalid line: {line[:1000]}{' (...)' if len(line) > 1000 else ''}"
                )
                continue
            yield receipt

        # Save the total page size so that next time we request the page we know what range to request.
        if use_range_request:
            # Range requests return a "content-range" header that contains full size of the file.
            # We ask the server not to gzip range requests - so this is the real size of the file.
            range_header = response.headers["content-range"]
            assert range_header.startswith("bytes ")
            _, total_str = range_header.split("/")
            self._checkpoints[checkpoint_key] = int(total_str)
        else:
            # This is the inflated size (uncompressed). The content length in the headers refers to compressed size.
            self._checkpoints[checkpoint_key] = len(response.content)
