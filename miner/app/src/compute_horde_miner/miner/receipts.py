import logging

from asgiref.sync import async_to_sync
from compute_horde.receipts.store.base import BaseReceiptStore
from compute_horde.receipts.store.local import LocalFilesystemPagedReceiptStore
from compute_horde.receipts.store.noop import NoopReceiptStore

from compute_horde_miner.miner.dynamic_config import aget_config

logger = logging.getLogger(__name__)


@async_to_sync
async def current_store() -> BaseReceiptStore:
    try:
        if await aget_config("DYNAMIC_RECEIPT_TRANSFER_ENABLED"):
            return LocalFilesystemPagedReceiptStore()
    except KeyError:
        logger.warning("DYNAMIC_RECEIPT_TRANSFER_ENABLED dynamic config is not set up!")

    return NoopReceiptStore()
