from functools import cache

from compute_horde.receipts.store.base import BaseReceiptStore
from compute_horde.receipts.store.local import LocalFilesystemPagedReceiptStore


@cache
def receipt_store() -> BaseReceiptStore:
    return LocalFilesystemPagedReceiptStore()
