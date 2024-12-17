from compute_horde.receipts.store.base import BaseReceiptStore
from compute_horde.receipts.store.local import LocalFilesystemPagedReceiptStore


def current_store() -> BaseReceiptStore:
    return LocalFilesystemPagedReceiptStore()
