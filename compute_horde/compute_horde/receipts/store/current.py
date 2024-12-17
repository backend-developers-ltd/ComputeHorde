from django.conf import settings

from compute_horde.receipts.store.base import BaseReceiptStore
from compute_horde.receipts.store.local import LocalFilesystemPagedReceiptStore
from compute_horde.receipts.store.noop import NoopReceiptStore


def receipt_store() -> BaseReceiptStore:
    if getattr(settings, "DYNAMIC_RECEIPT_TRANSFER_ENABLED", False):
        return LocalFilesystemPagedReceiptStore()
    else:
        return NoopReceiptStore()
