import importlib
from functools import cache

from django.conf import settings
from django.core.exceptions import ImproperlyConfigured

from compute_horde.receipts.store.base import BaseReceiptStore


@cache
def receipts_store() -> BaseReceiptStore:
    if not getattr(settings, "RECEIPT_STORE_CLASS_PATH", ""):
        raise ImproperlyConfigured("Required settings.py setting missing: RECEIPT_STORE_CLASS_PATH")
    class_path: str = settings.RECEIPT_STORE_CLASS_PATH  # type: ignore
    module_path, class_name = class_path.split(":", 1)
    target_module = importlib.import_module(module_path)
    klass: type[BaseReceiptStore] = getattr(target_module, class_name)
    return klass()
