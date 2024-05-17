import importlib

from django.conf import settings

from compute_horde_miner.miner.receipt_store.base import BaseReceiptStore

module_path, class_name = settings.RECEIPT_STORE_CLASS_PATH.split(":", 1)
target_module = importlib.import_module(module_path)
klass = getattr(target_module, class_name)

receipts_store: BaseReceiptStore = klass()
