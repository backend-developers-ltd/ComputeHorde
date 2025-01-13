import datetime
from collections.abc import Sequence

from compute_horde.receipts import Receipt
from compute_horde.receipts.store.base import BaseReceiptStore


class NoopReceiptStore(BaseReceiptStore):
    def store(self, receipts: Sequence[Receipt]) -> None:
        pass

    def evict(self, cutoff: datetime.datetime) -> None:
        pass
