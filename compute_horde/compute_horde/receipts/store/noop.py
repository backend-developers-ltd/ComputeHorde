from collections.abc import Sequence

from compute_horde.receipts import Receipt
from compute_horde.receipts.store.base import BaseReceiptStore


class NoopReceiptStore(BaseReceiptStore):
    """
    Used when the receipt transfer feature flag is off.
    """

    def store(self, receipts: Sequence[Receipt]) -> None:
        pass
