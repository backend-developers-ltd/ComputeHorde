import abc
from collections.abc import Sequence

from compute_horde.receipts.schemas import Receipt


class BaseReceiptStore(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def store(self, receipts: Sequence[Receipt]) -> None:
        """
        Append given receipts to the store.
        """
        ...
