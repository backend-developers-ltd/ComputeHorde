import abc
import datetime
from collections.abc import Sequence

from compute_horde.receipts.schemas import Receipt


class BaseReceiptStore(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def store(self, receipts: Sequence[Receipt]) -> None:
        """
        Append receipts to the store.
        """
        ...

    @abc.abstractmethod
    def evict(self, cutoff: datetime.datetime) -> None:
        """
        Remove receipts (roughly) older than the cutoff
        """
        ...
