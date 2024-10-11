import abc

from compute_horde.receipts.pydantic import Receipt


class BaseReceiptStore(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def store(self, receipts: list[Receipt]) -> None: ...
