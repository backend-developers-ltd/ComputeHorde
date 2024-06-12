import abc

from compute_horde.receipts import Receipt


class BaseReceiptStore(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def store(self, receipts: list[Receipt]) -> None: ...
