import abc
from typing import Self

from compute_horde.mv_protocol.validator_requests import ReceiptPayload
from pydantic import BaseModel

from compute_horde_miner.miner.models import JobReceipt


class Receipt(BaseModel):
    payload: ReceiptPayload
    validator_signature: str
    miner_signature: str

    @classmethod
    def from_job_receipt(cls, jr: JobReceipt) -> Self:
        return cls(
            payload=ReceiptPayload(
                job_uuid=str(jr.job_uuid),
                miner_hotkey=jr.miner_hotkey,
                validator_hotkey=jr.validator_hotkey,
                time_started=jr.time_started,
                time_took=jr.time_took,
                score=jr.score,
            ),
            validator_signature=jr.validator_signature,
            miner_signature=jr.miner_signature,
        )

    def json(self, *args, **kwargs) -> str:
        kwargs.setdefault("sort_keys", True)
        return super().json(*args, **kwargs)


class BaseReceiptStore(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    async def store(self, receipts: list[Receipt]):
        ...

    @abc.abstractmethod
    async def get_url(self) -> str:
        ...
