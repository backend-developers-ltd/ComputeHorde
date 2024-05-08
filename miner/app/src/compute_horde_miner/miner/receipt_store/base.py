import abc
import datetime
from typing import Self

from django.conf import settings

from compute_horde.mv_protocol.validator_requests import V0ReceiptRequest, RequestType, ReceiptPayload
from compute_horde_miner.miner.models import JobReceipt


class Receipt(V0ReceiptRequest):
    miner_signature: str

    @classmethod
    def from_job_receipt(cls, jr: JobReceipt) -> Self:
        return cls(
            message_type=RequestType.V0ReceiptRequest,
            payload=ReceiptPayload(
                job_uuid=str(jr.job_uuid),
                miner_hotkey=settings.BITTENSOR_WALLET().hotkey.ss58_address,
                validator_hotkey=jr.validator_hotkey,
                time_started=jr.time_started,
                time_took=jr.time_took,
                score=jr.score,
            ),
            signature=jr.validator_signature,
            miner_signature=jr.miner_signature,
        )


class BaseReceiptStore(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    async def store(self, date: datetime.date, receipts: list[Receipt]):
        ...

    @abc.abstractmethod
    async def get_url(self, date: datetime.date) -> str:
        ...
