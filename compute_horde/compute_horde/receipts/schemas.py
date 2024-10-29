import datetime
import enum
import json
from typing import Annotated, Literal

import bittensor
from pydantic import BaseModel, Field

from compute_horde.executor_class import ExecutorClass


class ReceiptType(enum.StrEnum):
    JobStartedReceipt = "JobStartedReceipt"
    JobAcceptedReceipt = "JobAcceptedReceipt"
    JobFinishedReceipt = "JobFinishedReceipt"


class BaseReceiptPayload(BaseModel):
    job_uuid: str
    miner_hotkey: str
    validator_hotkey: str
    timestamp: datetime.datetime  # when the receipt was generated

    def blob_for_signing(self):
        # pydantic v2 does not support sort_keys anymore.
        return json.dumps(self.model_dump(mode="json"), sort_keys=True)


class JobStartedReceiptPayload(BaseReceiptPayload):
    receipt_type: Literal[ReceiptType.JobStartedReceipt] = ReceiptType.JobStartedReceipt
    executor_class: ExecutorClass
    max_timeout: int  # seconds
    is_organic: bool
    ttl: int


class JobAcceptedReceiptPayload(BaseReceiptPayload):
    receipt_type: Literal[ReceiptType.JobAcceptedReceipt] = ReceiptType.JobAcceptedReceipt
    time_accepted: datetime.datetime
    ttl: int


class JobFinishedReceiptPayload(BaseReceiptPayload):
    receipt_type: Literal[ReceiptType.JobFinishedReceipt] = ReceiptType.JobFinishedReceipt
    time_started: datetime.datetime
    time_took_us: int  # micro-seconds
    score_str: str

    @property
    def time_took(self):
        return datetime.timedelta(microseconds=self.time_took_us)

    @property
    def score(self):
        return float(self.score_str)


ReceiptPayload = Annotated[
    JobStartedReceiptPayload | JobAcceptedReceiptPayload | JobFinishedReceiptPayload,
    Field(discriminator="receipt_type"),
]


class Receipt(BaseModel):
    payload: ReceiptPayload
    validator_signature: str
    miner_signature: str

    def verify_miner_signature(self):
        miner_keypair = bittensor.Keypair(ss58_address=self.payload.miner_hotkey)
        return miner_keypair.verify(self.payload.blob_for_signing(), self.miner_signature)

    def verify_validator_signature(self):
        validator_keypair = bittensor.Keypair(ss58_address=self.payload.validator_hotkey)
        return validator_keypair.verify(self.payload.blob_for_signing(), self.validator_signature)
