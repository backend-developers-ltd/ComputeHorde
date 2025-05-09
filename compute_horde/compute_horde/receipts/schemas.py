import datetime
import enum
import json
from typing import Annotated, Literal

import bittensor
from compute_horde_core.executor_class import ExecutorClass
from pydantic import BaseModel, Field


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
    is_organic: bool
    ttl: int


class JobAcceptedReceiptPayload(BaseReceiptPayload):
    receipt_type: Literal[ReceiptType.JobAcceptedReceipt] = ReceiptType.JobAcceptedReceipt
    time_accepted: datetime.datetime
    ttl: int  # unused


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


class BadReceiptSignature(Exception):
    def __init__(self, receipt: "Receipt"):
        self.receipt = receipt


class BadMinerReceiptSignature(BadReceiptSignature):
    pass


class BadValidatorReceiptSignature(BadReceiptSignature):
    pass


class Receipt(BaseModel):
    payload: ReceiptPayload
    validator_signature: str
    miner_signature: str

    def verify_miner_signature(self, throw=False):
        miner_keypair = bittensor.Keypair(ss58_address=self.payload.miner_hotkey)
        is_valid = miner_keypair.verify(self.payload.blob_for_signing(), self.miner_signature)
        if throw and not is_valid:
            raise BadMinerReceiptSignature(self)
        return is_valid

    def verify_validator_signature(self, throw=False):
        validator_keypair = bittensor.Keypair(ss58_address=self.payload.validator_hotkey)
        blob = self.payload.blob_for_signing()
        try:
            is_valid = validator_keypair.verify(blob, self.validator_signature)
        except Exception:
            is_valid = False
        if throw and not is_valid:
            raise BadValidatorReceiptSignature(self)
        return is_valid
