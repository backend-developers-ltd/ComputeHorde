import datetime
import enum
import json
from typing import Annotated

import bittensor
from pydantic import Field, field_serializer, BaseModel

from compute_horde.executor_class import ExecutorClass
from compute_horde.utils import _json_dumps_default, empty_string_none


class ReceiptType(enum.Enum):
    JobStartedReceipt = "JobStartedReceipt"
    JobStillRunningReceipt = "JobStillRunningReceipt"
    JobFinishedReceipt = "JobFinishedReceipt"


class BaseReceiptPayload(BaseModel):
    receipt_type: ReceiptType
    job_uuid: str
    miner_hotkey: str
    validator_hotkey: str
    timestamp: datetime.datetime  # when the receipt was generated

    @field_serializer("timestamp")
    def serialize_timestamp(self, dt: datetime.datetime, _info):
        return dt.isoformat()

    def blob_for_signing(self):
        # pydantic v2 does not support sort_keys anymore.
        return json.dumps(self.model_dump(), sort_keys=True, default=_json_dumps_default)


class JobStartedReceiptPayload(BaseReceiptPayload):
    receipt_type: ReceiptType = ReceiptType.JobStartedReceipt
    executor_class: ExecutorClass
    time_accepted: Annotated[datetime.datetime | None, empty_string_none]
    max_timeout: int  # seconds
    ttl: Annotated[int | None, empty_string_none] = None  # seconds

    @field_serializer("time_accepted", when_used="unless-none")
    def serialize_time_accepted(self, dt: datetime.datetime, _info):
        return dt.isoformat()


class JobStillRunningReceiptPayload(BaseReceiptPayload):
    receipt_type: ReceiptType = ReceiptType.JobStillRunningReceipt


class JobFinishedReceiptPayload(BaseReceiptPayload):
    receipt_type: ReceiptType = ReceiptType.JobFinishedReceipt
    time_started: datetime.datetime
    time_took_us: int  # micro-seconds
    score_str: str

    @property
    def time_took(self):
        return datetime.timedelta(microseconds=self.time_took_us)

    @property
    def score(self):
        return float(self.score_str)

    @field_serializer("time_started")
    def serialize_time_started(self, dt: datetime.datetime, _info):
        return dt.isoformat()


ReceiptPayload = Annotated[
    JobStartedReceiptPayload | JobFinishedReceiptPayload | JobStillRunningReceiptPayload,
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
