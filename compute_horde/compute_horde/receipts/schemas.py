import datetime
import enum
import json
import logging
from typing import Annotated

import bittensor
import pydantic
from pydantic import field_serializer

from compute_horde.executor_class import ExecutorClass
from compute_horde.utils import _json_dumps_default, empty_string_none

logger = logging.getLogger(__name__)


class ReceiptType(enum.Enum):
    JobStartedReceipt = "JobStartedReceipt"
    JobStillRunningReceipt = "JobStillRunningReceipt"
    JobFinishedReceipt = "JobFinishedReceipt"


class ReceiptPayload(pydantic.BaseModel):
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


class JobStartedReceiptPayload(ReceiptPayload):
    executor_class: ExecutorClass
    time_accepted: Annotated[datetime.datetime | None, empty_string_none]
    max_timeout: int  # seconds
    ttl: Annotated[int | None, empty_string_none] = None  # seconds

    @field_serializer("time_accepted", when_used="unless-none")
    def serialize_dt(self, dt: datetime.datetime, _info):
        return dt.isoformat()


class JobStillRunningReceiptPayload(ReceiptPayload):
    pass


class JobFinishedReceiptPayload(ReceiptPayload):
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
    def serialize_dt(self, dt: datetime.datetime, _info):
        return dt.isoformat()


class Receipt(pydantic.BaseModel):
    payload: JobStartedReceiptPayload | JobFinishedReceiptPayload | JobStillRunningReceiptPayload
    validator_signature: str
    miner_signature: str

    def verify_miner_signature(self):
        miner_keypair = bittensor.Keypair(ss58_address=self.payload.miner_hotkey)
        return miner_keypair.verify(self.payload.blob_for_signing(), self.miner_signature)

    def verify_validator_signature(self):
        validator_keypair = bittensor.Keypair(ss58_address=self.payload.validator_hotkey)
        return validator_keypair.verify(self.payload.blob_for_signing(), self.validator_signature)
