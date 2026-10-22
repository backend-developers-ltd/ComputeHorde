import datetime
import enum
import json
import re
import uuid
from typing import Annotated, Literal

import bittensor
from compute_horde_core.executor_class import ExecutorClass
from pydantic import AfterValidator, AwareDatetime, BaseModel, Field

# Receipts are signed by their issuers but transferred from miners, which can forge them for any
# hotkey they control. The bounds below keep every accepted receipt storable in the receipt tables
# and keep its arithmetic (timestamp + ttl, score sums) finite and in range.
MAX_SCORE_STR_LENGTH = 256
MAX_TTL_SECONDS = 30 * 24 * 60 * 60
MAX_TIME_TOOK_US = 365 * 24 * 60 * 60 * 1_000_000
MAX_BLOCK_NUMBER = 2**32
MAX_BLOCK_NUMBERS = 10_000
MIN_TIMESTAMP_YEAR = 2000
MAX_TIMESTAMP_YEAR = 2100

# Executor seconds as validators write them: an unsigned decimal, no exponent, no sign, no NaN.
# Usable both as a Python pattern and as a POSIX regex in database lookups.
SCORE_STR_PATTERN = re.compile(r"^[0-9]+(\.[0-9]+)?$")


class ReceiptType(enum.StrEnum):
    JobStartedReceipt = "JobStartedReceipt"
    JobAcceptedReceipt = "JobAcceptedReceipt"
    JobFinishedReceipt = "JobFinishedReceipt"


def parse_score_str(score_str: str) -> float:
    """
    Parse the executor-seconds cost carried by a job finished receipt.

    Raises ValueError for anything but an unsigned decimal of at most MAX_SCORE_STR_LENGTH
    characters, so the result is always finite and non-negative.
    """
    if len(score_str) > MAX_SCORE_STR_LENGTH or not SCORE_STR_PATTERN.fullmatch(score_str):
        raise ValueError(f"Malformed score_str: {score_str[:MAX_SCORE_STR_LENGTH]!r}")
    return float(score_str)


def _validate_score_str(value: str) -> str:
    parse_score_str(value)
    return value


def _validate_job_uuid(value: str) -> str:
    try:
        canonical = str(uuid.UUID(value))
    except ValueError:
        raise ValueError(f"job_uuid is not a UUID: {value[:64]!r}") from None
    if canonical != value:
        raise ValueError(f"job_uuid must be in canonical form ({canonical}), got {value!r}")
    return value


def _validate_timestamp(value: datetime.datetime) -> datetime.datetime:
    if not MIN_TIMESTAMP_YEAR <= value.year < MAX_TIMESTAMP_YEAR:
        raise ValueError(
            f"Timestamp year must be within [{MIN_TIMESTAMP_YEAR}, {MAX_TIMESTAMP_YEAR}), "
            f"got {value.isoformat()}"
        )
    return value


JobUuid = Annotated[str, AfterValidator(_validate_job_uuid)]
ScoreStr = Annotated[str, AfterValidator(_validate_score_str)]
ReceiptTimestamp = Annotated[AwareDatetime, AfterValidator(_validate_timestamp)]
TtlSeconds = Annotated[int, Field(ge=0, le=MAX_TTL_SECONDS)]
BlockNumber = Annotated[int, Field(ge=0, le=MAX_BLOCK_NUMBER)]


def clamp_ttl(ttl: int) -> int:
    """
    Clamp a ttl summed from job time limits to MAX_TTL_SECONDS, the largest that payloads accept.

    Validators and miners evict receipts two days after their timestamp, long before
    MAX_TTL_SECONDS runs out, so clamping does not change how long a receipt counts as valid.
    """
    return min(ttl, MAX_TTL_SECONDS)


class BaseReceiptPayload(BaseModel):
    job_uuid: JobUuid
    miner_hotkey: str
    validator_hotkey: str
    timestamp: ReceiptTimestamp  # when the receipt was generated

    def blob_for_signing(self):
        # pydantic v2 does not support sort_keys anymore.
        return json.dumps(self.model_dump(mode="json"), sort_keys=True)


class JobStartedReceiptPayload(BaseReceiptPayload):
    receipt_type: Literal[ReceiptType.JobStartedReceipt] = ReceiptType.JobStartedReceipt
    executor_class: ExecutorClass
    is_organic: bool
    ttl: TtlSeconds


class JobAcceptedReceiptPayload(BaseReceiptPayload):
    receipt_type: Literal[ReceiptType.JobAcceptedReceipt] = ReceiptType.JobAcceptedReceipt
    time_accepted: ReceiptTimestamp
    ttl: TtlSeconds


class JobFinishedReceiptPayload(BaseReceiptPayload):
    receipt_type: Literal[ReceiptType.JobFinishedReceipt] = ReceiptType.JobFinishedReceipt
    time_started: ReceiptTimestamp
    time_took_us: Annotated[int, Field(ge=0, le=MAX_TIME_TOOK_US)]  # micro-seconds
    score_str: ScoreStr
    block_numbers: list[BlockNumber] = Field(
        default_factory=list,
        max_length=MAX_BLOCK_NUMBERS,
        description="List of block numbers used to pay for this job",
    )

    @property
    def time_took(self):
        return datetime.timedelta(microseconds=self.time_took_us)

    @property
    def score(self):
        return parse_score_str(self.score_str)


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


def _signature_is_valid(hotkey: str, blob: str, signature: str) -> bool:
    try:
        return bool(bittensor.Keypair(ss58_address=hotkey).verify(blob, signature))
    except Exception:
        # A hotkey or signature that cannot even be decoded is as bad as a wrong one.
        return False


class Receipt(BaseModel):
    payload: ReceiptPayload
    validator_signature: str
    miner_signature: str

    def verify_miner_signature(self, throw=False):
        is_valid = _signature_is_valid(
            self.payload.miner_hotkey, self.payload.blob_for_signing(), self.miner_signature
        )
        if throw and not is_valid:
            raise BadMinerReceiptSignature(self)
        return is_valid

    def verify_validator_signature(self, throw=False):
        is_valid = _signature_is_valid(
            self.payload.validator_hotkey, self.payload.blob_for_signing(), self.validator_signature
        )
        if throw and not is_valid:
            raise BadValidatorReceiptSignature(self)
        return is_valid
