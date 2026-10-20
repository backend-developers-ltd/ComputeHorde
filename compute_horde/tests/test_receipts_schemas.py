import datetime

import pytest
from compute_horde_core.executor_class import ExecutorClass
from pydantic import ValidationError

from compute_horde.receipts.schemas import (
    MAX_BLOCK_NUMBER,
    MAX_BLOCK_NUMBERS,
    MAX_SCORE_STR_LENGTH,
    MAX_TIME_TOOK_US,
    MAX_TTL_SECONDS,
    BadMinerReceiptSignature,
    BadValidatorReceiptSignature,
    JobAcceptedReceiptPayload,
    JobFinishedReceiptPayload,
    JobStartedReceiptPayload,
    Receipt,
    parse_score_str,
)
from compute_horde.utils import sign_blob
from tests.utils import random_keypair

JOB_UUID = "3342460e-4a99-438b-8757-795f4cb348dd"
MINER_HOTKEY = "5FHneW46xGXgs5mUiveU4sbTyGBzmstUspZC92UhjJM694ty"
VALIDATOR_HOTKEY = "5HGjWAeFDfFCWPsjFQdVV2Msvz2XtMktvgocEZcCj68kUMaw"
TIMESTAMP = datetime.datetime(2025, 3, 4, 5, 6, 7, 123456, tzinfo=datetime.UTC)

COMMON_FIELDS = {
    "job_uuid": JOB_UUID,
    "miner_hotkey": MINER_HOTKEY,
    "validator_hotkey": VALIDATOR_HOTKEY,
    "timestamp": TIMESTAMP,
}
STARTED_FIELDS = {
    **COMMON_FIELDS,
    "executor_class": ExecutorClass.always_on__gpu_24gb,
    "is_organic": True,
    "ttl": 300,
}
ACCEPTED_FIELDS = {
    **COMMON_FIELDS,
    "time_accepted": datetime.datetime(2025, 3, 4, 5, 6, 8, tzinfo=datetime.UTC),
    "ttl": 123,
}
FINISHED_FIELDS = {
    **COMMON_FIELDS,
    "time_started": datetime.datetime(2025, 3, 4, 5, 0, 0, tzinfo=datetime.UTC),
    "time_took_us": 367_000_000,
    "score_str": "1337.000000",
    "block_numbers": [100, 101],
}

# Blobs as produced before field validation was added. Signatures already made across the network
# are over these exact bytes, so they must not change.
STARTED_BLOB = (
    '{"executor_class": "always_on.gpu-24gb", "is_organic": true, '
    '"job_uuid": "3342460e-4a99-438b-8757-795f4cb348dd", '
    '"miner_hotkey": "5FHneW46xGXgs5mUiveU4sbTyGBzmstUspZC92UhjJM694ty", '
    '"receipt_type": "JobStartedReceipt", "timestamp": "2025-03-04T05:06:07.123456Z", '
    '"ttl": 300, "validator_hotkey": "5HGjWAeFDfFCWPsjFQdVV2Msvz2XtMktvgocEZcCj68kUMaw"}'
)
ACCEPTED_BLOB = (
    '{"job_uuid": "3342460e-4a99-438b-8757-795f4cb348dd", '
    '"miner_hotkey": "5FHneW46xGXgs5mUiveU4sbTyGBzmstUspZC92UhjJM694ty", '
    '"receipt_type": "JobAcceptedReceipt", "time_accepted": "2025-03-04T05:06:08Z", '
    '"timestamp": "2025-03-04T05:06:07.123456Z", "ttl": 123, '
    '"validator_hotkey": "5HGjWAeFDfFCWPsjFQdVV2Msvz2XtMktvgocEZcCj68kUMaw"}'
)
FINISHED_BLOB = (
    '{"block_numbers": [100, 101], "job_uuid": "3342460e-4a99-438b-8757-795f4cb348dd", '
    '"miner_hotkey": "5FHneW46xGXgs5mUiveU4sbTyGBzmstUspZC92UhjJM694ty", '
    '"receipt_type": "JobFinishedReceipt", "score_str": "1337.000000", '
    '"time_started": "2025-03-04T05:00:00Z", "time_took_us": 367000000, '
    '"timestamp": "2025-03-04T05:06:07.123456Z", '
    '"validator_hotkey": "5HGjWAeFDfFCWPsjFQdVV2Msvz2XtMktvgocEZcCj68kUMaw"}'
)


@pytest.mark.parametrize(
    "payload_cls,fields,expected_blob",
    [
        (JobStartedReceiptPayload, STARTED_FIELDS, STARTED_BLOB),
        (JobAcceptedReceiptPayload, ACCEPTED_FIELDS, ACCEPTED_BLOB),
        (JobFinishedReceiptPayload, FINISHED_FIELDS, FINISHED_BLOB),
    ],
)
def test_blob_for_signing_is_unchanged(payload_cls, fields, expected_blob):
    assert payload_cls(**fields).blob_for_signing() == expected_blob


@pytest.mark.parametrize(
    "payload_cls,fields,overrides",
    [
        (JobFinishedReceiptPayload, FINISHED_FIELDS, {"job_uuid": "not-a-uuid"}),
        (JobFinishedReceiptPayload, FINISHED_FIELDS, {"job_uuid": ""}),
        (JobFinishedReceiptPayload, FINISHED_FIELDS, {"job_uuid": JOB_UUID.upper()}),
        (JobFinishedReceiptPayload, FINISHED_FIELDS, {"job_uuid": JOB_UUID.replace("-", "")}),
        (JobFinishedReceiptPayload, FINISHED_FIELDS, {"job_uuid": "{" + JOB_UUID + "}"}),
        (JobFinishedReceiptPayload, FINISHED_FIELDS, {"score_str": "abc"}),
        (JobFinishedReceiptPayload, FINISHED_FIELDS, {"score_str": "nan"}),
        (JobFinishedReceiptPayload, FINISHED_FIELDS, {"score_str": "inf"}),
        (JobFinishedReceiptPayload, FINISHED_FIELDS, {"score_str": "-1"}),
        (JobFinishedReceiptPayload, FINISHED_FIELDS, {"score_str": "+1"}),
        (JobFinishedReceiptPayload, FINISHED_FIELDS, {"score_str": "1e5"}),
        (JobFinishedReceiptPayload, FINISHED_FIELDS, {"score_str": "1_000"}),
        (JobFinishedReceiptPayload, FINISHED_FIELDS, {"score_str": "0x10"}),
        (JobFinishedReceiptPayload, FINISHED_FIELDS, {"score_str": ""}),
        (JobFinishedReceiptPayload, FINISHED_FIELDS, {"score_str": " 1"}),
        (JobFinishedReceiptPayload, FINISHED_FIELDS, {"score_str": "1\n"}),
        (JobFinishedReceiptPayload, FINISHED_FIELDS, {"score_str": "1."}),
        (JobFinishedReceiptPayload, FINISHED_FIELDS, {"score_str": ".5"}),
        (JobFinishedReceiptPayload, FINISHED_FIELDS, {"score_str": "١"}),
        (
            JobFinishedReceiptPayload,
            FINISHED_FIELDS,
            {"score_str": "9" * (MAX_SCORE_STR_LENGTH + 1)},
        ),
        (JobFinishedReceiptPayload, FINISHED_FIELDS, {"time_took_us": -1}),
        (JobFinishedReceiptPayload, FINISHED_FIELDS, {"time_took_us": MAX_TIME_TOOK_US + 1}),
        (JobFinishedReceiptPayload, FINISHED_FIELDS, {"time_took_us": 2**70}),
        (JobFinishedReceiptPayload, FINISHED_FIELDS, {"block_numbers": [-1]}),
        (JobFinishedReceiptPayload, FINISHED_FIELDS, {"block_numbers": [MAX_BLOCK_NUMBER + 1]}),
        (JobFinishedReceiptPayload, FINISHED_FIELDS, {"block_numbers": [[1]]}),
        (JobFinishedReceiptPayload, FINISHED_FIELDS, {"block_numbers": ["a"]}),
        (
            JobFinishedReceiptPayload,
            FINISHED_FIELDS,
            {"block_numbers": [1] * (MAX_BLOCK_NUMBERS + 1)},
        ),
        (
            JobFinishedReceiptPayload,
            FINISHED_FIELDS,
            {"timestamp": datetime.datetime(1999, 12, 31, 23, 59, 59, tzinfo=datetime.UTC)},
        ),
        (
            JobFinishedReceiptPayload,
            FINISHED_FIELDS,
            {"timestamp": datetime.datetime(2100, 1, 1, tzinfo=datetime.UTC)},
        ),
        (
            JobFinishedReceiptPayload,
            FINISHED_FIELDS,
            {"time_started": datetime.datetime(9999, 12, 31, tzinfo=datetime.UTC)},
        ),
        (JobFinishedReceiptPayload, FINISHED_FIELDS, {"timestamp": datetime.datetime(2025, 3, 4)}),
        (
            JobFinishedReceiptPayload,
            FINISHED_FIELDS,
            {"time_started": datetime.datetime(2025, 3, 4)},
        ),
        (JobStartedReceiptPayload, STARTED_FIELDS, {"ttl": -1}),
        (JobStartedReceiptPayload, STARTED_FIELDS, {"ttl": MAX_TTL_SECONDS + 1}),
        (JobStartedReceiptPayload, STARTED_FIELDS, {"ttl": 2**40}),
        (JobStartedReceiptPayload, STARTED_FIELDS, {"job_uuid": "not-a-uuid"}),
        (JobAcceptedReceiptPayload, ACCEPTED_FIELDS, {"ttl": -1}),
        (JobAcceptedReceiptPayload, ACCEPTED_FIELDS, {"ttl": MAX_TTL_SECONDS + 1}),
        (
            JobAcceptedReceiptPayload,
            ACCEPTED_FIELDS,
            {"time_accepted": datetime.datetime(1, 1, 1, tzinfo=datetime.UTC)},
        ),
    ],
    ids=lambda value: repr(value)[:60] if isinstance(value, dict) else None,
)
def test_rejects_out_of_bounds_field(payload_cls, fields, overrides):
    with pytest.raises(ValidationError):
        payload_cls(**{**fields, **overrides})


@pytest.mark.parametrize(
    "payload_cls,fields,overrides",
    [
        (JobFinishedReceiptPayload, FINISHED_FIELDS, {"score_str": "0"}),
        (JobFinishedReceiptPayload, FINISHED_FIELDS, {"score_str": "0.5"}),
        (JobFinishedReceiptPayload, FINISHED_FIELDS, {"score_str": "007.250"}),
        (JobFinishedReceiptPayload, FINISHED_FIELDS, {"score_str": "9" * MAX_SCORE_STR_LENGTH}),
        (JobFinishedReceiptPayload, FINISHED_FIELDS, {"time_took_us": 0}),
        (JobFinishedReceiptPayload, FINISHED_FIELDS, {"time_took_us": MAX_TIME_TOOK_US}),
        (JobFinishedReceiptPayload, FINISHED_FIELDS, {"block_numbers": []}),
        (
            JobFinishedReceiptPayload,
            FINISHED_FIELDS,
            {"block_numbers": [MAX_BLOCK_NUMBER] * MAX_BLOCK_NUMBERS},
        ),
        (
            JobFinishedReceiptPayload,
            FINISHED_FIELDS,
            {"timestamp": datetime.datetime(2000, 1, 1, tzinfo=datetime.UTC)},
        ),
        (
            JobFinishedReceiptPayload,
            FINISHED_FIELDS,
            {"timestamp": datetime.datetime(2099, 12, 31, 23, 59, 59, tzinfo=datetime.UTC)},
        ),
        (JobStartedReceiptPayload, STARTED_FIELDS, {"ttl": 0}),
        (JobStartedReceiptPayload, STARTED_FIELDS, {"ttl": MAX_TTL_SECONDS}),
        (JobAcceptedReceiptPayload, ACCEPTED_FIELDS, {"ttl": MAX_TTL_SECONDS}),
    ],
    ids=lambda value: repr(value)[:60] if isinstance(value, dict) else None,
)
def test_accepts_boundary_field(payload_cls, fields, overrides):
    payload = payload_cls(**{**fields, **overrides})
    assert payload.model_dump() == {**fields, **overrides, "receipt_type": payload.receipt_type}


@pytest.mark.parametrize(
    "score_str,expected",
    [("0", 0.0), ("0.5", 0.5), ("1337.000000", 1337.0), ("007", 7.0)],
)
def test_parse_score_str(score_str, expected):
    assert parse_score_str(score_str) == expected


@pytest.mark.parametrize("score_str", ["nan", "inf", "-1", "abc", "1e5", ""])
def test_parse_score_str_rejects_what_the_wire_format_rejects(score_str):
    with pytest.raises(ValueError):
        parse_score_str(score_str)


def test_signed_receipt_survives_json_round_trip():
    miner_keypair = random_keypair()
    validator_keypair = random_keypair()
    payload = JobFinishedReceiptPayload(
        **{
            **FINISHED_FIELDS,
            "miner_hotkey": miner_keypair.ss58_address,
            "validator_hotkey": validator_keypair.ss58_address,
        }
    )
    blob = payload.blob_for_signing()
    receipt = Receipt(
        payload=payload,
        miner_signature=sign_blob(miner_keypair, blob),
        validator_signature=sign_blob(validator_keypair, blob),
    )

    received = Receipt.model_validate_json(receipt.model_dump_json())

    assert received == receipt
    assert received.verify_miner_signature(throw=True)
    assert received.verify_validator_signature(throw=True)


def test_undecodable_hotkeys_count_as_bad_signatures():
    payload = JobStartedReceiptPayload(
        **{**STARTED_FIELDS, "miner_hotkey": "not-an-address", "validator_hotkey": "x" * 300}
    )
    receipt = Receipt(payload=payload, miner_signature="0x00", validator_signature="not-hex")

    assert receipt.verify_miner_signature() is False
    assert receipt.verify_validator_signature() is False
    with pytest.raises(BadMinerReceiptSignature):
        receipt.verify_miner_signature(throw=True)
    with pytest.raises(BadValidatorReceiptSignature):
        receipt.verify_validator_signature(throw=True)
