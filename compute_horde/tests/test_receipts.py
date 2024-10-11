import csv
import io

import pytest

from compute_horde.mv_protocol.validator_requests import (
    JobFinishedReceiptPayload,
    JobStartedReceiptPayload,
)
from compute_horde.receipts.schemas import (
    Receipt,
    ReceiptType,
)
from compute_horde.receipts.transfer import ReceiptFetchError, get_miner_receipts


def receipts_helper(mocked_responses, receipts: list[Receipt], miner_keypair):
    payload_fields = set()
    for payload_cls in [JobStartedReceiptPayload, JobFinishedReceiptPayload]:
        payload_fields |= set(payload_cls.model_fields.keys())

    buf = io.StringIO()
    csv_writer = csv.DictWriter(
        buf,
        [
            "type",
            "validator_signature",
            "miner_signature",
            *payload_fields,
        ],
    )
    csv_writer.writeheader()
    for receipt in receipts:
        match receipt.payload:
            case JobStartedReceiptPayload():
                receipt_type = ReceiptType.JobStartedReceipt
            case JobFinishedReceiptPayload():
                receipt_type = ReceiptType.JobFinishedReceipt
        row = (
            dict(
                type=receipt_type.value,
                validator_signature=receipt.validator_signature,
                miner_signature=receipt.miner_signature,
            )
            | receipt.payload.model_dump()
        )
        csv_writer.writerow(row)

    mocked_responses.get("http://127.0.0.1:8000/receipts/receipts.csv", body=buf.getvalue())
    return get_miner_receipts(miner_keypair.ss58_address, "127.0.0.1", 8000)


def receipts_one_skipped_helper(mocked_responses, receipts, miner_keypair):
    got_receipts = receipts_helper(mocked_responses, receipts, miner_keypair)
    # only the valid receipt should be stored
    assert len(got_receipts) == 1
    assert got_receipts[0] == receipts[0]


def test__get_miner_receipts__happy_path(mocked_responses, receipts, miner_keypair):
    got_receipts = receipts_helper(mocked_responses, receipts, miner_keypair)
    assert len(got_receipts) == 2
    for receipt in receipts:
        got_receipt = [
            x
            for x in got_receipts
            if x.payload.job_uuid == receipt.payload.job_uuid
            and x.payload.__class__ is receipt.payload.__class__
        ][0]
        assert got_receipt == receipt


@pytest.mark.filterwarnings("ignore:Pydantic serializer warnings")
def test__get_miner_receipts__invalid_receipt_skipped(mocked_responses, receipts, miner_keypair):
    """
    Invalidate one receipt payload fields to make it invalid
    """

    receipts[1].payload.miner_hotkey = 0
    receipts[1].payload.validator_hotkey = None
    receipts_one_skipped_helper(mocked_responses, receipts, miner_keypair)


def test__get_miner_receipts__miner_hotkey_mismatch_skipped(
    mocked_responses, receipts, miner_keypair, keypair
):
    receipts[1].payload.miner_hotkey = keypair.ss58_address
    receipts_one_skipped_helper(mocked_responses, receipts, miner_keypair)


def test__get_miner_receipts__invalid_miner_signature_skipped(
    mocked_responses, receipts, miner_keypair
):
    receipts[1].miner_signature = f"0x{miner_keypair.sign('bla').hex()}"
    receipts_one_skipped_helper(mocked_responses, receipts, miner_keypair)


def test__get_miner_receipts__invalid_validator_signature_skipped(
    mocked_responses, receipts, miner_keypair
):
    receipts[1].validator_signature = f"0x{miner_keypair.sign('bla').hex()}"
    receipts_one_skipped_helper(mocked_responses, receipts, miner_keypair)


def test__get_miner_receipts__no_receipts(mocked_responses, miner_keypair):
    mocked_responses.get("http://127.0.0.1:8000/receipts/receipts.csv", status=404)
    with pytest.raises(ReceiptFetchError):
        get_miner_receipts(miner_keypair.ss58_address, "127.0.0.1", 8000)

    with pytest.raises(ReceiptFetchError):
        get_miner_receipts(miner_keypair.ss58_address, "127.0.0.1", 8001)
