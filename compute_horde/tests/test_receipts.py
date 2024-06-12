import io
import zipfile

import pydantic
import pytest

from compute_horde.receipts import ReceiptFetchError, get_miner_receipts


def receipts_helper(mocked_responses, receipts, miner_keypair):
    zip_buf = io.BytesIO()
    with zipfile.ZipFile(zip_buf, "w") as zip_file:
        for receipt in receipts:
            zip_file.writestr(receipt.payload.job_uuid + ".json", receipt.model_dump_json())

    zip_buf.seek(0)
    mocked_responses.get("http://127.0.0.1:8000/receipts/receipts.zip", body=zip_buf.read())
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
        got_receipt = [x for x in got_receipts if x.payload.job_uuid == receipt.payload.job_uuid][0]
        assert got_receipt == receipt


def test__get_miner_receipts__invalid_receipt_skipped(mocked_responses, receipts, miner_keypair):
    class InvalidReceiptPayload(pydantic.BaseModel):
        job_uuid: str

    class InvalidReceipt(pydantic.BaseModel):
        payload: InvalidReceiptPayload

    receipts[1] = InvalidReceipt(payload=InvalidReceiptPayload(job_uuid="invalid"))
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
    mocked_responses.get("http://127.0.0.1:8000/receipts/receipts.zip", status=404)
    with pytest.raises(ReceiptFetchError):
        get_miner_receipts(miner_keypair.ss58_address, "127.0.0.1", 8000)

    with pytest.raises(ReceiptFetchError):
        get_miner_receipts(miner_keypair.ss58_address, "127.0.0.1", 8001)
