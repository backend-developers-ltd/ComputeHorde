import csv
import io

import pytest

from compute_horde.receipts import ReceiptFetchError, get_miner_receipts


def receipts_helper(mocked_responses, receipts, miner_keypair):
    buf = io.StringIO()
    csv_writer = csv.writer(buf)
    csv_writer.writerow(
        [
            "job_uuid",
            "miner_hotkey",
            "validator_hotkey",
            "time_started",
            "time_took_us",
            "score_str",
            "validator_signature",
            "miner_signature",
        ]
    )
    for receipt in receipts:
        csv_writer.writerow(
            [
                receipt.payload.job_uuid,
                receipt.payload.miner_hotkey,
                receipt.payload.validator_hotkey,
                receipt.payload.time_started.isoformat(),
                receipt.payload.time_took_us,
                receipt.payload.score_str,
                receipt.validator_signature,
                receipt.miner_signature,
            ]
        )

    buf.seek(0)
    mocked_responses.get("http://127.0.0.1:8000/receipts/receipts.csv", body=buf.read())
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
    """
    Populate all the fields of one csv row with "invalid" :D
    """

    class Mock:
        def __str__(self):
            return "invalid"

        isoformat = __str__

        def __getattr__(self, item):
            return Mock()

    receipts[1] = Mock()
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
