import datetime

import pytest
import responses
from bittensor import Keypair

from compute_horde.mv_protocol.validator_requests import ReceiptPayload
from compute_horde.receipts import Receipt


@pytest.fixture
def keypair():
    return Keypair.create_from_mnemonic(
        "edit evoke caught tunnel harsh plug august group enact cable govern immense"
    )


@pytest.fixture
def validator_keypair():
    return Keypair.create_from_mnemonic(
        "slot excuse valid grief praise rifle spoil auction weasel glove pen share"
    )


@pytest.fixture
def miner_keypair():
    return Keypair.create_from_mnemonic(
        "almost fatigue race slim picnic mass better clog deal solve already champion"
    )


@pytest.fixture
def receipts(validator_keypair, miner_keypair):
    payload1 = ReceiptPayload(
        job_uuid="0d89161e-65e4-46ad-bed8-ecfec1cc3c6b",
        miner_hotkey=miner_keypair.ss58_address,
        validator_hotkey=validator_keypair.ss58_address,
        time_started=datetime.datetime(2024, 1, 1, tzinfo=datetime.UTC),
        time_took_us=1_000_000,
        score_str="1.00",
    )
    receipt1 = Receipt(
        payload=payload1,
        validator_signature=f"0x{validator_keypair.sign(payload1.blob_for_signing()).hex()}",
        miner_signature=f"0x{miner_keypair.sign(payload1.blob_for_signing()).hex()}",
    )

    payload2 = ReceiptPayload(
        job_uuid="3342460e-4a99-438b-8757-795f4cb348dd",
        miner_hotkey=miner_keypair.ss58_address,
        validator_hotkey=validator_keypair.ss58_address,
        time_started=datetime.datetime(2024, 1, 2, tzinfo=datetime.UTC),
        time_took_us=2_000_000,
        score_str="2.00",
    )
    receipt2 = Receipt(
        payload=payload2,
        validator_signature=f"0x{validator_keypair.sign(payload2.blob_for_signing()).hex()}",
        miner_signature=f"0x{miner_keypair.sign(payload2.blob_for_signing()).hex()}",
    )

    return [receipt1, receipt2]


@pytest.fixture
def mocked_responses():
    with responses.RequestsMock() as rsps:
        yield rsps
