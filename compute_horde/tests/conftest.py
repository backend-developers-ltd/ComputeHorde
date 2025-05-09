import datetime

import bittensor
import pytest
import responses
from bittensor import Keypair

from compute_horde.executor_class import DEFAULT_EXECUTOR_CLASS
from compute_horde.receipts.schemas import (
    JobAcceptedReceiptPayload,
    JobFinishedReceiptPayload,
    JobStartedReceiptPayload,
    Receipt,
)
from compute_horde.utils import sign_blob


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
def signature_wallet():
    wallet = bittensor.wallet(name="test_signature")
    # workaround the overwrite flag
    wallet.regenerate_coldkey(seed="8" * 64, use_password=False, overwrite=True)
    wallet.regenerate_hotkey(seed="9" * 64, use_password=False, overwrite=True)
    return wallet


@pytest.fixture
def receipts(validator_keypair, miner_keypair):
    payload1 = JobStartedReceiptPayload(
        job_uuid="3342460e-4a99-438b-8757-795f4cb348dd",
        miner_hotkey=miner_keypair.ss58_address,
        validator_hotkey=validator_keypair.ss58_address,
        timestamp=datetime.datetime(2020, 1, 1, 0, 0, 0, tzinfo=datetime.UTC),
        executor_class=DEFAULT_EXECUTOR_CLASS,
        is_organic=False,
        ttl=30,
    )
    receipt1 = Receipt(
        payload=payload1,
        validator_signature=sign_blob(validator_keypair, payload1.blob_for_signing()),
        miner_signature=sign_blob(miner_keypair, payload1.blob_for_signing()),
    )

    payload2 = JobAcceptedReceiptPayload(
        job_uuid="3342460e-4a99-438b-8757-795f4cb348dd",
        miner_hotkey=miner_keypair.ss58_address,
        validator_hotkey=validator_keypair.ss58_address,
        timestamp=datetime.datetime(2020, 1, 1, 0, 5, 0, tzinfo=datetime.UTC),
        time_accepted=datetime.datetime(2020, 1, 1, 0, 4, 0, tzinfo=datetime.UTC),
        ttl=300,
    )
    receipt2 = Receipt(
        payload=payload2,
        validator_signature=sign_blob(validator_keypair, payload2.blob_for_signing()),
        miner_signature=sign_blob(miner_keypair, payload2.blob_for_signing()),
    )

    payload3 = JobFinishedReceiptPayload(
        job_uuid="3342460e-4a99-438b-8757-795f4cb348dd",
        miner_hotkey=miner_keypair.ss58_address,
        validator_hotkey=validator_keypair.ss58_address,
        timestamp=datetime.datetime(2020, 1, 1, 0, 10, 0, tzinfo=datetime.UTC),
        time_started=datetime.datetime(2020, 1, 1, 0, 9, 0, tzinfo=datetime.UTC),
        time_took_us=60_000_000,
        score_str="2.00",
    )
    receipt3 = Receipt(
        payload=payload3,
        validator_signature=sign_blob(validator_keypair, payload3.blob_for_signing()),
        miner_signature=sign_blob(miner_keypair, payload3.blob_for_signing()),
    )

    return [receipt1, receipt2, receipt3]


@pytest.fixture
def mocked_responses():
    with responses.RequestsMock() as rsps:
        yield rsps
