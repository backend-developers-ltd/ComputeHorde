from datetime import UTC, datetime, timedelta

import pytest
from compute_horde.receipts import schemas
from compute_horde_core.executor_class import ExecutorClass

from compute_horde_validator.validator import job_excuses
from compute_horde_validator.validator.allowance.types import ValidatorModel


@pytest.fixture(autouse=True)
def patch_wallet_and_signatures(monkeypatch):
    class MockHotkey:
        ss58_address = "wallet_hotkey"

    class MockWallet:
        def get_hotkey(self):
            return MockHotkey()

    monkeypatch.setattr(
        job_excuses.settings, "BITTENSOR_WALLET", lambda: MockWallet(), raising=True
    )

    def mock_verify_validator_signature(self, *, throw: bool = False):  # noqa: ARG001
        return getattr(self, "validator_signature", "") == "valid"

    def mock_verify_miner_signature(self, *, throw: bool = False):  # noqa: ARG001
        return True

    monkeypatch.setattr(
        schemas.Receipt, "verify_validator_signature", mock_verify_validator_signature, raising=True
    )
    monkeypatch.setattr(
        schemas.Receipt, "verify_miner_signature", mock_verify_miner_signature, raising=True
    )


def test_filter_valid_excuse_receipts_valid():
    now = datetime.now(UTC)
    miner = "miner_1"
    validator = "validator_1"
    executor_class = ExecutorClass.always_on__llm__a6000

    receipt = schemas.Receipt(
        payload=schemas.JobStartedReceiptPayload(
            job_uuid="00000000-0000-0000-0000-000000000001",
            miner_hotkey=miner,
            validator_hotkey=validator,
            is_organic=True,
            executor_class=executor_class,
            timestamp=now - timedelta(seconds=1),
            ttl=10,
        ),
        validator_signature="valid",
        miner_signature="valid",
    )

    valid_receipts = job_excuses.filter_valid_excuse_receipts(
        receipts_to_check=[receipt],
        check_time=now,
        declined_job_uuid="00000000-0000-0000-0000-00000000000b",
        declined_job_executor_class=executor_class,
        declined_job_is_synthetic=True,
        miner_hotkey=miner,
        minimum_validator_stake_for_excuse=5.0,
        active_validators=[ValidatorModel(uid=1, hotkey=validator, effective_stake=10.0)],
    )

    assert [receipt.payload.job_uuid for receipt in valid_receipts] == [receipt.payload.job_uuid]


def test_filter_valid_excuse_receipts_with_duplicate_receipts():
    now = datetime.now(UTC)
    miner = "miner_1"
    validator = "validator_1"
    executor_class = ExecutorClass.always_on__llm__a6000

    receipts = [
        schemas.Receipt(
            payload=schemas.JobStartedReceiptPayload(
                job_uuid="00000000-0000-0000-0000-000000000002",
                miner_hotkey=miner,
                validator_hotkey=validator,
                is_organic=True,
                executor_class=executor_class,
                timestamp=now - timedelta(seconds=1),
                ttl=10,
            ),
            validator_signature="valid",
            miner_signature="valid",
        ),
        schemas.Receipt(
            payload=schemas.JobStartedReceiptPayload(
                job_uuid="00000000-0000-0000-0000-000000000002",
                miner_hotkey=miner,
                validator_hotkey=validator,
                is_organic=True,
                executor_class=executor_class,
                timestamp=now - timedelta(seconds=1),
                ttl=10,
            ),
            validator_signature="valid",
            miner_signature="valid",
        ),
    ]

    valid_receipts = job_excuses.filter_valid_excuse_receipts(
        receipts_to_check=receipts,
        check_time=now,
        declined_job_uuid="00000000-0000-0000-0000-00000000000b",
        declined_job_executor_class=executor_class,
        declined_job_is_synthetic=True,
        miner_hotkey=miner,
        minimum_validator_stake_for_excuse=5.0,
        active_validators=[ValidatorModel(uid=1, hotkey=validator, effective_stake=10.0)],
    )

    assert len(valid_receipts) == 1
    assert valid_receipts[0].payload.job_uuid == "00000000-0000-0000-0000-000000000002"


@pytest.mark.parametrize(
    "case_name, make_receipts",
    [
        (
            "not_a_job_started_receipt",
            lambda now, miner, validator, executor_class: [
                schemas.Receipt(
                    payload=schemas.JobAcceptedReceiptPayload(
                        job_uuid="00000000-0000-0000-0000-000000000003",
                        miner_hotkey=miner,
                        validator_hotkey=validator,
                        timestamp=now - timedelta(seconds=1),
                        time_accepted=now - timedelta(seconds=1),
                        ttl=10,
                    ),
                    validator_signature="valid",
                    miner_signature="valid",
                ),
            ],
        ),
        (
            "is_organic_check_failed",
            lambda now, miner, validator, executor_class: [
                schemas.Receipt(
                    payload=schemas.JobStartedReceiptPayload(
                        job_uuid="00000000-0000-0000-0000-000000000004",
                        miner_hotkey=miner,
                        validator_hotkey=validator,
                        is_organic=False,
                        executor_class=executor_class,
                        timestamp=now - timedelta(seconds=1),
                        ttl=10,
                    ),
                    validator_signature="valid",
                    miner_signature="valid",
                ),
            ],
        ),
        (
            "miner_hotkey_mismatch",
            lambda now, miner, validator, executor_class: [
                schemas.Receipt(
                    payload=schemas.JobStartedReceiptPayload(
                        job_uuid="00000000-0000-0000-0000-000000000005",
                        miner_hotkey="other_miner",
                        validator_hotkey=validator,
                        is_organic=True,
                        executor_class=executor_class,
                        timestamp=now - timedelta(seconds=1),
                        ttl=10,
                    ),
                    validator_signature="valid",
                    miner_signature="valid",
                ),
            ],
        ),
        (
            "same_job_uuid_as_declined",
            lambda now, miner, validator, executor_class: [
                schemas.Receipt(
                    payload=schemas.JobStartedReceiptPayload(
                        job_uuid="00000000-0000-0000-0000-00000000000b",
                        miner_hotkey=miner,
                        validator_hotkey=validator,
                        is_organic=True,
                        executor_class=executor_class,
                        timestamp=now - timedelta(seconds=1),
                        ttl=10,
                    ),
                    validator_signature="valid",
                    miner_signature="valid",
                ),
            ],
        ),
        (
            "validator_not_allowed",
            lambda now, miner, validator, executor_class: [
                schemas.Receipt(
                    payload=schemas.JobStartedReceiptPayload(
                        job_uuid="00000000-0000-0000-0000-000000000006",
                        miner_hotkey=miner,
                        validator_hotkey="validator_not_allowed",
                        is_organic=True,
                        executor_class=executor_class,
                        timestamp=now - timedelta(seconds=1),
                        ttl=10,
                    ),
                    validator_signature="valid",
                    miner_signature="valid",
                ),
            ],
        ),
        (
            "executor_class_mismatch",
            lambda now, miner, validator, executor_class: [
                schemas.Receipt(
                    payload=schemas.JobStartedReceiptPayload(
                        job_uuid="00000000-0000-0000-0000-000000000007",
                        miner_hotkey=miner,
                        validator_hotkey=validator,
                        is_organic=True,
                        executor_class=ExecutorClass.always_on__gpu_24gb,
                        timestamp=now - timedelta(seconds=1),
                        ttl=10,
                    ),
                    validator_signature="valid",
                    miner_signature="valid",
                ),
            ],
        ),
        (
            "timestamp_too_new",
            lambda now, miner, validator, executor_class: [
                schemas.Receipt(
                    payload=schemas.JobStartedReceiptPayload(
                        job_uuid="00000000-0000-0000-0000-000000000008",
                        miner_hotkey=miner,
                        validator_hotkey=validator,
                        is_organic=True,
                        executor_class=executor_class,
                        timestamp=now + timedelta(seconds=1),
                        ttl=10,
                    ),
                    validator_signature="valid",
                    miner_signature="valid",
                ),
            ],
        ),
        (
            "receipt_expired_with_leeway",
            lambda now, miner, validator, executor_class: [
                schemas.Receipt(
                    payload=schemas.JobStartedReceiptPayload(
                        job_uuid="00000000-0000-0000-0000-000000000009",
                        miner_hotkey=miner,
                        validator_hotkey=validator,
                        is_organic=True,
                        executor_class=executor_class,
                        timestamp=now - timedelta(seconds=13),
                        ttl=10,
                    ),
                    validator_signature="valid",
                    miner_signature="valid",
                ),
            ],
        ),
        (
            "validator_signature_invalid",
            lambda now, miner, validator, executor_class: [
                schemas.Receipt(
                    payload=schemas.JobStartedReceiptPayload(
                        job_uuid="00000000-0000-0000-0000-00000000000a",
                        miner_hotkey=miner,
                        validator_hotkey=validator,
                        is_organic=True,
                        executor_class=executor_class,
                        timestamp=now - timedelta(seconds=1),
                        ttl=10,
                    ),
                    validator_signature="invalid",
                    miner_signature="valid",
                ),
            ],
        ),
    ],
)
def test_filter_valid_excuse_receipts_failures(case_name, make_receipts):
    now = datetime.now(UTC)
    miner = "miner_1"
    validator = "validator_1"
    executor_class = ExecutorClass.always_on__llm__a6000

    receipts = make_receipts(now, miner, validator, executor_class)

    active_validators = [ValidatorModel(uid=1, hotkey=validator, effective_stake=10.0)]
    if case_name == "validator_not_allowed":
        active_validators = []

    res = job_excuses.filter_valid_excuse_receipts(
        receipts_to_check=receipts,
        check_time=now,
        declined_job_uuid="00000000-0000-0000-0000-00000000000b",
        declined_job_executor_class=executor_class,
        declined_job_is_synthetic=True,
        miner_hotkey=miner,
        minimum_validator_stake_for_excuse=5.0,
        active_validators=active_validators,
    )

    assert res == [], f"case {case_name} expected no valid receipts, got {res}"
