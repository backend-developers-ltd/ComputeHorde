from datetime import UTC, datetime, timedelta

import pytest
from compute_horde.receipts import schemas
from compute_horde.utils import ValidatorInfo
from compute_horde_core.executor_class import ExecutorClass

from compute_horde_validator.validator import job_excuses


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


@pytest.mark.asyncio
async def test_filter_valid_excuse_receipts_valid():
    now = datetime.now(UTC)
    miner = "miner_1"
    validator = "validator_1"
    executor_class = ExecutorClass.always_on__llm__a6000

    receipt = schemas.Receipt(
        payload=schemas.JobStartedReceiptPayload(
            job_uuid="job-uuid-1",
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

    valid_receipts = await job_excuses.filter_valid_excuse_receipts(
        receipts_to_check=[receipt],
        check_time=now,
        declined_job_uuid="declined",
        declined_job_executor_class=executor_class,
        declined_job_is_synthetic=True,
        miner_hotkey=miner,
        minimum_validator_stake_for_excuse=5.0,
        active_validators=[ValidatorInfo(uid=1, hotkey=validator, stake=10.0)],
    )

    assert [receipt.payload.job_uuid for receipt in valid_receipts] == [receipt.payload.job_uuid]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "case_name, make_receipts, expect_count",
    [
        (
            "not_a_job_started_receipt",
            lambda now, miner, validator, executor_class: [
                schemas.Receipt(
                    payload=schemas.JobAcceptedReceiptPayload(
                        job_uuid="job-accepted-uuid-1",
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
            0,
        ),
        (
            "is_organic_check_failed",
            lambda now, miner, validator, executor_class: [
                schemas.Receipt(
                    payload=schemas.JobStartedReceiptPayload(
                        job_uuid="job-synthetic-uuid-2",
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
            0,
        ),
        (
            "miner_hotkey_mismatch",
            lambda now, miner, validator, executor_class: [
                schemas.Receipt(
                    payload=schemas.JobStartedReceiptPayload(
                        job_uuid="job-from-other-miner-uuid-3",
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
            0,
        ),
        (
            "same_job_uuid_as_declined",
            lambda now, miner, validator, executor_class: [
                schemas.Receipt(
                    payload=schemas.JobStartedReceiptPayload(
                        job_uuid="declined",
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
            0,
        ),
        (
            "validator_not_allowed",
            lambda now, miner, validator, executor_class: [
                schemas.Receipt(
                    payload=schemas.JobStartedReceiptPayload(
                        job_uuid="job-from-other-validator-uuid-4",
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
            0,
        ),
        (
            "executor_class_mismatch",
            lambda now, miner, validator, executor_class: [
                schemas.Receipt(
                    payload=schemas.JobStartedReceiptPayload(
                        job_uuid="job-from-other-executor-class-uuid-5",
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
            0,
        ),
        (
            "timestamp_too_new",
            lambda now, miner, validator, executor_class: [
                schemas.Receipt(
                    payload=schemas.JobStartedReceiptPayload(
                        job_uuid="job-from-future-uuid-6",
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
            0,
        ),
        (
            "receipt_expired_with_leeway",
            lambda now, miner, validator, executor_class: [
                schemas.Receipt(
                    payload=schemas.JobStartedReceiptPayload(
                        job_uuid="job-expired-uuid-7",
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
            0,
        ),
        (
            "validator_signature_invalid",
            lambda now, miner, validator, executor_class: [
                schemas.Receipt(
                    payload=schemas.JobStartedReceiptPayload(
                        job_uuid="job-from-validator-with-invalid-signature-uuid-8",
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
            0,
        ),
        (
            "duplicate_receipts",
            lambda now, miner, validator, executor_class: [
                schemas.Receipt(
                    payload=schemas.JobStartedReceiptPayload(
                        job_uuid="job-with-duplicated-uuid-9",
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
                        job_uuid="job-with-duplicated-uuid-9",
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
            1,
        ),
    ],
)
async def test_filter_valid_excuse_receipts_failures(case_name, make_receipts, expect_count):
    now = datetime.now(UTC)
    miner = "miner_1"
    validator = "validator_1"
    executor_class = ExecutorClass.always_on__llm__a6000

    receipts = make_receipts(now, miner, validator, executor_class)

    active_validators = [ValidatorInfo(uid=1, hotkey=validator, stake=10.0)]
    if case_name == "validator_not_allowed":
        active_validators = []

    res = await job_excuses.filter_valid_excuse_receipts(
        receipts_to_check=receipts,
        check_time=now,
        declined_job_uuid="declined",
        declined_job_executor_class=executor_class,
        declined_job_is_synthetic=True,
        miner_hotkey=miner,
        minimum_validator_stake_for_excuse=5.0,
        active_validators=active_validators,
    )

    assert len(res) == expect_count, (
        f"case {case_name} expected {expect_count} valid receipts, got {len(res)}"
    )
