import uuid
from datetime import UTC, datetime, timedelta
from unittest.mock import MagicMock, patch

import pytest
from compute_horde_core.executor_class import ExecutorClass

from compute_horde_validator.validator.models import Block, BlockAllowance
from compute_horde_validator.validator.receipts.base import ReceiptsBase
from compute_horde_validator.validator.receipts.types import JobSpendingInfo
from compute_horde_validator.validator.scoring.calculations import (
    calculate_allowance_paid_job_scores,
)
from compute_horde_validator.validator.tests.helpers import patch_constance

pytestmark = [
    pytest.mark.django_db,
]

BLOCK_EXPIRY = 10
MAX_JOB_RUNTIME = 20
CYCLE_START = 40
CYCLE_END = 60

VALIDATOR = "validator"
MINER = "miner"
EXECUTOR_CLASS = ExecutorClass.always_on__gpu_24gb


def _block_time(block_number: int) -> datetime:
    return datetime(2025, 1, 1, 0, 0, 0, tzinfo=UTC) + timedelta(minutes=block_number)


def _spend_info(amount: int, blocks: list[int], job_start_block: int) -> JobSpendingInfo:
    return JobSpendingInfo(
        job_uuid=str(uuid.uuid4()),
        validator_hotkey=VALIDATOR,
        miner_hotkey=MINER,
        executor_class=EXECUTOR_CLASS,
        executor_seconds_cost=amount,
        paid_with_blocks=blocks,
        started_at=_block_time(job_start_block),
    )


@pytest.fixture(autouse=True)
def mock_settings():
    """
    Mocks the expiry and job runtime settings used by the validation
    """
    with (
        patch(
            "compute_horde_validator.validator.allowance.utils.spending.settings.BLOCK_EXPIRY",
            BLOCK_EXPIRY,
        ),
        patch(
            "compute_horde_validator.validator.allowance.utils.spending.settings.MAX_JOB_RUN_TIME_BLOCKS_APPROX",
            MAX_JOB_RUNTIME,
        ),
        patch_constance(
            {
                # TODO: This requires a proper investigation and fix rather than a band-aid.
                # For clarity of tests, assume there is no leeway.
                "DYNAMIC_SPENDING_VALIDATION_BLOCK_LEEWAY_LOWER": 0,
                "DYNAMIC_SPENDING_VALIDATION_BLOCK_LEEWAY_UPPER": 0,
            }
        ),
    ):
        yield


@pytest.fixture(autouse=True)
def mock_test_data():
    """
    Creates test data:
    - blocks 1-100, inclusive
    - allowances for the test miner, each block = 1 allowance
    """
    Block.objects.bulk_create(
        [
            Block(
                block_number=i, creation_timestamp=_block_time(i), end_timestamp=_block_time(i + 1)
            )
            for i in range(1, 101)
        ]
    )
    BlockAllowance.objects.bulk_create(
        [
            BlockAllowance(
                block_id=i,
                allowance=1,
                miner_ss58=MINER,
                validator_ss58=VALIDATOR,
                executor_class=ExecutorClass.always_on__gpu_24gb,
            )
            for i in range(1, 101)
        ]
    )


@pytest.mark.parametrize(
    "job_spendings,expected_scores",
    [
        (
            # Simple exact spend
            [
                _spend_info(amount=3, blocks=[40, 41, 42], job_start_block=50),
            ],
            3,
        ),
        (
            # Double spend
            # Only the first spend is accepted, the second spend is completely rejected
            [
                _spend_info(amount=3, blocks=[40, 41, 42], job_start_block=50),
                _spend_info(amount=3, blocks=[40, 41, 42], job_start_block=50),
            ],
            3,
        ),
        (
            # Overspending - job costs 1 but paid with a total of 3
            # All blocks consumed on first spend, further spends are rejected
            [
                _spend_info(amount=1, blocks=[40, 41, 42], job_start_block=50),
                _spend_info(amount=1, blocks=[40, 41, 42], job_start_block=50),
                _spend_info(amount=1, blocks=[40, 41, 42], job_start_block=50),
            ],
            1,
        ),
        (
            # Failed spend doesn't consume the blocks
            # First spend is rejected and, second spend has overlapping blocks but is accepted
            [
                _spend_info(amount=5, blocks=[40, 41, 42], job_start_block=50),
                _spend_info(amount=4, blocks=[41, 42, 43, 44], job_start_block=50),
            ],
            4,
        ),
        (
            # Underspending - not enough allowance from the blocks
            [
                _spend_info(amount=4, blocks=[40, 41, 42], job_start_block=50),
            ],
            None,
        ),
        (
            # Spent blocks that are barely within expiration range
            [
                _spend_info(amount=2, blocks=[50 - BLOCK_EXPIRY, 50], job_start_block=50),
            ],
            2,
        ),
        (
            # Spent a barely expired block
            [
                _spend_info(
                    amount=1,
                    blocks=[50 - BLOCK_EXPIRY - 1],
                    job_start_block=50,
                ),
            ],
            None,
        ),
        (
            # Spent a future block (in relation to the job)
            [
                _spend_info(
                    amount=1,
                    blocks=[50 + 1],
                    job_start_block=50,
                ),
            ],
            None,
        ),
        (
            # Youngest and shortest possible job passes the validation
            # (job started and finished during the last block of the cycle)
            # (for extra fun, cycle_end is exclusive)
            [
                _spend_info(
                    amount=1,
                    blocks=[CYCLE_END - 1],
                    job_start_block=CYCLE_END - 1,
                ),
            ],
            1,
        ),
        (
            # ... but not any younger
            # (spend is valid, but not within the scoring boundaries)
            [
                _spend_info(
                    amount=1,
                    blocks=[CYCLE_END],
                    job_start_block=CYCLE_END,
                ),
            ],
            None,
        ),
        (
            # Oldest possible job passes the validation
            # (longest possible job started as far as it could have in the past, using the oldest block possible)
            [
                _spend_info(
                    amount=2,
                    blocks=[
                        CYCLE_START - MAX_JOB_RUNTIME - BLOCK_EXPIRY,
                        CYCLE_START - MAX_JOB_RUNTIME,
                    ],
                    job_start_block=CYCLE_START - MAX_JOB_RUNTIME,
                ),
            ],
            2,
        ),
        (
            # ... but not any older
            [
                _spend_info(
                    amount=1,
                    blocks=[CYCLE_START - MAX_JOB_RUNTIME - BLOCK_EXPIRY - 1],
                    job_start_block=CYCLE_START - MAX_JOB_RUNTIME - BLOCK_EXPIRY - 1,
                ),
            ],
            None,
        ),
    ],
)
def test_paid_job_scores(job_spendings, expected_scores):
    """
    Mocking job spendings reported by the receipts,
    check if for simple and edge cases the scoring setup validates the spendings as expected.
    """

    def finished_jobs(_, __, executor_class, organic_only):
        return job_spendings if executor_class == ExecutorClass.always_on__gpu_24gb else []

    with patch(
        "compute_horde_validator.validator.receipts.default._receipts_instance",
        MagicMock(spec=ReceiptsBase),
    ) as receipts_module_mock:
        receipts_module_mock.get_finished_jobs_for_block_range.side_effect = finished_jobs
        scores = calculate_allowance_paid_job_scores(CYCLE_START, CYCLE_END)
        if expected_scores is None:
            # Detail of how the scores are calculated - no valid spendings means no score at all
            assert scores == {}
        else:
            assert scores == {EXECUTOR_CLASS: {MINER: expected_scores}}
