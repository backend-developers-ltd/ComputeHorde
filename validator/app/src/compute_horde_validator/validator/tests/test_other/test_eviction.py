import datetime
import uuid

import pytest
from compute_horde.receipts.models import JobAcceptedReceipt, JobFinishedReceipt, JobStartedReceipt
from compute_horde_core.executor_class import ExecutorClass

from compute_horde_validator.validator.eviction import evict_malformed_receipts, evict_receipts


def _finished_receipt(score_str: str) -> JobFinishedReceipt:
    now = datetime.datetime.now(datetime.UTC)
    return JobFinishedReceipt(
        job_uuid=str(uuid.uuid4()),
        miner_hotkey="miner",
        validator_hotkey="validator",
        validator_signature="sig",
        timestamp=now,
        time_started=now,
        time_took_us=1,
        score_str=score_str,
    )


@pytest.mark.django_db
def test_evict_malformed_receipts_keeps_parsable_scores():
    JobFinishedReceipt.objects.bulk_create(
        [
            _finished_receipt(score_str)
            for score_str in ["0", "1.000000", "12.5", "nan", "inf", "abc", "-1", "1e5", "", " 1"]
        ]
    )

    evict_malformed_receipts()

    assert sorted(JobFinishedReceipt.objects.values_list("score_str", flat=True)) == [
        "0",
        "1.000000",
        "12.5",
    ]


def _receipts_dated(
    timestamp: datetime.datetime,
) -> list[JobStartedReceipt | JobAcceptedReceipt | JobFinishedReceipt]:
    common_fields = dict(
        job_uuid=str(uuid.uuid4()),
        miner_hotkey="miner",
        validator_hotkey="validator",
        validator_signature="sig",
        timestamp=timestamp,
    )
    return [
        JobStartedReceipt(
            **common_fields,
            executor_class=str(ExecutorClass.always_on__gpu_24gb),
            is_organic=True,
            ttl=60,
        ),
        JobAcceptedReceipt(**common_fields, time_accepted=timestamp, ttl=60),
        JobFinishedReceipt(**common_fields, time_started=timestamp, time_took_us=1, score_str="1"),
    ]


@pytest.mark.django_db
def test_evict_receipts_drops_rows_dated_outside_the_retention_window():
    now = datetime.datetime.now(datetime.UTC)
    kept = [now - datetime.timedelta(days=1), now + datetime.timedelta(minutes=10)]
    dropped = [now - datetime.timedelta(days=3), now + datetime.timedelta(days=3)]
    for timestamp in kept + dropped:
        for receipt in _receipts_dated(timestamp):
            receipt.save()

    evict_receipts()

    for receipt_model in (JobStartedReceipt, JobAcceptedReceipt, JobFinishedReceipt):
        assert sorted(receipt_model.objects.values_list("timestamp", flat=True)) == sorted(kept)
