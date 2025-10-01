import itertools
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta
from unittest.mock import MagicMock, patch

import pytest
from compute_horde.executor_class import DEFAULT_EXECUTOR_CLASS
from django.conf import settings
from django.utils.timezone import now
from requests import Response

from compute_horde_validator.validator.models import (
    AdminJobRequest,
    Cycle,
    Miner,
    OrganicJob,
    SyntheticJobBatch,
    SystemEvent,
)
from compute_horde_validator.validator.tasks import (
    calculate_job_start_block,
    check_missed_synthetic_jobs,
    run_synthetic_jobs,
    send_events_to_facilitator,
    trigger_run_admin_job_request,
)

from ..helpers import (
    MockMinerClient,
    check_system_events,
)


def _get_cycle(block: int) -> Cycle:
    return Cycle.from_block(block, settings.BITTENSOR_NETUID)


@patch("compute_horde_validator.validator.tasks.MinerClient", MockMinerClient)
@pytest.mark.django_db(databases=["default", "default_alias"], transaction=True)
def test_trigger_run_admin_job__should_trigger_job(bittensor):
    miner = Miner.objects.create(hotkey="miner_client_1")
    OrganicJob.objects.all().delete()
    job_request = AdminJobRequest.objects.create(
        miner=miner,
        timeout=0,  # should timeout
        executor_class=DEFAULT_EXECUTOR_CLASS,
        docker_image="python:3.11-slim",
        args="",
    )

    assert AdminJobRequest.objects.count() == 1
    trigger_run_admin_job_request.apply(args=(job_request.pk,))

    job_request.refresh_from_db()
    assert job_request.status_message == "Job successfully triggered"

    assert OrganicJob.objects.count() == 1
    job = OrganicJob.objects.filter(job_uuid=job_request.uuid).first()
    assert "Timed out waiting for initial response" in job.comment
    assert job.status == OrganicJob.Status.FAILED


@patch("compute_horde_validator.validator.tasks.MinerClient", MockMinerClient)
@pytest.mark.django_db(databases=["default", "default_alias"], transaction=True)
def test_trigger_run_admin_job__should_not_trigger_job():
    miner = Miner.objects.create(hotkey="miner_client_2")
    OrganicJob.objects.all().delete()
    job_request = AdminJobRequest.objects.create(
        miner=miner,
        timeout=0,  # should timeout
        executor_class=DEFAULT_EXECUTOR_CLASS,
        docker_image="python:3.11-slim",
        args="",
    )

    assert AdminJobRequest.objects.count() == 1
    trigger_run_admin_job_request.apply(args=(job_request.pk,))

    job_request.refresh_from_db()
    assert "Job failed to trigger" in job_request.status_message

    assert OrganicJob.objects.count() == 0


def add_system_events():
    events = SystemEvent.objects.using(settings.DEFAULT_DB_ALIAS)
    events.create(
        type=SystemEvent.EventType.WEIGHT_SETTING_SUCCESS,
        subtype=SystemEvent.EventSubType.SUCCESS,
        data={},
        sent=False,
    )
    events.create(
        type=SystemEvent.EventType.WEIGHT_SETTING_FAILURE,
        subtype=SystemEvent.EventSubType.GENERIC_ERROR,
        data={},
        sent=False,
    )
    events.create(
        type=SystemEvent.EventType.WEIGHT_SETTING_FAILURE,
        subtype=SystemEvent.EventSubType.GENERIC_ERROR,
        data={},
        sent=True,
    )


def get_response(status):
    response = Response()
    response.status_code = status
    return response


@patch(
    "compute_horde_validator.validator.tasks.requests.post",
    lambda url, json, headers: get_response(201),
)
@pytest.mark.django_db(databases=["default", "default_alias"])
def test_send_events_to_facilitator__success():
    add_system_events()
    send_events_to_facilitator()
    assert SystemEvent.objects.using(settings.DEFAULT_DB_ALIAS).filter(sent=True).count() == 3


@patch(
    "compute_horde_validator.validator.tasks.requests.post",
    lambda url, json, headers: get_response(400),
)
@pytest.mark.django_db(databases=["default", "default_alias"])
def test_send_events_to_facilitator__failure():
    add_system_events()
    send_events_to_facilitator()
    assert SystemEvent.objects.using(settings.DEFAULT_DB_ALIAS).filter(sent=False).count() == 2


@pytest.mark.django_db(databases=["default", "default_alias"])
def test__calculate_job_start_block():
    assert calculate_job_start_block(cycle=range(100, 151), total=4, index_=2) == 125
    assert calculate_job_start_block(cycle=range(100, 151), total=2, index_=0) == 100
    assert calculate_job_start_block(cycle=range(100, 151), total=2, index_=1) == 125

    assert calculate_job_start_block(cycle=range(100, 201), offset=10, total=3, index_=0) == 110
    assert calculate_job_start_block(cycle=range(100, 201), offset=10, total=3, index_=1) == 140
    assert calculate_job_start_block(cycle=range(100, 201), offset=10, total=3, index_=2) == 170


@patch("compute_horde_validator.validator.tasks._run_synthetic_jobs")
@pytest.mark.django_db(databases=["default", "default_alias"])
def test__run_synthetic_jobs__not_serving(_run_synthetic_jobs, bittensor):
    from constance import config

    config.SERVING = False

    run_synthetic_jobs()

    assert _run_synthetic_jobs.apply_async.call_count == 0


@patch("compute_horde_validator.validator.tasks._run_synthetic_jobs")
@pytest.mark.django_db(databases=["default", "default_alias"])
def test__run_synthetic_jobs__not_scheduled(_run_synthetic_jobs, bittensor):
    run_synthetic_jobs()

    assert _run_synthetic_jobs.apply_async.call_count == 0


@patch("compute_horde_validator.validator.tasks._run_synthetic_jobs")
@pytest.mark.django_db(databases=["default", "default_alias"])
def test__run_synthetic_jobs__debug_dont_stagger_validators__true(
    _run_synthetic_jobs, settings, bittensor
):
    settings.DEBUG_DONT_STAGGER_VALIDATORS = True
    settings.CELERY_TASK_ALWAYS_EAGER = True

    run_synthetic_jobs()

    assert _run_synthetic_jobs.apply_async.call_count == 1


@patch("compute_horde_validator.validator.tasks._run_synthetic_jobs")
@pytest.mark.django_db(databases=["default", "default_alias"])
def test__run_synthetic_jobs__debug_dont_stagger_validators__false(
    _run_synthetic_jobs, settings, bittensor
):
    settings.DEBUG_DONT_STAGGER_VALIDATORS = False
    settings.CELERY_TASK_ALWAYS_EAGER = True

    current_block = 1

    SyntheticJobBatch.objects.create(block=current_block + 50, cycle=_get_cycle(current_block + 50))

    run_synthetic_jobs()

    assert _run_synthetic_jobs.apply_async.call_count == 0


@pytest.mark.django_db(databases=["default", "default_alias"])
@pytest.mark.parametrize(
    ("trigger_block", "expected_logs", "expected_runs", "increase", "system_event"),
    [
        (96, 0, 0, True, None),
        (
            97,
            0,
            1,
            True,
            (
                SystemEvent.EventType.VALIDATOR_OVERSLEPT_SCHEDULED_JOB_WARNING,
                SystemEvent.EventSubType.WARNING,
            ),
        ),
        (
            98,
            0,
            1,
            True,
            (
                SystemEvent.EventType.VALIDATOR_OVERSLEPT_SCHEDULED_JOB_WARNING,
                SystemEvent.EventSubType.WARNING,
            ),
        ),
        (
            99,
            0,
            1,
            True,
            (
                SystemEvent.EventType.VALIDATOR_OVERSLEPT_SCHEDULED_JOB_WARNING,
                SystemEvent.EventSubType.WARNING,
            ),
        ),
        (100, 0, 1, True, None),
        (101, 0, 1, True, None),
        (102, 1, 1, True, None),
        (103, 2, 1, True, None),
        (104, 0, 0, True, None),
        (
            101,
            6,
            0,
            False,
            (
                SystemEvent.EventType.VALIDATOR_SYNTHETIC_JOBS_FAILURE,
                SystemEvent.EventSubType.FAILED_TO_WAIT,
            ),
        ),
    ],
)
@patch("time.sleep", MagicMock())
def test__run_synthetic_jobs__different_timings(
    bittensor, settings, caplog, trigger_block, expected_logs, expected_runs, increase, system_event
):
    bittensor.blocks.head.side_effect = (
        MagicMock(
            number=i,
        )
        for i in itertools.count(100, increase)
    )

    settings.CELERY_TASK_ALWAYS_EAGER = True

    SyntheticJobBatch.objects.create(block=trigger_block, cycle=_get_cycle(trigger_block))

    with patch(
        "compute_horde_validator.validator.tasks._run_synthetic_jobs"
    ) as _run_synthetic_jobs:
        run_synthetic_jobs(wait_in_advance_blocks=3)

    assert len([r for r in caplog.records if "Waiting for block " in r.message]) == expected_logs, (
        str([r.message for r in caplog.records])
    )

    if system_event:
        check_system_events(*system_event)
    else:
        assert SystemEvent.objects.using(settings.DEFAULT_DB_ALIAS).count() == 0

    assert _run_synthetic_jobs.apply_async.call_count == expected_runs


@patch("compute_horde_validator.validator.tasks._run_synthetic_jobs")
@pytest.mark.django_db(databases=["default", "default_alias"])
def test__run_synthetic_jobs__many_scheduled_runs(_run_synthetic_jobs, settings, bittensor):
    settings.CELERY_TASK_ALWAYS_EAGER = True

    init_time = time.monotonic()

    bittensor.blocks.head.side_effect = lambda *args, **kwargs: MagicMock(
        number=1000 + int(time.monotonic() - init_time),
    )

    current_block = 1000 + int(time.monotonic() - init_time)

    SyntheticJobBatch.objects.create(block=current_block + 1, cycle=_get_cycle(current_block + 1))
    SyntheticJobBatch.objects.create(block=current_block + 2, cycle=_get_cycle(current_block + 2))
    SyntheticJobBatch.objects.create(block=current_block + 3, cycle=_get_cycle(current_block + 3))

    run_synthetic_jobs(wait_in_advance_blocks=5, poll_interval=timedelta(seconds=1))

    assert _run_synthetic_jobs.apply_async.call_count == 1


@patch("compute_horde_validator.validator.tasks._run_synthetic_jobs")
@pytest.mark.django_db(databases=["default", "default_alias"], transaction=True)
def test__run_synthetic_jobs__concurrent(_run_synthetic_jobs, settings, bittensor):
    settings.CELERY_TASK_ALWAYS_EAGER = True
    current_block = 1

    bittensor.blocks.head.side_effect = (
        MagicMock(
            number=i,
        )
        for i in itertools.count(current_block)
    )

    SyntheticJobBatch.objects.create(block=current_block + 1, cycle=_get_cycle(current_block + 1))

    num_threads = 10
    with ThreadPoolExecutor(max_workers=num_threads) as pool:
        pool.map(lambda _: run_synthetic_jobs(wait_in_advance_blocks=3), range(num_threads))

    assert _run_synthetic_jobs.apply_async.call_count == 1
    assert SyntheticJobBatch.objects.last().started_at is not None


@patch("compute_horde_validator.validator.tasks._run_synthetic_jobs", MagicMock())
@pytest.mark.django_db(databases=["default", "default_alias"], transaction=True)
def test__check_missed_synthetic_jobs(settings, bittensor):
    settings.CELERY_TASK_ALWAYS_EAGER = True

    assert SystemEvent.objects.using(settings.DEFAULT_DB_ALIAS).count() == 0

    check_missed_synthetic_jobs()
    assert SystemEvent.objects.using(settings.DEFAULT_DB_ALIAS).count() == 0

    current_block = 1

    bittensor.blocks.head.side_effect = (
        MagicMock(
            number=i,
        )
        for i in itertools.count(current_block)
    )

    SyntheticJobBatch.objects.create(block=current_block + 10, cycle=_get_cycle(current_block + 10))
    SyntheticJobBatch.objects.create(
        block=current_block - 10,
        cycle=_get_cycle(current_block - 10),
        started_at=now() - timedelta(seconds=5),
    )
    check_missed_synthetic_jobs()
    assert SystemEvent.objects.using(settings.DEFAULT_DB_ALIAS).count() == 0

    missed_batch = SyntheticJobBatch.objects.create(
        block=current_block - 5, cycle=_get_cycle(current_block - 5)
    )
    check_missed_synthetic_jobs()
    assert (
        SystemEvent.objects.using(settings.DEFAULT_DB_ALIAS)
        .filter(
            type=SystemEvent.EventType.VALIDATOR_SYNTHETIC_JOBS_FAILURE,
            subtype=SystemEvent.EventSubType.OVERSLEPT,
        )
        .count()
        == 1
    )

    missed_batch.refresh_from_db()
    assert missed_batch.is_missed is True
    check_missed_synthetic_jobs()
    assert (
        SystemEvent.objects.using(settings.DEFAULT_DB_ALIAS)
        .filter(
            type=SystemEvent.EventType.VALIDATOR_SYNTHETIC_JOBS_FAILURE,
            subtype=SystemEvent.EventSubType.OVERSLEPT,
        )
        .count()
        == 1
    )
