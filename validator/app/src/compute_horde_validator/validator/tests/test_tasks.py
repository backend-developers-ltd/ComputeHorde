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
    Miner,
    OrganicJob,
    SyntheticJobBatch,
    SystemEvent,
)
from compute_horde_validator.validator.tasks import (
    calculate_job_start_block,
    check_missed_synthetic_jobs,
    get_cycle_containing_block,
    get_epoch_containing_block,
    run_synthetic_jobs,
    schedule_synthetic_jobs,
    send_events_to_facilitator,
    trigger_run_admin_job_request,
)

from .helpers import (
    MockMetagraph,
    MockMinerClient,
    MockSubtensor,
    SingleExecutorMockMinerClient,
    check_system_events,
    mock_get_miner_axon_info,
    throw_error,
)


@patch("compute_horde_validator.validator.tasks.get_miner_axon_info", mock_get_miner_axon_info)
@patch("compute_horde_validator.validator.tasks.MinerClient", MockMinerClient)
@pytest.mark.django_db(databases=["default", "default_alias"], transaction=True)
def test_trigger_run_admin_job__should_trigger_job():
    miner = Miner.objects.create(hotkey="miner_client_1")
    OrganicJob.objects.all().delete()
    job_request = AdminJobRequest.objects.create(
        miner=miner,
        timeout=0,  # should timeout
        executor_class=DEFAULT_EXECUTOR_CLASS,
        docker_image="python:3.11-slim",
        raw_script="print('hello world')",
        args="",
    )

    assert AdminJobRequest.objects.count() == 1
    trigger_run_admin_job_request.apply(args=(job_request.pk,))

    job_request.refresh_from_db()
    assert job_request.status_message == "Job successfully triggered"

    assert OrganicJob.objects.count() == 1
    job = OrganicJob.objects.filter(job_uuid=job_request.uuid).first()
    assert "timed out while preparing executor" in job.comment
    assert job.status == OrganicJob.Status.FAILED


@patch("compute_horde_validator.validator.tasks.get_miner_axon_info", throw_error)
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
        raw_script="print('hello world')",
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


@pytest.mark.parametrize(
    ("netuid", "block", "expected_epoch"),
    [
        # netuid == 0
        (0, 25, range(-2, 359)),
        (0, 359, range(-2, 359)),
        (0, 360, range(359, 720)),
        (0, 720, range(359, 720)),
        (0, 721, range(720, 1081)),
        # netuid == 12
        (12, 25, range(-14, 347)),
        (12, 347, range(-14, 347)),
        (12, 348, range(347, 708)),
        (12, 708, range(347, 708)),
        (12, 709, range(708, 1069)),
        (12, 1100, range(1069, 1430)),
    ],
)
def test__get_epoch_containing_block(netuid, block, expected_epoch):
    assert (
        get_epoch_containing_block(block=block, netuid=netuid) == expected_epoch
    ), f"block: {block}, netuid: {netuid}, expected: {expected_epoch}"


@pytest.mark.parametrize(
    ("netuid", "block", "expected_cycle"),
    [
        # netuid == 0
        (0, 25, range(-2, 359 + 361)),
        (0, 359, range(-2, 359 + 361)),
        (0, 360, range(359 - 361, 720)),
        (0, 720, range(359 - 361, 720)),
        (0, 721, range(720, 1081 + 361)),
        # netuid == 12
        (12, 25, range(-14, 347 + 361)),
        (12, 347, range(-14, 347 + 361)),
        (12, 348, range(347 - 361, 708)),
        (12, 708, range(347 - 361, 708)),
        (12, 709, range(708, 1069 + 361)),
        (12, 1100, range(1069 - 361, 1430)),
    ],
)
def test__get_cycle_containing_block(netuid, block, expected_cycle):
    assert (
        get_cycle_containing_block(block=block, netuid=netuid) == expected_cycle
    ), f"block: {block}, netuid: {netuid}, expected: {expected_cycle}"


@pytest.mark.django_db(databases=["default", "default_alias"])
def test__calculate_job_start_block():
    assert calculate_job_start_block(cycle=range(100, 151), total=4, index_=2) == 125
    assert calculate_job_start_block(cycle=range(100, 151), total=2, index_=0) == 100
    assert calculate_job_start_block(cycle=range(100, 151), total=2, index_=1) == 125

    assert calculate_job_start_block(cycle=range(100, 201), offset=10, total=3, index_=0) == 110
    assert calculate_job_start_block(cycle=range(100, 201), offset=10, total=3, index_=1) == 140
    assert calculate_job_start_block(cycle=range(100, 201), offset=10, total=3, index_=2) == 170


@patch("bittensor.subtensor", lambda *args, **kwargs: MockSubtensor())
@pytest.mark.django_db(databases=["default", "default_alias"])
def test__schedule_validation_run__not_in_validators(validators):
    with patch(
        "compute_horde_validator.validator.tasks.get_validators", lambda *args, **kwargs: validators
    ):
        assert SyntheticJobBatch.objects.count() == 0
        schedule_synthetic_jobs()
        assert SyntheticJobBatch.objects.count() == 0


@patch("bittensor.subtensor", lambda *args, **kwargs: MockSubtensor(override_block_number=1200))
@pytest.mark.django_db(databases=["default", "default_alias"])
def test__schedule_validation_run__unable_to_fetch_metagraph(validators):
    assert SyntheticJobBatch.objects.count() == 0
    schedule_synthetic_jobs()
    assert SyntheticJobBatch.objects.count() == 0


@pytest.mark.django_db(databases=["default", "default_alias"])
def test__schedule_validation_run__simple(validators_with_this_hotkey):
    with patch(
        "bittensor.subtensor",
        lambda *args, **kwargs: MockSubtensor(
            override_block_number=140,
            mocked_metagraph=lambda *a, **kw: MockMetagraph(
                neurons=validators_with_this_hotkey, num_neurons=None
            ),
        ),
    ):
        assert SyntheticJobBatch.objects.count() == 0
        schedule_synthetic_jobs()
        assert SyntheticJobBatch.objects.count() == 1

        schedule = SyntheticJobBatch.objects.last()
        assert schedule.block == 390


@patch("bittensor.subtensor", lambda *args, **kwargs: MockSubtensor())
@pytest.mark.django_db(databases=["default", "default_alias"], transaction=True)
def test__schedule_validation_run__concurrent(validators_with_this_hotkey):
    with patch(
        "compute_horde_validator.validator.tasks.get_validators",
        lambda *args, **kwargs: validators_with_this_hotkey,
    ):
        assert SyntheticJobBatch.objects.count() == 0
        num_threads = 10
        with ThreadPoolExecutor(max_workers=num_threads) as pool:
            pool.map(lambda _: schedule_synthetic_jobs(), range(num_threads))

        assert SyntheticJobBatch.objects.count() == 1


@patch("bittensor.subtensor", lambda *args, **kwargs: MockSubtensor())
@pytest.mark.django_db(databases=["default", "default_alias"])
def test__schedule_validation_run__already_scheduled(validators_with_this_hotkey):
    from bittensor import subtensor

    current_block = subtensor().get_current_block()
    assert current_block == 1000

    SyntheticJobBatch.objects.create(block=current_block + 20)
    with patch(
        "compute_horde_validator.validator.tasks.get_validators",
        lambda *args, **kwargs: validators_with_this_hotkey,
    ):
        schedule_synthetic_jobs()
        assert SyntheticJobBatch.objects.count() == 1


@patch("bittensor.subtensor", lambda *args, **kwargs: MockSubtensor())
@patch("compute_horde_validator.validator.tasks._run_synthetic_jobs", MagicMock())
@pytest.mark.django_db(databases=["default", "default_alias"])
def test__run_synthetic_jobs__not_serving():
    from constance import config

    config.SERVING = False

    run_synthetic_jobs()

    from compute_horde_validator.validator.tasks import _run_synthetic_jobs

    assert _run_synthetic_jobs.apply_async.call_count == 0


@patch(
    "compute_horde_validator.validator.tasks.get_subtensor", lambda *args, **kwargs: MockSubtensor()
)
@patch("compute_horde_validator.validator.tasks._run_synthetic_jobs", MagicMock())
@pytest.mark.django_db(databases=["default", "default_alias"])
def test__run_synthetic_jobs__not_scheduled():
    run_synthetic_jobs()

    from compute_horde_validator.validator.tasks import _run_synthetic_jobs

    assert _run_synthetic_jobs.apply_async.call_count == 0


@patch("bittensor.subtensor", lambda *args, **kwargs: MockSubtensor())
@patch(
    "compute_horde_validator.validator.tasks.get_subtensor", lambda *args, **kwargs: MockSubtensor()
)
@patch("compute_horde_validator.validator.tasks._run_synthetic_jobs", MagicMock())
@pytest.mark.django_db(databases=["default", "default_alias"])
def test__run_synthetic_jobs__debug_dont_stagger_validators__true(settings):
    settings.DEBUG_DONT_STAGGER_VALIDATORS = True
    settings.CELERY_TASK_ALWAYS_EAGER = True

    run_synthetic_jobs()

    from compute_horde_validator.validator.tasks import _run_synthetic_jobs

    assert _run_synthetic_jobs.apply_async.call_count == 1


@patch("bittensor.subtensor", lambda *args, **kwargs: MockSubtensor())
@patch(
    "compute_horde_validator.validator.tasks.get_subtensor", lambda *args, **kwargs: MockSubtensor()
)
@patch("compute_horde_validator.validator.tasks._run_synthetic_jobs", MagicMock())
@pytest.mark.django_db(databases=["default", "default_alias"])
def test__run_synthetic_jobs__debug_dont_stagger_validators__false(settings):
    settings.DEBUG_DONT_STAGGER_VALIDATORS = False
    settings.CELERY_TASK_ALWAYS_EAGER = True

    from bittensor import subtensor

    current_block = subtensor().get_current_block()
    SyntheticJobBatch.objects.create(block=current_block + 50)

    run_synthetic_jobs()

    from compute_horde_validator.validator.tasks import _run_synthetic_jobs

    assert _run_synthetic_jobs.apply_async.call_count == 0


@patch("bittensor.subtensor", lambda *args, **kwargs: MockSubtensor())
@patch(
    "compute_horde_validator.validator.tasks.get_subtensor",
    lambda *args, **kwargs: MockSubtensor(
        increase_block_number_with_each_call=True,
        override_block_number=100,
    ),
)
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
    settings, caplog, trigger_block, expected_logs, expected_runs, increase, system_event
):
    with patch(
        "compute_horde_validator.validator.tasks.get_subtensor",
        lambda *args, **kwargs: MockSubtensor(
            increase_block_number_with_each_call=increase,
            override_block_number=100,
        ),
    ):
        settings.CELERY_TASK_ALWAYS_EAGER = True

        SyntheticJobBatch.objects.create(block=trigger_block)

        with patch(
            "compute_horde_validator.validator.tasks._run_synthetic_jobs"
        ) as _run_synthetic_jobs:
            run_synthetic_jobs(wait_in_advance_blocks=3)

        assert (
            len([r for r in caplog.records if "Waiting for block " in r.message]) == expected_logs
        ), str([r.message for r in caplog.records])

        if system_event:
            check_system_events(*system_event)
        else:
            assert SystemEvent.objects.using(settings.DEFAULT_DB_ALIAS).count() == 0

        assert _run_synthetic_jobs.apply_async.call_count == expected_runs


@patch("bittensor.subtensor", lambda *args, **kwargs: MockSubtensor())
@patch(
    "compute_horde_validator.validator.tasks.get_subtensor", lambda *args, **kwargs: MockSubtensor()
)
@patch("compute_horde_validator.validator.tasks._run_synthetic_jobs", MagicMock())
@pytest.mark.django_db(databases=["default", "default_alias"])
def test__run_synthetic_jobs__many_scheduled_runs(settings):
    settings.CELERY_TASK_ALWAYS_EAGER = True

    from bittensor import subtensor

    current_block = subtensor().get_current_block()
    SyntheticJobBatch.objects.create(block=current_block + 1)
    SyntheticJobBatch.objects.create(block=current_block + 2)
    SyntheticJobBatch.objects.create(block=current_block + 3)

    run_synthetic_jobs(wait_in_advance_blocks=5, poll_interval=timedelta(seconds=1))

    from compute_horde_validator.validator.tasks import _run_synthetic_jobs

    assert _run_synthetic_jobs.apply_async.call_count == 1


@patch("bittensor.subtensor", lambda *args, **kwargs: MockSubtensor())
@patch(
    "compute_horde_validator.validator.tasks.get_subtensor", lambda *args, **kwargs: MockSubtensor()
)
@patch("compute_horde_validator.validator.tasks._run_synthetic_jobs", MagicMock())
@pytest.mark.django_db(databases=["default", "default_alias"], transaction=True)
def test__run_synthetic_jobs__concurrent(settings):
    settings.CELERY_TASK_ALWAYS_EAGER = True

    from bittensor import subtensor

    current_block = subtensor().get_current_block()
    SyntheticJobBatch.objects.create(block=current_block + 1)

    num_threads = 10
    with ThreadPoolExecutor(max_workers=num_threads) as pool:
        pool.map(lambda _: run_synthetic_jobs(wait_in_advance_blocks=3), range(num_threads))

    from compute_horde_validator.validator.tasks import _run_synthetic_jobs

    assert _run_synthetic_jobs.apply_async.call_count == 1
    assert SyntheticJobBatch.objects.last().started_at is not None


@patch("bittensor.subtensor", lambda *args, **kwargs: MockSubtensor())
@patch(
    "compute_horde_validator.validator.tasks.get_subtensor", lambda *args, **kwargs: MockSubtensor()
)
@patch("compute_horde_validator.validator.tasks._run_synthetic_jobs", MagicMock())
@pytest.mark.django_db(databases=["default", "default_alias"], transaction=True)
def test__check_missed_synthetic_jobs(settings):
    settings.CELERY_TASK_ALWAYS_EAGER = True

    assert SystemEvent.objects.using(settings.DEFAULT_DB_ALIAS).count() == 0

    check_missed_synthetic_jobs()
    assert SystemEvent.objects.using(settings.DEFAULT_DB_ALIAS).count() == 0

    from bittensor import subtensor

    current_block = subtensor().get_current_block()
    SyntheticJobBatch.objects.create(block=current_block + 10)
    SyntheticJobBatch.objects.create(
        block=current_block - 10, started_at=now() - timedelta(seconds=5)
    )
    check_missed_synthetic_jobs()
    assert SystemEvent.objects.using(settings.DEFAULT_DB_ALIAS).count() == 0

    missed_batch = SyntheticJobBatch.objects.create(block=current_block - 5)
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
