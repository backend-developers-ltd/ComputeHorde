import asyncio
import uuid
from unittest.mock import patch

import pytest
from asgiref.sync import sync_to_async
from compute_horde.executor_class import DEFAULT_EXECUTOR_CLASS
from constance import config
from django.test import override_settings
from django.utils.timezone import now

from compute_horde_validator.validator.models import (
    Cycle,
    Miner,
    SyntheticJob,
    SyntheticJobBatch,
    SystemEvent,
    Weights,
)
from compute_horde_validator.validator.tasks import (
    _normalize_weights_for_committing,
    reveal_scores,
    set_scores,
)

from .helpers import (
    NUM_NEURONS,
    Celery,
    MockHyperparameters,
    MockSubtensor,
    check_system_events,
    patch_constance,
    throw_error,
)


def setup_db(cycle_number=0):
    for i in range(NUM_NEURONS):
        Miner.objects.update_or_create(hotkey=f"hotkey_{i}")

    job_batch = SyntheticJobBatch.objects.create(
        started_at=now(),
        accepting_results_until=now(),
        scored=False,
        cycle=Cycle.objects.create(start=722 * cycle_number, stop=722 * (cycle_number + 1)),
    )
    for i in range(NUM_NEURONS):
        SyntheticJob.objects.create(
            batch=job_batch,
            score=i,
            job_uuid=uuid.uuid4(),
            miner=Miner.objects.get(hotkey=f"hotkey_{i}"),
            miner_address="ignore",
            miner_address_ip_version=4,
            miner_port=9999,
            executor_class=DEFAULT_EXECUTOR_CLASS,
            status=SyntheticJob.Status.COMPLETED,
        )


def test_normalize_scores():
    assert _normalize_weights_for_committing([0.5, 1.5, 100.1, 30], 65535) == [
        327,
        982,
        65535,
        19641,
    ]


@patch("bittensor.subtensor", lambda *args, **kwargs: MockSubtensor(override_block_number=361))
@pytest.mark.django_db(databases=["default", "default_alias"], transaction=True)
def test_set_scores__no_batches_found(settings):
    set_scores()
    assert SystemEvent.objects.using(settings.DEFAULT_DB_ALIAS).count() == 0


@pytest.mark.django_db(databases=["default", "default_alias"], transaction=True)
def test_set_scores__too_early(settings):
    subtensor_ = MockSubtensor(override_block_number=359)
    with patch("bittensor.subtensor", lambda *a, **kw: subtensor_):
        setup_db()
        set_scores()
        assert SystemEvent.objects.using(settings.DEFAULT_DB_ALIAS).count() == 0


@pytest.mark.django_db(databases=["default", "default_alias"], transaction=True)
@patch_constance({"DYNAMIC_COMMIT_REVEAL_WEIGHTS_ENABLED": False})
def test_set_scores__set_weight_success(settings):
    subtensor_ = MockSubtensor(override_block_number=723)
    with patch("bittensor.subtensor", lambda *a, **kw: subtensor_):
        setup_db()
        set_scores()
        assert SystemEvent.objects.using(settings.DEFAULT_DB_ALIAS).count() == 1
        assert subtensor_.weights_set == [
            [
                0.10000000149011612,
                0.20000000298023224,
                0.30000001192092896,
                0.4000000059604645,
            ]
        ]
        check_system_events(
            SystemEvent.EventType.WEIGHT_SETTING_SUCCESS,
            SystemEvent.EventSubType.SET_WEIGHTS_SUCCESS,
            1,
        )


@patch("compute_horde_validator.validator.tasks.WEIGHT_SETTING_ATTEMPTS", 1)
@patch("compute_horde_validator.validator.tasks.WEIGHT_SETTING_FAILURE_BACKOFF", 0)
@patch(
    "bittensor.subtensor",
    lambda *args, **kwargs: MockSubtensor(
        mocked_set_weights=lambda: (False, "error"), override_block_number=723
    ),
)
@pytest.mark.django_db(databases=["default", "default_alias"], transaction=True)
@patch_constance({"DYNAMIC_COMMIT_REVEAL_WEIGHTS_ENABLED": False})
def test_set_scores__set_weight_failure(settings):
    setup_db()
    set_scores()
    assert SystemEvent.objects.using(settings.DEFAULT_DB_ALIAS).count() == 2
    check_system_events(
        SystemEvent.EventType.WEIGHT_SETTING_FAILURE,
        SystemEvent.EventSubType.SET_WEIGHTS_ERROR,
        1,
    )
    # end of retries system event
    check_system_events(
        SystemEvent.EventType.WEIGHT_SETTING_FAILURE, SystemEvent.EventSubType.GIVING_UP, 1
    )


def set_weights_succeed_third_time():
    global weight_set_attempts
    weight_set_attempts += 1
    return (False, "error") if weight_set_attempts < 3 else (True, "")


@patch("compute_horde_validator.validator.tasks.WEIGHT_SETTING_ATTEMPTS", 3)
@patch("compute_horde_validator.validator.tasks.WEIGHT_SETTING_FAILURE_BACKOFF", 0)
@patch(
    "bittensor.subtensor",
    lambda *args, **kwargs: MockSubtensor(
        mocked_set_weights=set_weights_succeed_third_time, override_block_number=723
    ),
)
@pytest.mark.django_db(databases=["default", "default_alias"], transaction=True)
@patch_constance({"DYNAMIC_COMMIT_REVEAL_WEIGHTS_ENABLED": False})
def test_set_scores__set_weight_eventual_success(settings):
    global weight_set_attempts
    weight_set_attempts = 0
    setup_db()
    set_scores()
    assert SystemEvent.objects.using(settings.DEFAULT_DB_ALIAS).count() == 3
    check_system_events(
        SystemEvent.EventType.WEIGHT_SETTING_FAILURE,
        SystemEvent.EventSubType.SET_WEIGHTS_ERROR,
        2,
    )
    check_system_events(
        SystemEvent.EventType.WEIGHT_SETTING_SUCCESS,
        SystemEvent.EventSubType.SET_WEIGHTS_SUCCESS,
        1,
    )


@patch("compute_horde_validator.validator.tasks.WEIGHT_SETTING_ATTEMPTS", 1)
@patch("compute_horde_validator.validator.tasks.WEIGHT_SETTING_FAILURE_BACKOFF", 0)
@patch(
    "bittensor.subtensor",
    lambda *args, **kwargs: MockSubtensor(
        mocked_set_weights=throw_error, override_block_number=723
    ),
)
@pytest.mark.django_db(databases=["default", "default_alias"], transaction=True)
@patch_constance({"DYNAMIC_COMMIT_REVEAL_WEIGHTS_ENABLED": False})
def test_set_scores__set_weight__exception(settings):
    setup_db()
    set_scores()
    assert SystemEvent.objects.using(settings.DEFAULT_DB_ALIAS).count() == 2
    check_system_events(
        SystemEvent.EventType.WEIGHT_SETTING_FAILURE,
        SystemEvent.EventSubType.SET_WEIGHTS_ERROR,
        1,
    )
    # end of retries system event
    check_system_events(
        SystemEvent.EventType.WEIGHT_SETTING_FAILURE, SystemEvent.EventSubType.GIVING_UP, 1
    )


@patch("compute_horde_validator.validator.tasks.WEIGHT_SETTING_ATTEMPTS", 1)
@patch("compute_horde_validator.validator.tasks.WEIGHT_SETTING_FAILURE_BACKOFF", 0)
@patch(
    "bittensor.subtensor",
    lambda *args, **kwargs: MockSubtensor(
        mocked_commit_weights=lambda: throw_error(),
        override_block_number=723,
        hyperparameters=MockHyperparameters(
            commit_reveal_weights_enabled=True,
        ),
    ),
)
@pytest.mark.django_db(databases=["default", "default_alias"], transaction=True)
def test_set_scores__set_weight__commit__exception(settings):
    setup_db()
    set_scores()

    check_system_events(
        SystemEvent.EventType.WEIGHT_SETTING_FAILURE,
        SystemEvent.EventSubType.COMMIT_WEIGHTS_ERROR,
        1,
    )


@patch("compute_horde_validator.validator.tasks.WEIGHT_SETTING_ATTEMPTS", 1)
@patch("compute_horde_validator.validator.tasks.WEIGHT_SETTING_FAILURE_BACKOFF", 0)
@patch("compute_horde_validator.validator.tasks.WEIGHT_SETTING_HARD_TTL", 1)
@patch("compute_horde_validator.validator.tasks.WEIGHT_SETTING_TTL", 1)
@patch("bittensor.subtensor", lambda *args, **kwargs: MockSubtensor(override_block_number=723))
@pytest.mark.django_db(databases=["default", "default_alias"], transaction=True)
@patch_constance({"DYNAMIC_COMMIT_REVEAL_WEIGHTS_ENABLED": False})
def test_set_scores__set_weight_timeout(settings):
    settings.CELERY_TASK_ALWAYS_EAGER = False  # to make it timeout
    setup_db()
    set_scores()
    assert SystemEvent.objects.using(settings.DEFAULT_DB_ALIAS).count() == 2
    check_system_events(
        SystemEvent.EventType.WEIGHT_SETTING_FAILURE,
        SystemEvent.EventSubType.WRITING_TO_CHAIN_TIMEOUT,
        1,
    )
    # end of retries system event
    check_system_events(
        SystemEvent.EventType.WEIGHT_SETTING_FAILURE, SystemEvent.EventSubType.GIVING_UP, 1
    )


@patch("compute_horde_validator.validator.tasks.WEIGHT_SETTING_ATTEMPTS", 1)
@patch("compute_horde_validator.validator.tasks.WEIGHT_SETTING_FAILURE_BACKOFF", 0)
@patch("compute_horde_validator.validator.tasks.WEIGHT_SETTING_HARD_TTL", 1)
@patch("compute_horde_validator.validator.tasks.WEIGHT_SETTING_TTL", 1)
@pytest.mark.django_db(databases=["default", "default_alias"], transaction=True)
@patch_constance({"DYNAMIC_COMMIT_REVEAL_WEIGHTS_INTERVAL": 20})
def test_set_scores__set_weight__commit(settings):
    subtensor_ = MockSubtensor(
        override_block_number=723,
        hyperparameters=MockHyperparameters(
            commit_reveal_weights_enabled=True,
        ),
    )
    with patch("bittensor.subtensor", lambda *a, **kw: subtensor_):
        setup_db()
        set_scores()
        assert (
            SystemEvent.objects.using(settings.DEFAULT_DB_ALIAS).count() == 1
        ), SystemEvent.objects.all()
        assert subtensor_.weights_committed == [
            [
                16384,
                32768,
                49151,
                65535,
            ]
        ]
        check_system_events(
            SystemEvent.EventType.WEIGHT_SETTING_SUCCESS,
            SystemEvent.EventSubType.COMMIT_WEIGHTS_SUCCESS,
            1,
        )
        assert Weights.objects.count() == 1


@patch("compute_horde_validator.validator.tasks.WEIGHT_SETTING_ATTEMPTS", 1)
@patch(
    "bittensor.subtensor",
    lambda *args, **kwargs: MockSubtensor(
        override_block_number=1445,
        hyperparameters=MockHyperparameters(
            commit_reveal_weights_enabled=True,
        ),
    ),
)
@pytest.mark.django_db(databases=["default", "default_alias"], transaction=True)
@patch_constance({"DYNAMIC_COMMIT_REVEAL_WEIGHTS_INTERVAL": 20})
def test_set_scores__set_weight__double_commit_failure(settings):
    setup_db()
    set_scores()

    weights = Weights.objects.all()
    assert len(weights) == 1
    assert weights[0].revealed_at is None

    setup_db(cycle_number=1)

    set_scores()
    assert Weights.objects.count() == 1
    check_system_events(
        SystemEvent.EventType.WEIGHT_SETTING_FAILURE,
        SystemEvent.EventSubType.COMMIT_WEIGHTS_UNREVEALED_ERROR,
        1,
    )


@patch(
    "bittensor.subtensor",
    lambda *args, **kwargs: MockSubtensor(
        override_block_number=1000,
        hyperparameters=MockHyperparameters(
            commit_reveal_weights_enabled=True,
        ),
    ),
)
@pytest.mark.django_db(databases=["default", "default_alias"], transaction=True)
@patch_constance({"DYNAMIC_COMMIT_REVEAL_WEIGHTS_INTERVAL": 20})
def test_set_scores__set_weight__reveal__too_early(settings):
    setup_db()
    set_scores()

    last_weights = Weights.objects.order_by("-id").first()
    assert last_weights
    assert last_weights.revealed_at is None

    from bittensor import subtensor

    assert subtensor().get_current_block() == 1000
    reveal_scores()

    last_weights.refresh_from_db()
    assert last_weights.revealed_at is None
    check_system_events(
        SystemEvent.EventType.WEIGHT_SETTING_FAILURE,
        SystemEvent.EventSubType.REVEAL_WEIGHTS_ERROR,
        0,  # nothing happened because it's too early to reveal weights
    )


@pytest.mark.django_db(databases=["default", "default_alias"], transaction=True)
@patch_constance({"DYNAMIC_COMMIT_REVEAL_WEIGHTS_INTERVAL": 20})
def test_set_scores__set_weight__reveal__in_time(settings):
    subtensor_ = MockSubtensor(
        override_block_number=723,
        hyperparameters=MockHyperparameters(
            commit_reveal_weights_enabled=True,
        ),
    )
    with patch("bittensor.subtensor", lambda *a, **kw: subtensor_):
        setup_db()
        set_scores()

        last_weights = Weights.objects.order_by("-id").first()
        assert last_weights
        assert last_weights.revealed_at is None

        subtensor_.get_current_block = lambda: 1020
        reveal_scores()

        assert subtensor_.weights_revealed == [
            [
                16384,
                32768,
                49151,
                65535,
            ]
        ]
        last_weights.refresh_from_db()
        assert last_weights.revealed_at is not None
        check_system_events(
            SystemEvent.EventType.WEIGHT_SETTING_SUCCESS,
            SystemEvent.EventSubType.REVEAL_WEIGHTS_SUCCESS,
            1,
        )


@pytest.mark.django_db(databases=["default", "default_alias"], transaction=True)
@patch_constance({"DYNAMIC_COMMIT_REVEAL_WEIGHTS_INTERVAL": 20})
def test_set_scores__set_weight__reveal__timeout(settings, run_uuid):
    subtensor_ = MockSubtensor(
        override_block_number=723,
        hyperparameters=MockHyperparameters(
            commit_reveal_weights_enabled=True,
        ),
    )

    with patch("bittensor.subtensor", lambda *a, **kw: subtensor_):
        setup_db()
        set_scores()
        last_weights = Weights.objects.order_by("-id").first()
        assert last_weights
        assert last_weights.revealed_at is None

        config.DYNAMIC_WEIGHT_REVEALING_TTL = 9
        config.DYNAMIC_WEIGHT_REVEALING_HARD_TTL = 15
        config.DYNAMIC_WEIGHT_REVEALING_ATTEMPTS = 5
        config.DYNAMIC_WEIGHT_REVEALING_FAILURE_BACKOFF = 1

        with override_settings(CELERY_TASK_ALWAYS_EAGER=False):
            with Celery("compute_horde_validator.validator.tests.mock_subtensor_config", run_uuid):
                result = reveal_scores.apply_async()
                result.get(timeout=120)
        last_weights.refresh_from_db()
        assert last_weights.revealed_at is not None
        assert list(SystemEvent.objects.values_list("type", "subtype").order_by("id")) in [
            [
                ("WEIGHT_SETTING_SUCCESS", "COMMIT_WEIGHTS_SUCCESS"),
                ("WEIGHT_SETTING_FAILURE", "REVEAL_WEIGHTS_ERROR"),
                ("WEIGHT_SETTING_FAILURE", "REVEAL_WEIGHTS_ERROR"),
                ("WEIGHT_SETTING_FAILURE", "REVEAL_WEIGHTS_ERROR"),
                ("WEIGHT_SETTING_SUCCESS", "REVEAL_WEIGHTS_SUCCESS"),
            ],
            [
                ("WEIGHT_SETTING_SUCCESS", "COMMIT_WEIGHTS_SUCCESS"),
                ("WEIGHT_SETTING_FAILURE", "WRITING_TO_CHAIN_TIMEOUT"),
                ("WEIGHT_SETTING_FAILURE", "REVEAL_WEIGHTS_ERROR"),
                ("WEIGHT_SETTING_FAILURE", "REVEAL_WEIGHTS_ERROR"),
                ("WEIGHT_SETTING_SUCCESS", "REVEAL_WEIGHTS_SUCCESS"),
            ],
            [
                ("WEIGHT_SETTING_SUCCESS", "COMMIT_WEIGHTS_SUCCESS"),
                ("WEIGHT_SETTING_FAILURE", "REVEAL_WEIGHTS_ERROR"),
                ("WEIGHT_SETTING_FAILURE", "WRITING_TO_CHAIN_TIMEOUT"),
                ("WEIGHT_SETTING_FAILURE", "REVEAL_WEIGHTS_ERROR"),
                ("WEIGHT_SETTING_FAILURE", "REVEAL_WEIGHTS_ERROR"),
                ("WEIGHT_SETTING_SUCCESS", "REVEAL_WEIGHTS_SUCCESS"),
            ],
        ]


# ! This test is the last because otherwise it breaks other tests
# ! (probably it doesn't release lock properly, so other tests cannot set scores)
@patch("compute_horde_validator.validator.tasks.WEIGHT_SETTING_ATTEMPTS", 1)
@patch("compute_horde_validator.validator.tasks.WEIGHT_SETTING_FAILURE_BACKOFF", 0)
@patch("compute_horde_validator.validator.tasks.WEIGHT_SETTING_HARD_TTL", 1)
@patch("compute_horde_validator.validator.tasks.WEIGHT_SETTING_TTL", 1)
@patch("bittensor.subtensor", lambda *args, **kwargs: MockSubtensor(override_block_number=723))
@pytest.mark.django_db(databases=["default", "default_alias"], transaction=True)
@pytest.mark.asyncio
# TODO: Address the unclosed socket warning
@pytest.mark.filterwarnings("default:Exception ignored")
async def test_set_scores__multiple_starts(settings):
    # to ensure the other tasks will be run at the same time
    settings.CELERY_TASK_ALWAYS_EAGER = False
    await sync_to_async(setup_db)()

    tasks = [sync_to_async(set_scores, thread_sensitive=False)() for _ in range(5)]
    await asyncio.gather(*tasks)

    assert await SystemEvent.objects.using(settings.DEFAULT_DB_ALIAS).acount() == 2
    await sync_to_async(check_system_events)(
        SystemEvent.EventType.WEIGHT_SETTING_FAILURE,
        SystemEvent.EventSubType.WRITING_TO_CHAIN_TIMEOUT,
        1,
    )
    # end of retries system event
    await sync_to_async(check_system_events)(
        SystemEvent.EventType.WEIGHT_SETTING_FAILURE, SystemEvent.EventSubType.GIVING_UP, 1
    )
