import concurrent.futures
from contextlib import contextmanager
from unittest.mock import patch

import pytest
from compute_horde.executor_class import DEFAULT_EXECUTOR_CLASS
from constance import config
from constance.test.pytest import override_config
from django.db.models import Max
from django.test import override_settings

from compute_horde_validator.validator.models import (
    Miner,
    SystemEvent,
)

from ...models.scoring.internal import Weights, WeightSettingFinishedEvent
from ...scoring.tasks import _normalize_weights_for_committing, reveal_scores, set_scores
from ..helpers import (
    NUM_NEURONS,
    Celery,
    MockHyperparameters,
    MockSubtensor,
    check_system_events,
    patch_constance,
    throw_error,
)


@pytest.fixture(autouse=True)
def _default_commit_reveal_params():
    with override_config(
        DYNAMIC_COMMIT_REVEAL_WEIGHTS_INTERVAL=722,
        DYNAMIC_COMMIT_REVEAL_COMMIT_START_OFFSET=361,
        DYNAMIC_COMMIT_REVEAL_COMMIT_END_BUFFER=15,
        DYNAMIC_COMMIT_REVEAL_REVEAL_END_BUFFER=15,
        DYNAMIC_BURN_TARGET_SS58ADDRESSES="",
        DYNAMIC_BURN_RATE=0.0,
        DYNAMIC_BURN_PARTITION=0.0,
    ):
        yield


@contextmanager
def setup_db_and_scores(hotkey_to_score=None):
    if hotkey_to_score is None:
        hotkey_to_score = {f"hotkey_{i}": i for i in range(NUM_NEURONS)}
    for i in range(NUM_NEURONS):
        Miner.objects.update_or_create(hotkey=f"hotkey_{i}")

    with patch(
        "compute_horde_validator.validator.scoring.engine.calculate_allowance_paid_job_scores",
        return_value={DEFAULT_EXECUTOR_CLASS: hotkey_to_score},
    ):
        yield


@pytest.mark.django_db
def test_normalize_scores():
    assert _normalize_weights_for_committing([0.5, 1.5, 100.1, 30], 65535) == [
        327,
        982,
        65535,
        19641,
    ]


@pytest.mark.django_db(databases=["default", "default_alias"], transaction=True)
def test_set_scores__already_done(settings, bittensor):
    bittensor.blocks.head.return_value.number = 361
    WeightSettingFinishedEvent.from_block(361, settings.BITTENSOR_NETUID)
    set_scores()
    assert SystemEvent.objects.using(settings.DEFAULT_DB_ALIAS).count() == 0


@pytest.mark.django_db(databases=["default", "default_alias"], transaction=True)
def test_set_scores__too_early(settings, bittensor):
    bittensor.blocks.head.return_value.number = 359

    with setup_db_and_scores():
        set_scores()
    assert SystemEvent.objects.using(settings.DEFAULT_DB_ALIAS).count() == 0


@pytest.mark.override_config(DYNAMIC_EXECUTOR_CLASS_WEIGHTS="spin_up-4min.gpu-24gb=100")
@pytest.mark.django_db(databases=["default", "default_alias"], transaction=True)
@patch_constance({"DYNAMIC_COMMIT_REVEAL_WEIGHTS_ENABLED": False})
@pytest.mark.parametrize(
    "cycle_number,burn_rate,burn_partition,burn_targets,hotkey_to_score,expected_weights_set",
    [
        # no burn, either effectively or by configuration:
        (0, 0.0, 0.0, "", None, {1: 16384, 2: 32768, 3: 49151, 4: 65535}),
        (0, 0.5, 0.5, "", None, {1: 16384, 2: 32768, 3: 49151, 4: 65535}),
        (0, 0.0, 0.5, "hotkey_1", None, {1: 16384, 2: 32768, 3: 49151, 4: 65535}),
        (0, 0.5, 0.5, "hotkey_100", None, {1: 16384, 2: 32768, 3: 49151, 4: 65535}),
        (0, 0.5, 0.5, "hotkey_100,hotkey_200", None, {1: 16384, 2: 32768, 3: 49151, 4: 65535}),
        # burn burn:
        (
            0,
            0.9,
            0.9,
            "hotkey_1,hotkey_2",
            {"hotkey_0": 70, "hotkey_3": 70, "hotkey_4": 100},
            {0: 2360, 1: 7282, 2: 65535, 3: 2360, 4: 3371},
        ),
        (
            0,
            0.9,
            0.9,
            "hotkey_1,hotkey_2",
            {"hotkey_0": 140, "hotkey_3": 140, "hotkey_4": 200},
            {0: 2360, 1: 7282, 2: 65535, 3: 2360, 4: 3371},
        ),
        (
            1,
            0.9,
            0.9,
            "hotkey_1,hotkey_2",
            {"hotkey_0": 70, "hotkey_3": 70, "hotkey_4": 100},
            {0: 2360, 1: 65535, 2: 7282, 3: 2360, 4: 3371},
        ),
        (
            1,
            0.9,
            0.9,
            "hotkey_1,hotkey_200",
            {"hotkey_0": 70, "hotkey_3": 70, "hotkey_4": 100},
            {0: 2124, 1: 65535, 3: 2124, 4: 3034},
        ),
        (
            0,
            0.9,
            0.9,
            "hotkey_1,hotkey_3",
            {"hotkey_0": 70, "hotkey_3": 70, "hotkey_4": 100},
            {0: 2278, 1: 7029, 3: 65535, 4: 3254},
        ),
        (
            0,
            0.9,
            0.9,
            "hotkey_1,hotkey_3",
            {"hotkey_0": 70, "hotkey_1": 70, "hotkey_2": 70, "hotkey_3": 70, "hotkey_4": 100},
            {0: 1457, 1: 8577, 2: 1457, 3: 65535, 4: 2082},
        ),
        (
            0,
            0.9,
            0.9,
            "hotkey_1,hotkey_2,hotkey_3",
            {"hotkey_0": 70, "hotkey_1": 70, "hotkey_2": 70, "hotkey_3": 70, "hotkey_4": 100},
            {0: 1457, 1: 5017, 2: 65535, 3: 5017, 4: 2082},
        ),
        (
            0,
            0.9,
            0.9,
            "hotkey_1,hotkey_2,hotkey_3",
            {"hotkey_0": 70, "hotkey_4": 100},
            {0: 3331, 1: 3641, 2: 65535, 3: 3641, 4: 4759},
        ),
        (
            0,
            0.9,
            0.9,
            "hotkey_1,hotkey_3,hotkey_200",
            {"hotkey_0": 70, "hotkey_3": 70, "hotkey_4": 100},
            {0: 2278, 1: 7029, 3: 65535, 4: 3254},
        ),
        (
            0,
            0.9,
            0.9,
            "hotkey_1,hotkey_3,hotkey_200",
            {"hotkey_0": 70, "hotkey_3": 70, "hotkey_4": 100},
            {0: 2278, 1: 7029, 3: 65535, 4: 3254},
        ),
        # burn burn again but params are different
        (
            0,
            0.7,
            0.8,
            "hotkey_1,hotkey_2",
            {"hotkey_0": 70, "hotkey_3": 70, "hotkey_4": 100},
            {0: 10240, 1: 16384, 2: 65535, 3: 10240, 4: 14628},
        ),
        (
            0,
            0.7,
            0.8,
            "hotkey_1,hotkey_2",
            {"hotkey_0": 140, "hotkey_3": 140, "hotkey_4": 200},
            {0: 10240, 1: 16384, 2: 65535, 3: 10240, 4: 14628},
        ),
        (
            1,
            0.7,
            0.8,
            "hotkey_1,hotkey_200",
            {"hotkey_0": 70, "hotkey_3": 70, "hotkey_4": 100},
            {0: 8192, 1: 65535, 3: 8192, 4: 11703},
        ),
        (
            0,
            0.7,
            0.8,
            "hotkey_1,hotkey_2,hotkey_3",
            {"hotkey_0": 70, "hotkey_1": 70, "hotkey_2": 70, "hotkey_3": 70, "hotkey_4": 100},
            {0: 5886, 1: 13342, 2: 65535, 3: 13342, 4: 8409},
        ),
        (
            0,
            0.7,
            0.8,
            "hotkey_1,hotkey_2,hotkey_3",
            {"hotkey_0": 70, "hotkey_4": 100},
            {0: 14456, 1: 8192, 2: 65535, 3: 8192, 4: 20652},
        ),
    ],
)
@override_settings(
    BITTENSOR_NETUID=359
)  # these test cases were written assuming RNGs are being seeded with the right
# block numbers here and there and now that the code calculating the cycle start/stop has been refactored
# in order to get the expected values we need to meddle with the NETUID
def test_set_scores__set_weight_success(
    settings,
    bittensor,
    cycle_number,
    burn_rate,
    burn_partition,
    burn_targets,
    hotkey_to_score,
    expected_weights_set,
):
    def _normalize_weights(weights: dict[int, int]) -> dict[int, float]:
        total = sum(weights.values())
        return {uid: w / total for uid, w in weights.items()}

    bittensor.blocks.head.return_value.number = 723 + cycle_number * 722

    with setup_db_and_scores(hotkey_to_score=hotkey_to_score):
        with override_config(
            DYNAMIC_BURN_TARGET_SS58ADDRESSES=burn_targets,
            DYNAMIC_BURN_RATE=burn_rate,
            DYNAMIC_BURN_PARTITION=burn_partition,
        ):
            set_scores()
        assert SystemEvent.objects.using(settings.DEFAULT_DB_ALIAS).count() == 4

        # reconcile weights set with expected ones, disregarding rounding errors:
        # (the very precise comparisons can be found in the weight committing tests and commit-reveal is the one used
        # in production anyway
        assert bittensor.subnet.return_value.weights.commit.called
        expected_weights_set = _normalize_weights(expected_weights_set)
        for uid, w in expected_weights_set.items():
            actual_w = bittensor.subnet.return_value.weights.commit.call_args.args[0].get(uid)
            if actual_w is not None and ((w - actual_w) / w < 0.001):
                expected_weights_set[uid] = actual_w

        bittensor.subnet.return_value.weights.commit.assert_called_with(
            expected_weights_set,
            version_key=2,
        )

        check_system_events(
            SystemEvent.EventType.WEIGHT_SETTING_SUCCESS,
            SystemEvent.EventSubType.SET_WEIGHTS_SUCCESS,
            1,
        )
    assert [
        {
            "block_from": r.block_from,
            "block_to": r.block_to,
        }
        for r in WeightSettingFinishedEvent.objects.all()
    ] == [
        {
            "block_from": 722 * cycle_number,
            "block_to": 722 * (cycle_number + 1),
        }
    ]


@patch("compute_horde_validator.validator.scoring.tasks.WEIGHT_SETTING_ATTEMPTS", 1)
@patch("compute_horde_validator.validator.scoring.tasks.WEIGHT_SETTING_FAILURE_BACKOFF", 0)
@pytest.mark.django_db(databases=["default", "default_alias"], transaction=True)
@patch_constance({"DYNAMIC_COMMIT_REVEAL_WEIGHTS_ENABLED": False})
def test_set_scores__set_weight_failure(settings, bittensor):
    bittensor.blocks.head.return_value.number = 723
    bittensor.subnet.return_value.weights.commit.side_effect = Exception

    with setup_db_and_scores():
        set_scores()

    assert SystemEvent.objects.using(settings.DEFAULT_DB_ALIAS).count() == 4
    check_system_events(
        SystemEvent.EventType.WEIGHT_SETTING_FAILURE,
        SystemEvent.EventSubType.SET_WEIGHTS_ERROR,
        1,
    )
    # end of retries system event
    check_system_events(
        SystemEvent.EventType.WEIGHT_SETTING_FAILURE, SystemEvent.EventSubType.GIVING_UP, 1
    )
    assert [
        {
            "block_from": r.block_from,
            "block_to": r.block_to,
        }
        for r in WeightSettingFinishedEvent.objects.all()
    ] == []


@patch("compute_horde_validator.validator.scoring.tasks.WEIGHT_SETTING_ATTEMPTS", 3)
@patch("compute_horde_validator.validator.scoring.tasks.WEIGHT_SETTING_FAILURE_BACKOFF", 0)
@pytest.mark.django_db(databases=["default", "default_alias"], transaction=True)
@patch_constance({"DYNAMIC_COMMIT_REVEAL_WEIGHTS_ENABLED": False})
def test_set_scores__set_weight_eventual_success(settings, bittensor):
    bittensor.blocks.head.return_value.number = 723
    bittensor.subnet.return_value.weights.commit.side_effect = (
        Exception,
        Exception,
        None,
    )

    with setup_db_and_scores():
        set_scores()

    assert SystemEvent.objects.using(settings.DEFAULT_DB_ALIAS).count() == 5
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


@patch("compute_horde_validator.validator.scoring.tasks.WEIGHT_SETTING_ATTEMPTS", 1)
@patch("compute_horde_validator.validator.scoring.tasks.WEIGHT_SETTING_FAILURE_BACKOFF", 0)
@pytest.mark.django_db(databases=["default", "default_alias"], transaction=True)
@patch_constance({"DYNAMIC_COMMIT_REVEAL_WEIGHTS_ENABLED": False})
def test_set_scores__set_weight__exception(settings, bittensor):
    bittensor.blocks.head.return_value.number = 723
    bittensor.subnet.return_value.weights.commit.side_effect = Exception

    with setup_db_and_scores():
        set_scores()

    assert SystemEvent.objects.using(settings.DEFAULT_DB_ALIAS).count() == 4
    check_system_events(
        SystemEvent.EventType.WEIGHT_SETTING_FAILURE,
        SystemEvent.EventSubType.SET_WEIGHTS_ERROR,
        1,
    )
    # end of retries system event
    check_system_events(
        SystemEvent.EventType.WEIGHT_SETTING_FAILURE, SystemEvent.EventSubType.GIVING_UP, 1
    )


@patch("compute_horde_validator.validator.scoring.tasks.WEIGHT_SETTING_ATTEMPTS", 1)
@patch("compute_horde_validator.validator.scoring.tasks.WEIGHT_SETTING_FAILURE_BACKOFF", 0)
@patch(
    "bittensor.subtensor",
    lambda *args, **kwargs: MockSubtensor(
        mocked_commit_weights=lambda: throw_error(),
        override_block_number=1200,
        hyperparameters=MockHyperparameters(
            commit_reveal_weights_enabled=True,
        ),
    ),
)
@pytest.mark.django_db(databases=["default", "default_alias"], transaction=True)
def test_set_scores__set_weight__commit__exception(bittensor):
    bittensor.blocks.head.return_value.number = 1200

    with setup_db_and_scores():
        set_scores()

    check_system_events(
        SystemEvent.EventType.WEIGHT_SETTING_FAILURE,
        SystemEvent.EventSubType.COMMIT_WEIGHTS_ERROR,
        1,
    )


@patch("compute_horde_validator.validator.scoring.tasks.WEIGHT_SETTING_ATTEMPTS", 1)
@patch("compute_horde_validator.validator.scoring.tasks.WEIGHT_SETTING_FAILURE_BACKOFF", 0)
@patch("compute_horde_validator.validator.scoring.tasks.WEIGHT_SETTING_HARD_TTL", 1)
@patch("compute_horde_validator.validator.scoring.tasks.WEIGHT_SETTING_TTL", 1)
@pytest.mark.django_db(databases=["default", "default_alias"], transaction=True)
@patch_constance({"DYNAMIC_COMMIT_REVEAL_WEIGHTS_ENABLED": False})
def test_set_scores__set_weight_timeout(settings, bittensor):
    bittensor.blocks.head.return_value.number = 723

    settings.CELERY_TASK_ALWAYS_EAGER = False  # to make it timeout

    with setup_db_and_scores():
        set_scores()

    assert SystemEvent.objects.using(settings.DEFAULT_DB_ALIAS).count() == 4
    check_system_events(
        SystemEvent.EventType.WEIGHT_SETTING_FAILURE,
        SystemEvent.EventSubType.WRITING_TO_CHAIN_TIMEOUT,
        1,
    )
    # end of retries system event
    check_system_events(
        SystemEvent.EventType.WEIGHT_SETTING_FAILURE, SystemEvent.EventSubType.GIVING_UP, 1
    )


@patch("compute_horde_validator.validator.scoring.tasks.WEIGHT_SETTING_ATTEMPTS", 1)
@patch("compute_horde_validator.validator.scoring.tasks.WEIGHT_SETTING_FAILURE_BACKOFF", 0)
@patch("compute_horde_validator.validator.scoring.tasks.WEIGHT_SETTING_HARD_TTL", 1)
@patch("compute_horde_validator.validator.scoring.tasks.WEIGHT_SETTING_TTL", 1)
@pytest.mark.django_db(databases=["default", "default_alias"], transaction=True)
@patch_constance({"DYNAMIC_COMMIT_REVEAL_WEIGHTS_INTERVAL": 722})
@pytest.mark.parametrize(
    "cycle_number,burn_rate,burn_partition,burn_targets,hotkey_to_score,expected_weights_committed",
    [
        # no burn, either effectively or by configuration:
        (0, 0.0, 0.0, "", None, {1: 16384, 2: 32768, 3: 49151, 4: 65535}),
        (0, 0.5, 0.5, "", None, {1: 16384, 2: 32768, 3: 49151, 4: 65535}),
        (0, 0.0, 0.5, "hotkey_1", None, {1: 16384, 2: 32768, 3: 49151, 4: 65535}),
        (0, 0.5, 0.5, "hotkey_100", None, {1: 16384, 2: 32768, 3: 49151, 4: 65535}),
        (0, 0.5, 0.5, "hotkey_100,hotkey_200", None, {1: 16384, 2: 32768, 3: 49151, 4: 65535}),
        # burn burn:
        (
            0,
            0.9,
            0.9,
            "hotkey_1,hotkey_2",
            {"hotkey_0": 70, "hotkey_3": 70, "hotkey_4": 100},
            {0: 2360, 1: 7282, 2: 65535, 3: 2360, 4: 3371},
        ),
        (
            0,
            0.9,
            0.9,
            "hotkey_1,hotkey_2",
            {"hotkey_0": 140, "hotkey_3": 140, "hotkey_4": 200},
            {0: 2360, 1: 7282, 2: 65535, 3: 2360, 4: 3371},
        ),
        (
            1,
            0.9,
            0.9,
            "hotkey_1,hotkey_2",
            {"hotkey_0": 70, "hotkey_3": 70, "hotkey_4": 100},
            {0: 2360, 1: 65535, 2: 7282, 3: 2360, 4: 3371},
        ),
        (
            1,
            0.9,
            0.9,
            "hotkey_1,hotkey_200",
            {"hotkey_0": 70, "hotkey_3": 70, "hotkey_4": 100},
            {0: 2124, 1: 65535, 3: 2124, 4: 3034},
        ),
        (
            0,
            0.9,
            0.9,
            "hotkey_1,hotkey_3",
            {"hotkey_0": 70, "hotkey_3": 70, "hotkey_4": 100},
            {0: 2278, 1: 7029, 3: 65535, 4: 3254},
        ),
        (
            0,
            0.9,
            0.9,
            "hotkey_1,hotkey_3",
            {"hotkey_0": 70, "hotkey_1": 70, "hotkey_2": 70, "hotkey_3": 70, "hotkey_4": 100},
            {0: 1457, 1: 8577, 2: 1457, 3: 65535, 4: 2082},
        ),
        (
            0,
            0.9,
            0.9,
            "hotkey_1,hotkey_2,hotkey_3",
            {"hotkey_0": 70, "hotkey_1": 70, "hotkey_2": 70, "hotkey_3": 70, "hotkey_4": 100},
            {0: 1457, 1: 5017, 2: 65535, 3: 5017, 4: 2082},
        ),
        (
            0,
            0.9,
            0.9,
            "hotkey_1,hotkey_2,hotkey_3",
            {"hotkey_0": 70, "hotkey_4": 100},
            {0: 3331, 1: 3641, 2: 65535, 3: 3641, 4: 4759},
        ),
        (
            0,
            0.9,
            0.9,
            "hotkey_1,hotkey_3,hotkey_200",
            {"hotkey_0": 70, "hotkey_3": 70, "hotkey_4": 100},
            {0: 2278, 1: 7029, 3: 65535, 4: 3254},
        ),
        (
            0,
            0.9,
            0.9,
            "hotkey_1,hotkey_3,hotkey_200",
            {"hotkey_0": 70, "hotkey_3": 70, "hotkey_4": 100},
            {0: 2278, 1: 7029, 3: 65535, 4: 3254},
        ),
        # burn burn again but params are different
        (
            0,
            0.7,
            0.8,
            "hotkey_1,hotkey_2",
            {"hotkey_0": 70, "hotkey_3": 70, "hotkey_4": 100},
            {0: 10240, 1: 16384, 2: 65535, 3: 10240, 4: 14628},
        ),
        (
            0,
            0.7,
            0.8,
            "hotkey_1,hotkey_2",
            {"hotkey_0": 140, "hotkey_3": 140, "hotkey_4": 200},
            {0: 10240, 1: 16384, 2: 65535, 3: 10240, 4: 14628},
        ),
        (
            1,
            0.7,
            0.8,
            "hotkey_1,hotkey_200",
            {"hotkey_0": 70, "hotkey_3": 70, "hotkey_4": 100},
            {0: 8192, 1: 65535, 3: 8192, 4: 11703},
        ),
        (
            0,
            0.7,
            0.8,
            "hotkey_1,hotkey_2,hotkey_3",
            {"hotkey_0": 70, "hotkey_1": 70, "hotkey_2": 70, "hotkey_3": 70, "hotkey_4": 100},
            {0: 5886, 1: 13342, 2: 65535, 3: 13342, 4: 8409},
        ),
        (
            0,
            0.7,
            0.8,
            "hotkey_1,hotkey_2,hotkey_3",
            {"hotkey_0": 70, "hotkey_4": 100},
            {0: 14456, 1: 8192, 2: 65535, 3: 8192, 4: 20652},
        ),
    ],
)
@override_settings(
    BITTENSOR_NETUID=359
)  # these test cases were written assuming RNGs are being seeded with the right
# block numbers here and there and now that the code calculating the cycle start/stop has been refactored
# in order to get the expected values we need to meddle with the NETUID
def test_set_scores__set_weight__commit(
    settings,
    bittensor,
    cycle_number,
    burn_rate,
    burn_partition,
    burn_targets,
    hotkey_to_score,
    expected_weights_committed,
):
    current_block = 1084 + cycle_number * 722

    subtensor_ = MockSubtensor(
        override_block_number=current_block,
        hyperparameters=MockHyperparameters(
            commit_reveal_weights_enabled=True,
        ),
    )
    bittensor.blocks.head.return_value.number = current_block

    with (
        patch("bittensor.subtensor", lambda *a, **kw: subtensor_),
        patch(
            "compute_horde_validator.validator.scoring.engine.calculate_allowance_paid_job_scores",
            return_value={DEFAULT_EXECUTOR_CLASS: hotkey_to_score},
        ),
        setup_db_and_scores(hotkey_to_score=hotkey_to_score),
        override_config(
            DYNAMIC_BURN_TARGET_SS58ADDRESSES=burn_targets,
            DYNAMIC_BURN_RATE=burn_rate,
            DYNAMIC_BURN_PARTITION=burn_partition,
        ),
    ):
        set_scores()

        assert subtensor_.weights_committed == [expected_weights_committed]

        from_db = Weights.objects.get()
        assert from_db.block == current_block
        assert from_db.revealed_at is None

        assert SystemEvent.objects.using(settings.DEFAULT_DB_ALIAS).count() == 3, (
            SystemEvent.objects.all()
        )
        check_system_events(
            SystemEvent.EventType.WEIGHT_SETTING_SUCCESS,
            SystemEvent.EventSubType.COMMIT_WEIGHTS_SUCCESS,
            1,
        )
    assert [
        {
            "block_from": r.block_from,
            "block_to": r.block_to,
        }
        for r in WeightSettingFinishedEvent.objects.all()
    ] == [
        {
            "block_from": 722 * cycle_number,
            "block_to": 722 * (cycle_number + 1),
        }
    ]


@pytest.mark.parametrize("current_block", [723, 999, 1082, 1430, 1443])
@patch("compute_horde_validator.validator.scoring.tasks.WEIGHT_SETTING_ATTEMPTS", 1)
@patch("compute_horde_validator.validator.scoring.tasks.WEIGHT_SETTING_FAILURE_BACKOFF", 0)
@patch("compute_horde_validator.validator.scoring.tasks.WEIGHT_SETTING_HARD_TTL", 1)
@patch("compute_horde_validator.validator.scoring.tasks.WEIGHT_SETTING_TTL", 1)
@pytest.mark.django_db(databases=["default", "default_alias"], transaction=True)
def test_set_scores__set_weight__commit__too_early_or_too_late(bittensor, current_block: int):
    bittensor.blocks.head.return_value.number = current_block
    subtensor_ = MockSubtensor(
        override_block_number=current_block,
        hyperparameters=MockHyperparameters(
            commit_reveal_weights_enabled=True,
        ),
    )

    with (
        setup_db_and_scores(),
        patch("bittensor.subtensor", return_value=subtensor_),
    ):
        set_scores()

        assert not subtensor_.weights_committed

        assert not Weights.objects.exists()
        assert not SystemEvent.objects.exists()


@pytest.mark.django_db(databases=["default", "default_alias"], transaction=True)
@pytest.mark.parametrize(
    "reveal_block",
    [
        # interval start is 722
        # reveal window is 1444 - 1790 (722 + 722 + 361 - 15 end buffer)
        1444,  # first block in the window
        1678,
        1789,
    ],
)
def test_set_scores__set_weight__reveal__in_time(bittensor, reveal_block: int):
    current_block = 1234
    subtensor_ = MockSubtensor(
        override_block_number=current_block,
        hyperparameters=MockHyperparameters(
            commit_reveal_weights_enabled=True,
        ),
    )
    bittensor.blocks.head.return_value.number = current_block

    with (
        setup_db_and_scores(),
        patch("bittensor.subtensor", return_value=subtensor_),
    ):
        set_scores()

        last_weights = Weights.objects.order_by("-id").first()
        assert last_weights
        assert last_weights.revealed_at is None
        assert last_weights.block == current_block

        subtensor_.override_block_number = reveal_block
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


@pytest.mark.parametrize(
    "reveal_block",
    [
        # interval start is 722
        # reveal window is 1444 - 1790 (722 + 722 + 361 - 15 end buffer)
        1443,  # too early
        1791,  # too late
    ],
)
@pytest.mark.django_db(databases=["default", "default_alias"], transaction=True)
def test_set_scores__set_weight__reveal__out_of_window(bittensor, reveal_block: int):
    current_block = 1234

    subtensor_ = MockSubtensor(
        override_block_number=current_block,
        hyperparameters=MockHyperparameters(
            commit_reveal_weights_enabled=True,
        ),
    )
    bittensor.blocks.head.return_value.number = current_block

    with (
        setup_db_and_scores(),
        patch("bittensor.subtensor", return_value=subtensor_),
    ):
        set_scores()

        last_weights = Weights.objects.get()
        assert last_weights
        assert last_weights.revealed_at is None
        assert last_weights.block == current_block

        subtensor_.override_block_number = reveal_block

        reveal_scores()

    last_weights.refresh_from_db()
    assert last_weights.revealed_at is None
    check_system_events(
        SystemEvent.EventType.WEIGHT_SETTING_FAILURE,
        SystemEvent.EventSubType.REVEAL_WEIGHTS_ERROR,
        0,  # nothing happened because it's too early to reveal weights
    )


# TODO: Redesign this test
@pytest.mark.skip
@pytest.mark.django_db(databases=["default", "default_alias"], transaction=True)
def test_set_scores__set_weight__reveal__timeout(bittensor, run_uuid):
    subtensor_ = MockSubtensor(
        override_block_number=1234,
        hyperparameters=MockHyperparameters(
            commit_reveal_weights_enabled=True,
        ),
    )
    bittensor.blocks.head.return_value.number = 1234

    with (
        setup_db_and_scores(),
        patch("bittensor.subtensor", return_value=subtensor_),
    ):
        set_scores()
        last_weights = Weights.objects.order_by("-id").first()
        assert last_weights
        assert last_weights.revealed_at is None
        max_system_event_id_before = SystemEvent.objects.all().aggregate(Max("id"))["id__max"]
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
        system_events = list(
            SystemEvent.objects.filter(id__gt=max_system_event_id_before or 0)
            .values_list("type", "subtype")
            .order_by("id")
        )
        assert any(
            system_events[-len(expected_events) :] == expected_events
            for expected_events in [
                [
                    ("WEIGHT_SETTING_FAILURE", "REVEAL_WEIGHTS_ERROR"),
                    ("WEIGHT_SETTING_FAILURE", "REVEAL_WEIGHTS_ERROR"),
                    ("WEIGHT_SETTING_FAILURE", "REVEAL_WEIGHTS_ERROR"),
                    ("WEIGHT_SETTING_SUCCESS", "REVEAL_WEIGHTS_SUCCESS"),
                ],
                [
                    ("WEIGHT_SETTING_FAILURE", "WRITING_TO_CHAIN_TIMEOUT"),
                    ("WEIGHT_SETTING_FAILURE", "REVEAL_WEIGHTS_ERROR"),
                    ("WEIGHT_SETTING_FAILURE", "REVEAL_WEIGHTS_ERROR"),
                    ("WEIGHT_SETTING_SUCCESS", "REVEAL_WEIGHTS_SUCCESS"),
                ],
                [
                    ("WEIGHT_SETTING_FAILURE", "REVEAL_WEIGHTS_ERROR"),
                    ("WEIGHT_SETTING_FAILURE", "WRITING_TO_CHAIN_TIMEOUT"),
                    ("WEIGHT_SETTING_FAILURE", "REVEAL_WEIGHTS_ERROR"),
                    ("WEIGHT_SETTING_FAILURE", "REVEAL_WEIGHTS_ERROR"),
                    ("WEIGHT_SETTING_SUCCESS", "REVEAL_WEIGHTS_SUCCESS"),
                ],
            ]
        ), system_events


@patch("compute_horde_validator.validator.scoring.tasks.WEIGHT_SETTING_ATTEMPTS", 1)
@patch("compute_horde_validator.validator.scoring.tasks.WEIGHT_SETTING_FAILURE_BACKOFF", 0)
@patch("compute_horde_validator.validator.scoring.tasks.WEIGHT_SETTING_HARD_TTL", 1)
@patch("compute_horde_validator.validator.scoring.tasks.WEIGHT_SETTING_TTL", 1)
@pytest.mark.django_db(databases=["default", "default_alias"], transaction=True)
def test_set_scores__multiple_starts(settings, bittensor):
    bittensor.blocks.head.return_value.number = 1234

    # to ensure the other tasks will be run at the same time
    settings.CELERY_TASK_ALWAYS_EAGER = False
    threads = 5

    with (
        setup_db_and_scores(),
        concurrent.futures.ThreadPoolExecutor(max_workers=threads) as pool,
    ):
        for _ in range(threads):
            pool.submit(set_scores)

    assert SystemEvent.objects.using(settings.DEFAULT_DB_ALIAS).count() == 4

    check_system_events(
        SystemEvent.EventType.WEIGHT_SETTING_FAILURE,
        SystemEvent.EventSubType.WRITING_TO_CHAIN_TIMEOUT,
        1,
    )
    # end of retries system event
    check_system_events(
        SystemEvent.EventType.WEIGHT_SETTING_FAILURE, SystemEvent.EventSubType.GIVING_UP, 1
    )
