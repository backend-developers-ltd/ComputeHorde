import concurrent.futures
from contextlib import contextmanager
from unittest.mock import patch

import pytest
from compute_horde.executor_class import DEFAULT_EXECUTOR_CLASS
from constance.test.pytest import override_config
from django.test import override_settings

from compute_horde_validator.validator.models import (
    Miner,
    SystemEvent,
)

from ...models.scoring.internal import Weights, WeightSettingFinishedEvent
from ...scoring.pylon_client import (
    MockPylonBehaviour,
    setup_mock_pylon_client,
)
from ...scoring.tasks import _normalize_weights_for_committing, set_scores
from ..helpers import (
    NUM_NEURONS,
    MockHyperparameters,
    MockSubtensor,
    check_system_events,
    patch_constance,
)
from ..utils_for_tests import lenient_float_factory

LF = lenient_float_factory(1e-3, 1e-3)


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
        return {uid: LF(w / total) for uid, w in weights.items()}

    bittensor.blocks.head.return_value.number = 723 + cycle_number * 722

    with setup_db_and_scores(hotkey_to_score=hotkey_to_score):
        with override_config(
            DYNAMIC_BURN_TARGET_SS58ADDRESSES=burn_targets,
            DYNAMIC_BURN_RATE=burn_rate,
            DYNAMIC_BURN_PARTITION=burn_partition,
        ):
            with setup_mock_pylon_client() as mock_pylon_client:
                set_scores()
        assert SystemEvent.objects.using(settings.DEFAULT_DB_ALIAS).count() == 3

        assert mock_pylon_client.weights_submitted == [
            {
                f"hotkey_{uid}": weight
                for uid, weight in _normalize_weights(expected_weights_set).items()
            }
        ]

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
@setup_mock_pylon_client([MockPylonBehaviour.raise_])
def test_set_scores__set_weight_failure(settings, bittensor):
    bittensor.blocks.head.return_value.number = 723

    with setup_db_and_scores():
        set_scores()

    assert SystemEvent.objects.using(settings.DEFAULT_DB_ALIAS).count() == 4
    check_system_events(
        SystemEvent.EventType.WEIGHT_SETTING_FAILURE,
        SystemEvent.EventSubType.WRITING_TO_CHAIN_GENERIC_ERROR,
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
@setup_mock_pylon_client(
    [
        MockPylonBehaviour.raise_,
        MockPylonBehaviour.raise_,
        MockPylonBehaviour.work_fine_forever,
    ]
)
def test_set_scores__set_weight_eventual_success(settings, bittensor):
    bittensor.blocks.head.return_value.number = 723

    with setup_db_and_scores():
        set_scores()

    assert SystemEvent.objects.using(settings.DEFAULT_DB_ALIAS).count() == 5
    check_system_events(
        SystemEvent.EventType.WEIGHT_SETTING_FAILURE,
        SystemEvent.EventSubType.WRITING_TO_CHAIN_GENERIC_ERROR,
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
@setup_mock_pylon_client(
    [
        MockPylonBehaviour.raise_,
    ]
)
def test_set_scores__set_weight__exception(settings, bittensor):
    bittensor.blocks.head.return_value.number = 723

    with setup_db_and_scores():
        set_scores()

    assert SystemEvent.objects.using(settings.DEFAULT_DB_ALIAS).count() == 4
    check_system_events(
        SystemEvent.EventType.WEIGHT_SETTING_FAILURE,
        SystemEvent.EventSubType.WRITING_TO_CHAIN_GENERIC_ERROR,
        1,
    )
    # end of retries system event
    check_system_events(
        SystemEvent.EventType.WEIGHT_SETTING_FAILURE, SystemEvent.EventSubType.GIVING_UP, 1
    )


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
        SystemEvent.EventSubType.WRITING_TO_CHAIN_GENERIC_ERROR,
        1,
    )
    # end of retries system event
    check_system_events(
        SystemEvent.EventType.WEIGHT_SETTING_FAILURE, SystemEvent.EventSubType.GIVING_UP, 1
    )
