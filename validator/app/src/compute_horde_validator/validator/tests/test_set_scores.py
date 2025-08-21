import concurrent.futures
import os
import uuid
from unittest.mock import patch

import pytest
from compute_horde.executor_class import DEFAULT_EXECUTOR_CLASS
from constance.test.pytest import override_config
from django.test import override_settings
from django.utils.timezone import now
from pylon_client import PylonClient

from compute_horde_validator.validator.models import (
    Cycle,
    Miner,
    MinerManifest,
    SyntheticJob,
    SyntheticJobBatch,
    SystemEvent,
    Weights,
)
from compute_horde_validator.validator.tasks import (
    _normalize_weights_for_committing,
    set_scores,
)

from .helpers import (
    NUM_NEURONS,
    check_system_events,
)


@pytest.fixture
def mock_pylon_client():
    """Create a mock PylonClient with built-in mocking capabilities."""
    mock_data_path = os.path.join(os.path.dirname(__file__), "pylon_mock_data.json")
    return PylonClient(mock_data_path=mock_data_path)


@pytest.fixture(autouse=True)
def _default_commit_reveal_params():
    with (
        override_settings(
            COMMIT_WINDOW_END_BUFFER=15,
            COMMIT_NUM_EPOCHS_PER_CYLE=2,
            COMMIT_WEIGHTS_INTERVAL=722,
            COMMIT_WINDOW_START_OFFSET=361,
        ),
        override_config(
            DYNAMIC_BURN_TARGET_SS58ADDRESSES="",
            DYNAMIC_BURN_RATE=0.0,
            DYNAMIC_BURN_PARTITION=0.0,
        ),
    ):
        yield


def setup_db(cycle_number=0, hotkey_to_score=None):
    if hotkey_to_score is None:
        hotkey_to_score = {f"hotkey_{i}": i for i in range(NUM_NEURONS)}
    for i in range(NUM_NEURONS):
        Miner.objects.update_or_create(hotkey=f"hotkey_{i}")

    job_batch = SyntheticJobBatch.objects.create(
        started_at=now(),
        accepting_results_until=now(),
        scored=False,
        block=722 * cycle_number + 1,
        cycle=Cycle.objects.create(start=722 * cycle_number, stop=722 * (cycle_number + 1)),
    )
    for hotkey, score in hotkey_to_score.items():
        SyntheticJob.objects.create(
            batch=job_batch,
            score=score,
            job_uuid=uuid.uuid4(),
            miner=Miner.objects.get(hotkey=hotkey),
            miner_address="ignore",
            miner_address_ip_version=4,
            miner_port=9999,
            executor_class=DEFAULT_EXECUTOR_CLASS,
            status=SyntheticJob.Status.COMPLETED,
        )
        MinerManifest.objects.create(
            miner=Miner.objects.get(hotkey=hotkey),
            batch=job_batch,
            executor_class=DEFAULT_EXECUTOR_CLASS,
            executor_count=1,
            online_executor_count=1,
        )


@pytest.mark.django_db
def test_normalize_scores():
    assert _normalize_weights_for_committing([0.5, 1.5, 100.1, 30], 65535) == [
        327,
        982,
        65535,
        19641,
    ]


@pytest.mark.django_db(databases=["default", "default_alias"], transaction=True)
def test_set_scores__no_batches_found(settings, bittensor, mock_pylon_client):
    with patch(
        "compute_horde_validator.validator.tasks.pylon_client", return_value=mock_pylon_client
    ):
        mock_pylon_client.override("get_latest_block", 361)
        set_scores()
        assert SystemEvent.objects.using(settings.DEFAULT_DB_ALIAS).count() == 0


@pytest.mark.django_db(databases=["default", "default_alias"], transaction=True)
def test_set_scores__too_early(settings, bittensor, mock_pylon_client):
    with patch(
        "compute_horde_validator.validator.tasks.pylon_client", return_value=mock_pylon_client
    ):
        mock_pylon_client.override("get_latest_block", 359)
        setup_db()
        set_scores()
        assert SystemEvent.objects.using(settings.DEFAULT_DB_ALIAS).count() == 0


@pytest.mark.django_db(databases=["default", "default_alias"], transaction=True)
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
def test_set_scores__weight_commit_success(
    settings,
    bittensor,
    mock_pylon_client,
    cycle_number,
    burn_rate,
    burn_partition,
    burn_targets,
    hotkey_to_score,
    expected_weights_committed,
):
    current_block = 1084 + cycle_number * 722

    with patch(
        "compute_horde_validator.validator.tasks.pylon_client", return_value=mock_pylon_client
    ):
        mock_pylon_client.override("get_latest_block", current_block)

        setup_db(cycle_number=cycle_number, hotkey_to_score=hotkey_to_score)
        with override_config(
            DYNAMIC_BURN_TARGET_SS58ADDRESSES=burn_targets,
            DYNAMIC_BURN_RATE=burn_rate,
            DYNAMIC_BURN_PARTITION=burn_partition,
        ):
            set_scores()

        # Verify pylon client was called with correct weights
        mock_pylon_client.mock.set_weights.assert_called_once()
        actual_weights = mock_pylon_client.mock.set_weights.call_args.kwargs.get("weights", {})

        expected_weights_set = expected_weights_committed
        for uid, w in expected_weights_set.items():
            actual_w = actual_weights.get(uid)
            if actual_w is not None and ((w - actual_w) / w < 0.001):
                expected_weights_set[uid] = actual_w

        # Verify database state - Weights object should still be created
        from_db = Weights.objects.get()
        assert from_db.block == current_block

        assert SystemEvent.objects.using(settings.DEFAULT_DB_ALIAS).count() == 2
        check_system_events(
            SystemEvent.EventType.WEIGHT_SETTING_SUCCESS,
            SystemEvent.EventSubType.COMMIT_WEIGHTS_SUCCESS,
            1,
        )


@pytest.mark.django_db(databases=["default", "default_alias"], transaction=True)
def test_set_scores__weight_commit_failure(settings, bittensor, mock_pylon_client):
    """Test that pylon client failures are properly handled."""
    with patch(
        "compute_horde_validator.validator.tasks.pylon_client", return_value=mock_pylon_client
    ):
        mock_pylon_client.override("get_latest_block", 1084)
        mock_pylon_client.mock.set_weights.side_effect = Exception("Internal Server Error")
        setup_db()
        set_scores()

        # Verify pylon client was called
        mock_pylon_client.mock.set_weights.assert_called_once()

        assert SystemEvent.objects.using(settings.DEFAULT_DB_ALIAS).count() == 2
        check_system_events(
            SystemEvent.EventType.WEIGHT_SETTING_FAILURE,
            SystemEvent.EventSubType.COMMIT_WEIGHTS_ERROR,
            1,
        )


@pytest.mark.parametrize("current_block", [723, 999, 1082, 1430, 1443])
@pytest.mark.django_db(databases=["default", "default_alias"], transaction=True)
def test_set_scores__commit__too_early_or_too_late(
    bittensor, current_block: int, mock_pylon_client
):
    with patch(
        "compute_horde_validator.validator.tasks.pylon_client", return_value=mock_pylon_client
    ):
        mock_pylon_client.override("get_latest_block", current_block)

        setup_db()
        set_scores()

        mock_pylon_client.mock.set_weights.assert_not_called()

        assert not Weights.objects.exists()
        assert not SystemEvent.objects.exists()


@pytest.mark.django_db(databases=["default", "default_alias"], transaction=True)
def test_set_scores__multiple_starts(settings, bittensor, mock_pylon_client):
    # to ensure the other tasks will be run at the same time
    settings.CELERY_TASK_ALWAYS_EAGER = False
    threads = 5

    with patch(
        "compute_horde_validator.validator.tasks.pylon_client", return_value=mock_pylon_client
    ):
        setup_db()

        with concurrent.futures.ThreadPoolExecutor(max_workers=threads) as pool:
            for _ in range(threads):
                pool.submit(set_scores)
        mock_pylon_client.mock.set_weights.assert_called_once()

    assert SystemEvent.objects.using(settings.DEFAULT_DB_ALIAS).count() == 2

    check_system_events(
        SystemEvent.EventType.WEIGHT_SETTING_SUCCESS,
        SystemEvent.EventSubType.COMMIT_WEIGHTS_SUCCESS,
        1,
    )
