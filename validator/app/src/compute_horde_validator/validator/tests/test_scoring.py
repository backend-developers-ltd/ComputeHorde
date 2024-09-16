from datetime import timedelta
from unittest.mock import patch

import pytest
from django.utils import timezone

from compute_horde_validator.validator.models import Miner, SyntheticJob, SyntheticJobBatch
from compute_horde_validator.validator.scoring import ExecutorClass, score_batches


@pytest.fixture
def setup_data():
    # Create miners
    miner1 = Miner.objects.create(hotkey="hotkey1")
    miner2 = Miner.objects.create(hotkey="hotkey2")
    miner3 = Miner.objects.create(hotkey="hotkey3")
    miner4 = Miner.objects.create(hotkey="hotkey4")

    # Create batch
    batch = SyntheticJobBatch.objects.create(
        accepting_results_until=timezone.now() + timedelta(hours=1)
    )

    # Common job parameters
    common_params = {
        "miner_address": "127.0.0.1",
        "miner_address_ip_version": 4,
        "miner_port": 8080,
        "status": SyntheticJob.Status.COMPLETED,
        "batch": batch,
    }

    # Create jobs for miner1 (large horde of weaker executors)
    for _ in range(11):
        SyntheticJob.objects.create(
            miner=miner1,
            executor_class=ExecutorClass.spin_up_4min__gpu_24gb,
            score=6,
            **common_params,
        )

    # Create jobs for miner2 (smaller horde of stronger executors)
    for _ in range(5):
        SyntheticJob.objects.create(
            miner=miner2,
            executor_class=ExecutorClass.spin_up_4min__gpu_24gb,
            score=12,
            **common_params,
        )

    # Create jobs for miner3 (mix of spin-up and always-on)
    for _ in range(11):
        SyntheticJob.objects.create(
            miner=miner3,
            executor_class=ExecutorClass.spin_up_4min__gpu_24gb,
            score=6,
            **common_params,
        )
    SyntheticJob.objects.create(
        miner=miner3, executor_class=ExecutorClass.always_on__gpu_24gb, score=15, **common_params
    )

    # Create jobs for miner4 (very large horde of very weak executors)
    for _ in range(40):
        SyntheticJob.objects.create(
            miner=miner4,
            executor_class=ExecutorClass.spin_up_4min__gpu_24gb,
            score=1,
            **common_params,
        )

    return batch


@pytest.fixture
def mocked_executor_class_weights():
    mocked_weights = {
        ExecutorClass.spin_up_4min__gpu_24gb: 8,
        ExecutorClass.always_on__gpu_24gb: 2,
    }
    with patch("compute_horde_validator.validator.scoring.EXECUTOR_CLASS_WEIGHTS", mocked_weights):
        yield mocked_weights


@pytest.mark.django_db(databases=["default", "default_alias"], transaction=True)
def test_score_batches_basic(setup_data, mocked_executor_class_weights):
    batch = setup_data
    scores = score_batches([batch])

    assert len(scores) == 4
    assert all(isinstance(score, float) for score in scores.values())
    assert all(score > 0 for score in scores.values())

    # 6 * 11 > 5 * 12
    assert scores["hotkey1"] > scores["hotkey2"]

    # hotkey3 has the same spin_up horde, but additional always_on - so he wins
    assert scores["hotkey3"] > scores["hotkey1"]

    # 6 * 11 > 40 * 1
    assert scores["hotkey1"] > scores["hotkey4"]


@pytest.mark.django_db(databases=["default", "default_alias"], transaction=True)
def test_score_batches_with_changed_params_avg(setup_data, settings, mocked_executor_class_weights):
    batch = setup_data

    settings.HORDE_SCORE_AVG_PARAM = (
        2  # Increase importance of average score (use avg**2 for better effect)
    )

    changed_scores = score_batches([batch])

    # With changed parameters, miner2 should now have the highest score
    assert changed_scores["hotkey2"] > changed_scores["hotkey1"]
    assert changed_scores["hotkey2"] > changed_scores["hotkey3"]
    assert changed_scores["hotkey2"] > changed_scores["hotkey4"]


@pytest.mark.django_db(databases=["default", "default_alias"], transaction=True)
def test_score_batches_with_changed_params_horde_size(
    setup_data, settings, mocked_executor_class_weights
):
    batch = setup_data

    settings.HORDE_SCORE_SIZE_PARAM = 1.75
    settings.HORDE_SCORE_CENTRAL_SIZE_PARAM = 20
    changed_scores = score_batches([batch])

    # With changed parameters, miner4 should now have the highest score
    assert changed_scores["hotkey4"] > changed_scores["hotkey1"]
    assert changed_scores["hotkey4"] > changed_scores["hotkey2"]
    assert changed_scores["hotkey4"] > changed_scores["hotkey3"]


@pytest.mark.django_db(databases=["default", "default_alias"], transaction=True)
def test_score_batches_executor_classes_weights(mocked_executor_class_weights):
    miner1 = Miner.objects.create(hotkey="hotkey1")
    miner2 = Miner.objects.create(hotkey="hotkey2")

    # Create batch
    batch = SyntheticJobBatch.objects.create(
        accepting_results_until=timezone.now() + timedelta(hours=1)
    )

    # Common job parameters
    common_params = {
        "miner_address": "127.0.0.1",
        "miner_address_ip_version": 4,
        "miner_port": 8080,
        "status": SyntheticJob.Status.COMPLETED,
        "batch": batch,
    }

    # create two jobs for different miners in different executor classes
    SyntheticJob.objects.create(
        miner=miner1,
        executor_class=ExecutorClass.spin_up_4min__gpu_24gb,
        score=0.1,
        **common_params,
    )
    SyntheticJob.objects.create(
        miner=miner2, executor_class=ExecutorClass.always_on__gpu_24gb, score=120, **common_params
    )

    scores = score_batches([batch])
    total = scores["hotkey1"] + scores["hotkey2"]
    assert 0.8 - 0.01 < scores["hotkey1"] / total < 0.8 + 0.01
    assert 0.2 - 0.01 < scores["hotkey2"] / total < 0.2 + 0.01
