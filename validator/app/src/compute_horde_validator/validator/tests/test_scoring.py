from datetime import timedelta

import pytest
from django.utils import timezone
from django.utils.timezone import now

from compute_horde_validator.validator.models import (
    Cycle,
    Miner,
    MinerManifest,
    OrganicJob,
    SyntheticJob,
    SyntheticJobBatch,
)
from compute_horde_validator.validator.scoring import ExecutorClass, score_batches

EXECUTOR_CLASS_WEIGHTS_OVERRIDE = "spin_up-4min.gpu-24gb=8,always_on.gpu-24gb=2"


@pytest.fixture
def setup_data():
    # Create miners
    miner1 = Miner.objects.create(hotkey="hotkey1")
    miner2 = Miner.objects.create(hotkey="hotkey2")
    miner3 = Miner.objects.create(hotkey="hotkey3")
    miner4 = Miner.objects.create(hotkey="hotkey4")

    # Create batch
    batch = SyntheticJobBatch.objects.create(
        accepting_results_until=timezone.now() + timedelta(hours=1),
        cycle=Cycle.objects.create(start=708, stop=1430),
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


@pytest.mark.override_config(DYNAMIC_EXECUTOR_CLASS_WEIGHTS=EXECUTOR_CLASS_WEIGHTS_OVERRIDE)
@pytest.mark.django_db(databases=["default", "default_alias"], transaction=True)
def test_score_batches_basic(setup_data):
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


@pytest.mark.override_config(DYNAMIC_EXECUTOR_CLASS_WEIGHTS=EXECUTOR_CLASS_WEIGHTS_OVERRIDE)
@pytest.mark.django_db(databases=["default", "default_alias"], transaction=True)
def test_score_batches_with_changed_params_avg(setup_data, settings):
    batch = setup_data

    settings.HORDE_SCORE_AVG_PARAM = (
        2  # Increase importance of average score (use avg**2 for better effect)
    )

    changed_scores = score_batches([batch])

    # With changed parameters, miner2 should now have the highest score
    assert changed_scores["hotkey2"] > changed_scores["hotkey1"]
    assert changed_scores["hotkey2"] > changed_scores["hotkey3"]
    assert changed_scores["hotkey2"] > changed_scores["hotkey4"]


@pytest.mark.override_config(DYNAMIC_EXECUTOR_CLASS_WEIGHTS=EXECUTOR_CLASS_WEIGHTS_OVERRIDE)
@pytest.mark.django_db(databases=["default", "default_alias"], transaction=True)
def test_score_batches_with_changed_params_horde_size(setup_data, settings):
    batch = setup_data

    settings.HORDE_SCORE_SIZE_PARAM = 1.75
    settings.HORDE_SCORE_CENTRAL_SIZE_PARAM = 20
    changed_scores = score_batches([batch])

    # With changed parameters, miner4 should now have the highest score
    assert changed_scores["hotkey4"] > changed_scores["hotkey1"]
    assert changed_scores["hotkey4"] > changed_scores["hotkey2"]
    assert changed_scores["hotkey4"] > changed_scores["hotkey3"]


@pytest.mark.override_config(DYNAMIC_EXECUTOR_CLASS_WEIGHTS=EXECUTOR_CLASS_WEIGHTS_OVERRIDE)
@pytest.mark.django_db(databases=["default", "default_alias"], transaction=True)
def test_score_batches_executor_classes_weights():
    miner1 = Miner.objects.create(hotkey="hotkey1")
    miner2 = Miner.objects.create(hotkey="hotkey2")

    # Create batch
    batch = SyntheticJobBatch.objects.create(
        accepting_results_until=timezone.now() + timedelta(hours=1),
        cycle=Cycle.objects.create(start=708, stop=1430),
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


@pytest.mark.override_config(DYNAMIC_EXECUTOR_CLASS_WEIGHTS=EXECUTOR_CLASS_WEIGHTS_OVERRIDE)
@pytest.mark.django_db(databases=["default", "default_alias"], transaction=True)
def test_rejected_synthetic_jobs_scored():
    miner1 = Miner.objects.create(hotkey="hotkey1")
    miner2 = Miner.objects.create(hotkey="hotkey2")

    # Create batch
    batch = SyntheticJobBatch.objects.create(
        accepting_results_until=timezone.now() + timedelta(hours=1),
        cycle=Cycle.objects.create(start=708, stop=1430),
    )

    # Common job parameters
    common_params = {
        "miner_address": "127.0.0.1",
        "miner_address_ip_version": 4,
        "miner_port": 8080,
        "batch": batch,
        "executor_class": ExecutorClass.spin_up_4min__gpu_24gb,
    }

    # create a completed job and a rejected job for two different miners
    SyntheticJob.objects.create(
        miner=miner1,
        status=SyntheticJob.Status.COMPLETED,
        score=1,
        **common_params,
    )
    SyntheticJob.objects.create(
        miner=miner2,
        status="PROPERLY_REJECTED",  # TODO: fix after merging with Kordian's PR
        score=1,
        **common_params,
    )

    scores = score_batches([batch])
    assert scores.get("hotkey1", 0) > 0
    assert scores.get("hotkey2", 0) > 0


@pytest.mark.override_config(DYNAMIC_EXECUTOR_CLASS_WEIGHTS=EXECUTOR_CLASS_WEIGHTS_OVERRIDE)
@pytest.mark.django_db(databases=["default", "default_alias"], transaction=True)
def test_organic_jobs_scored():
    miner1 = Miner.objects.create(hotkey="hotkey1")
    miner2 = Miner.objects.create(hotkey="hotkey2")

    # Create batch
    cycle = Cycle.objects.create(start=708, stop=1430)
    batch = SyntheticJobBatch.objects.create(
        accepting_results_until=timezone.now() + timedelta(hours=1),
        cycle=cycle,
    )

    # Common job parameters
    common_params = {
        "miner_address": "127.0.0.1",
        "miner_address_ip_version": 4,
        "miner_port": 8080,
        "executor_class": ExecutorClass.spin_up_4min__gpu_24gb,
        "status": SyntheticJob.Status.COMPLETED,
    }

    # create a completed job and a rejected job for two different miners
    SyntheticJob.objects.create(
        miner=miner1,
        batch=batch,
        score=1,
        **common_params,
    )
    SyntheticJob.objects.create(
        miner=miner2,
        batch=batch,
        score=1,
        **common_params,
    )
    OrganicJob.objects.create(
        miner=miner2,
        block=1000,
        **common_params,
    )

    scores = score_batches([batch])
    assert scores.get("hotkey1", 0) > 0
    assert scores.get("hotkey2", 0) > 0
    assert scores.get("hotkey2", 0) > scores.get("hotkey1", 0)


def create_batch(n: int, cycle: Cycle) -> SyntheticJobBatch:
    """create a batch with n completed jobs"""
    miner, _ = Miner.objects.get_or_create(hotkey="miner_hotkey")
    batch = SyntheticJobBatch.objects.create(
        accepting_results_until=now(),
        cycle=cycle,
        scored=True,
    )

    MinerManifest.objects.create(
        miner=miner,
        batch=batch,
        executor_class=ExecutorClass.always_on__llm__a6000,
        executor_count=n,
        online_executor_count=n,
    )

    for _ in range(n):
        SyntheticJob.objects.create(
            miner=miner,
            miner_address="127.0.0.1",
            miner_address_ip_version=4,
            miner_port=8080,
            status=SyntheticJob.Status.COMPLETED,
            batch=batch,
            executor_class=ExecutorClass.always_on__llm__a6000,
            score=1,
        )

    return batch


@pytest.mark.override_config(
    DYNAMIC_EXECUTOR_CLASS_WEIGHTS="always_on.llm.a6000=100",
    DYNAMIC_MANIFEST_SCORE_MULTIPLIER=1.5,
    DYNAMIC_MANIFEST_DANCE_RATIO_THRESHOLD=2.0,
)
@pytest.mark.django_db(databases=["default", "default_alias"], transaction=True)
@pytest.mark.parametrize(
    ("prev_online_executor_count", "expected_multiplier"),
    [
        (None, 1.5),
        (5, 1.5),
        (7, 1.0),
        (15, 1.0),
        (20, 1.5),
    ],
)
def test_manifest_dance_incentives(
    prev_online_executor_count,
    expected_multiplier,
    override_weights_version_v2,
):
    if prev_online_executor_count is not None:
        _prev_batch = create_batch(
            prev_online_executor_count, Cycle.objects.create(start=708, stop=1430)
        )

    curr_batch = create_batch(10, Cycle.objects.create(start=1430, stop=2152))

    scores = score_batches([curr_batch])
    assert "miner_hotkey" in scores
    assert abs(scores["miner_hotkey"] - 100 * expected_multiplier) < 0.0001


@pytest.mark.override_config(
    DYNAMIC_EXECUTOR_CLASS_WEIGHTS="always_on.llm.a6000=100",
    DYNAMIC_MANIFEST_SCORE_MULTIPLIER=1.5,
    DYNAMIC_MANIFEST_DANCE_RATIO_THRESHOLD=2.0,
)
@pytest.mark.django_db(databases=["default", "default_alias"], transaction=True)
def test_manifest_dance_incentives_disabled_on_v1(override_weights_version_v1):
    _prev_batch = create_batch(1, Cycle.objects.create(start=708, stop=1430))
    curr_batch = create_batch(5, Cycle.objects.create(start=1430, stop=2152))

    scores = score_batches([curr_batch])
    assert "miner_hotkey" in scores

    # there should be no bonus
    assert abs(scores["miner_hotkey"] - 100) < 0.0001
