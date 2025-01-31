from datetime import timedelta

import pytest
from compute_horde.subtensor import get_cycle_containing_block
from django.utils import timezone
from django.utils.timezone import now
from pytest import approx

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
DYNAMIC_MANIFEST_SCORE_MULTIPLIER = 1.5
DYNAMIC_MANIFEST_DANCE_RATIO_THRESHOLD = 2.0


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


@pytest.mark.override_config(DYNAMIC_EXECUTOR_CLASS_WEIGHTS="always_on.llm.a6000=100")
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
        "executor_class": ExecutorClass.always_on__llm__a6000,
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
        status=SyntheticJob.Status.EXCUSED,
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
    DYNAMIC_MANIFEST_SCORE_MULTIPLIER=DYNAMIC_MANIFEST_SCORE_MULTIPLIER,
    DYNAMIC_MANIFEST_DANCE_RATIO_THRESHOLD=DYNAMIC_MANIFEST_DANCE_RATIO_THRESHOLD,
)
@pytest.mark.django_db(databases=["default", "default_alias"], transaction=True)
@pytest.mark.parametrize(
    ("prev_online_executor_count", "expected_multiplier"),
    [
        (None, DYNAMIC_MANIFEST_SCORE_MULTIPLIER),
        (5, DYNAMIC_MANIFEST_SCORE_MULTIPLIER),
        (7, 1.0),
        (15, 1.0),
        (20, DYNAMIC_MANIFEST_SCORE_MULTIPLIER),
    ],
)
def test_manifest_dance_incentives(
    prev_online_executor_count,
    expected_multiplier,
    override_weights_version_v2,
    settings,
):
    curr_cycle_range = get_cycle_containing_block(1000, settings.BITTENSOR_NETUID)
    curr_cycle = Cycle.objects.create(start=curr_cycle_range.start, stop=curr_cycle_range.stop)
    curr_batch = create_batch(10, curr_cycle)

    if prev_online_executor_count is not None:
        prev_cycle_range = get_cycle_containing_block(
            curr_cycle_range.start - 1,
            settings.BITTENSOR_NETUID,
        )
        prev_cycle = Cycle.objects.create(start=prev_cycle_range.start, stop=prev_cycle_range.stop)
        _prev_batch = create_batch(prev_online_executor_count, prev_cycle)

    scores = score_batches([curr_batch])
    assert "miner_hotkey" in scores
    assert abs(scores["miner_hotkey"] - 100 * expected_multiplier) < 0.0001


@pytest.mark.override_config(
    DYNAMIC_EXECUTOR_CLASS_WEIGHTS="always_on.llm.a6000=100",
    DYNAMIC_MANIFEST_SCORE_MULTIPLIER=DYNAMIC_MANIFEST_SCORE_MULTIPLIER,
    DYNAMIC_MANIFEST_DANCE_RATIO_THRESHOLD=DYNAMIC_MANIFEST_DANCE_RATIO_THRESHOLD,
)
@pytest.mark.django_db(databases=["default", "default_alias"], transaction=True)
def test_dance_incentives_disabled_on_v1(override_weights_version_v1, settings):
    prev_cycle_range = get_cycle_containing_block(1000, settings.BITTENSOR_NETUID)
    curr_cycle_range = get_cycle_containing_block(
        prev_cycle_range.stop + 1, settings.BITTENSOR_NETUID
    )
    prev_cycle = Cycle.objects.create(start=prev_cycle_range.start, stop=prev_cycle_range.stop)
    curr_cycle = Cycle.objects.create(start=curr_cycle_range.start, stop=curr_cycle_range.stop)

    _prev_batch = create_batch(1, prev_cycle)
    curr_batch = create_batch(5, curr_cycle)

    scores = score_batches([curr_batch])
    assert "miner_hotkey" in scores

    # there should be no bonus
    assert abs(scores["miner_hotkey"] - 100) < 0.0001


@pytest.mark.override_config(
    DYNAMIC_EXECUTOR_CLASS_WEIGHTS="always_on.llm.a6000=100",
    DYNAMIC_MANIFEST_SCORE_MULTIPLIER=DYNAMIC_MANIFEST_SCORE_MULTIPLIER,
    DYNAMIC_MANIFEST_DANCE_RATIO_THRESHOLD=DYNAMIC_MANIFEST_DANCE_RATIO_THRESHOLD,
)
@pytest.mark.django_db(databases=["default", "default_alias"], transaction=True)
def test_dance_incentives_applied_on_missing_prev_cycle(override_weights_version_v2, settings):
    cycle1_range = get_cycle_containing_block(1000, settings.BITTENSOR_NETUID)
    cycle2_range = get_cycle_containing_block(cycle1_range.stop + 1, settings.BITTENSOR_NETUID)
    cycle3_range = get_cycle_containing_block(cycle2_range.stop + 1, settings.BITTENSOR_NETUID)
    cycle1 = Cycle.objects.create(start=cycle1_range.start, stop=cycle1_range.stop)
    cycle3 = Cycle.objects.create(start=cycle3_range.start, stop=cycle3_range.stop)

    _batch1 = create_batch(10, cycle1)
    # batch2 is missing
    batch3 = create_batch(10, cycle3)

    scores = score_batches([batch3])
    assert "miner_hotkey" in scores
    assert scores["miner_hotkey"] == approx(100 * DYNAMIC_MANIFEST_SCORE_MULTIPLIER, abs=10**-4)


def setup_batch_jobs(
    batch: SyntheticJobBatch,
    miner: Miner,
    synthetic_completed: int = 0,
    synthetic_excused: int = 0,
    synthetic_failed: int = 0,
    organic_completed: int = 0,
    organic_failed: int = 0,
):
    common_params = {
        "miner": miner,
        "miner_address": "127.0.0.1",
        "miner_address_ip_version": 4,
        "miner_port": 8080,
        "executor_class": ExecutorClass.always_on__llm__a6000,
    }
    for _ in range(synthetic_completed):
        SyntheticJob.objects.create(
            status=SyntheticJob.Status.COMPLETED,
            batch=batch,
            score=1,
            **common_params,
        )
    for _ in range(synthetic_excused):
        SyntheticJob.objects.create(
            status=SyntheticJob.Status.EXCUSED,
            batch=batch,
            score=1,
            **common_params,
        )
    for _ in range(synthetic_failed):
        SyntheticJob.objects.create(
            status=SyntheticJob.Status.FAILED,
            batch=batch,
            score=0,
            **common_params,
        )
    for _ in range(organic_completed):
        OrganicJob.objects.create(
            status=OrganicJob.Status.COMPLETED,
            block=batch.cycle.start,
            **common_params,
        )
    for _ in range(organic_failed):
        OrganicJob.objects.create(
            status=OrganicJob.Status.FAILED,
            block=batch.cycle.start,
            **common_params,
        )


@pytest.mark.override_config(DYNAMIC_EXECUTOR_CLASS_WEIGHTS="always_on.llm.a6000=100")
@pytest.mark.django_db(databases=["default", "default_alias"], transaction=True)
def test_temporary_scoring_formula(override_weights_version_v1):
    # override_weights_version_v1 is used to skip testing dancing bonus here

    batch = SyntheticJobBatch.objects.create(
        accepting_results_until=timezone.now() + timedelta(hours=1),
        cycle=Cycle.objects.create(start=708, stop=1430),
    )
    miner1 = Miner.objects.create(hotkey="miner1")
    miner2 = Miner.objects.create(hotkey="miner2")

    setup_batch_jobs(
        batch=batch,
        miner=miner1,
        synthetic_completed=2,
        synthetic_excused=3,
        synthetic_failed=5,
        organic_completed=7,
        organic_failed=11,
    )
    setup_batch_jobs(
        batch=batch,
        miner=miner2,
        synthetic_completed=13,
        synthetic_excused=17,
        synthetic_failed=19,
        organic_completed=23,
        organic_failed=29,
    )

    scores = score_batches([batch])

    # expected scores
    miner1_correct = 2 + 3 + 7
    miner2_correct = 13 + 17 + 23
    total = miner1_correct + miner2_correct
    miner1_score = 100 * miner1_correct / total
    miner2_score = 100 * miner2_correct / total

    assert "miner1" in scores
    assert "miner2" in scores
    assert scores["miner1"] == approx(miner1_score, abs=10**-4)
    assert scores["miner2"] == approx(miner2_score, abs=10**-4)
