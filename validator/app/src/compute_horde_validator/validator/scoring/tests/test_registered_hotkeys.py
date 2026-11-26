from unittest.mock import patch

import pytest
from compute_horde_core.executor_class import ExecutorClass

from compute_horde_validator.validator.models import Miner
from compute_horde_validator.validator.scoring.engine import DefaultScoringEngine

EXECUTOR_CLASS = ExecutorClass.spin_up_4min__gpu_24gb


def calculate_scores(paid_scores: dict[str, float], main_hotkeys: dict[str, str], registered):
    with (
        patch(
            "compute_horde_validator.validator.scoring.engine.calculate_allowance_paid_job_scores",
            return_value={EXECUTOR_CLASS: paid_scores},
        ),
        patch(
            "compute_horde_validator.validator.scoring.engine.get_executor_class_weights",
            return_value={EXECUTOR_CLASS: 1.0},
        ),
        patch(
            "compute_horde_validator.validator.scoring.engine.query_miner_main_hotkeys",
            return_value=main_hotkeys,
        ),
    ):
        return DefaultScoringEngine().calculate_scores_for_cycles(
            current_cycle_start=1444, previous_cycle_start=722, registered_hotkeys=registered
        )


@pytest.fixture
def miners():
    # "a_old" was deregistered; its Miner row stays in the database.
    Miner.objects.create(hotkey="a_main", coldkey="ck_a", uid=1)
    Miner.objects.create(hotkey="a_old", coldkey="ck_a", uid=2)
    Miner.objects.create(hotkey="b_main", coldkey="ck_b", uid=3)


@pytest.mark.django_db
def test_deregistered_hotkey_gets_no_share(miners):
    scores = calculate_scores(
        {"a_main": 10.0, "b_main": 10.0},
        {"a_main": "a_main", "a_old": "a_main", "b_main": "b_main"},
        registered=["a_main", "b_main"],
    )
    assert scores == pytest.approx({"a_main": 0.5, "b_main": 0.5})


@pytest.mark.django_db
def test_work_of_deregistered_hotkey_goes_to_registered_hotkeys_of_its_coldkey(miners):
    scores = calculate_scores(
        {"a_old": 10.0, "b_main": 10.0},
        {"a_main": "a_main", "a_old": "a_main", "b_main": "b_main"},
        registered=["a_main", "b_main"],
    )
    assert scores == pytest.approx({"a_main": 0.5, "b_main": 0.5})


@pytest.mark.django_db
def test_coldkey_without_registered_hotkeys_gets_nothing(miners):
    scores = calculate_scores(
        {"a_main": 10.0, "a_old": 10.0, "b_main": 10.0},
        {"a_main": "a_main", "a_old": "a_main", "b_main": "b_main"},
        registered=["b_main"],
    )
    assert scores == pytest.approx({"b_main": 1.0})
