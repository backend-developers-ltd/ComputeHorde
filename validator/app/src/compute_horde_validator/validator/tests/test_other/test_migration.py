import pytest
from pytest_mock import MockerFixture

from compute_horde_validator.validator.scoring.tasks import set_scores
from compute_horde_validator.validator.tasks import run_synthetic_jobs


@pytest.mark.django_db
@pytest.mark.override_config(SERVING=False)
def test__migration__not_serving__should_not_set_scores(mocker: MockerFixture, bittensor):
    subtensor_mock = mocker.patch("bittensor.subtensor")

    set_scores()

    assert not bittensor.subnet.return_value.weights.commit.called
    assert subtensor_mock.call_count == 0
    assert len(subtensor_mock.method_calls) == 0


@pytest.mark.django_db
@pytest.mark.override_config(SERVING=False)
def test__migration__not_serving__should_not_send_synthetic_jobs(
    settings,
    bittensor,
    mocker: MockerFixture,
):
    settings.DEBUG_DONT_STAGGER_VALIDATORS = True
    _run_synthetic_jobs_mock = mocker.patch(
        "compute_horde_validator.validator.tasks._run_synthetic_jobs"
    )

    run_synthetic_jobs()

    assert _run_synthetic_jobs_mock.call_count == 0
    assert len(_run_synthetic_jobs_mock.method_calls) == 0
