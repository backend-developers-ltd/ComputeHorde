import pytest
from pytest_mock import MockerFixture

from compute_horde_validator.validator.scoring.tasks import set_scores


@pytest.mark.django_db
@pytest.mark.override_config(SERVING=False)
def test__migration__not_serving__should_not_set_scores(mocker: MockerFixture, bittensor):
    subtensor_mock = mocker.patch("bittensor.subtensor")

    set_scores()

    assert not bittensor.subnet.return_value.weights.commit.called
    assert subtensor_mock.call_count == 0
    assert len(subtensor_mock.method_calls) == 0
