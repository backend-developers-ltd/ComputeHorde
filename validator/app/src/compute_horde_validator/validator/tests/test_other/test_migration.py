import pytest

from compute_horde_validator.validator.scoring.pylon_client import setup_mock_pylon_client
from compute_horde_validator.validator.scoring.tasks import set_scores


@pytest.mark.django_db
@pytest.mark.override_config(SERVING=False)
def test__migration__not_serving__should_not_set_scores():
    with setup_mock_pylon_client() as mock_pylon_client:
        set_scores()

    assert mock_pylon_client.weights_submitted == []
