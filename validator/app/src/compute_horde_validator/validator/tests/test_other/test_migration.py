import pytest

from compute_horde_validator.validator.scoring.tasks import set_scores


@pytest.mark.django_db
@pytest.mark.override_config(SERVING=False)
def test__migration__not_serving__should_not_set_scores(pylon_client_mock):
    set_scores()
    pylon_client_mock.identity.put_weights.assert_not_called()
