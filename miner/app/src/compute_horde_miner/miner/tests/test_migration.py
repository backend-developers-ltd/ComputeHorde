import pytest
from pytest_mock import MockerFixture

from compute_horde_miner.miner.tasks import announce_address_and_port


@pytest.mark.django_db
@pytest.mark.override_config(SERVING=False)
def test__migration__not_serving__should_not_announce_address(mocker: MockerFixture):
    announce_mock = mocker.patch(
        "compute_horde_miner.miner.tasks.quasi_axon.announce_address_and_port"
    )

    announce_address_and_port()

    assert announce_mock.call_count == 0
    assert len(announce_mock.method_calls) == 0
