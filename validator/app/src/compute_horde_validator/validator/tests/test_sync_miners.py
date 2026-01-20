import pytest
from compute_horde.test_base.pylon import mock_pylon_client_case_100

from compute_horde_validator.validator.miner_sync import sync_miners
from compute_horde_validator.validator.models import Miner, SystemEvent


@pytest.mark.django_db
def test_sync_miners_fresh_db(mock_pylon_client):
    with mock_pylon_client_case_100(mock_pylon_client):
        sync_miners()

    expected_miners_data = [
        {
            "hotkey": "hotkey1",
            "uid": 1,
            "coldkey": "coldkey1",
            "address": "1.1.1.1",
            "port": 8000,
            "ip_version": 4,
        },
        {
            "hotkey": "hotkey2",
            "uid": 2,
            "coldkey": "coldkey2",
            "address": "2.2.2.2",
            "port": 8001,
            "ip_version": 4,
        },
        {
            "hotkey": "hotkey3",
            "uid": 3,
            "coldkey": "coldkey3",
            "address": "2001:db8::",
            "port": 8003,
            "ip_version": 6,
        },
    ]

    actual_miners_data = list(
        Miner.objects.values("hotkey", "uid", "coldkey", "address", "port", "ip_version").order_by(
            "hotkey"
        )
    )
    assert actual_miners_data == sorted(expected_miners_data, key=lambda x: x["hotkey"])

    event = SystemEvent.objects.get(
        type=SystemEvent.EventType.VALIDATOR_MINERS_REFRESH,
        subtype=SystemEvent.EventSubType.SUCCESS,
    )
    assert event.data == {
        "block": 100,
        "new_neurons": 3,
        "updated_axon_infos": 0,
    }
