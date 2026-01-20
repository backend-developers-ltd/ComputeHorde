from ipaddress import IPv4Address

import pytest
from compute_horde_validator.validator.miner_sync import sync_miners
from compute_horde_validator.validator.models import Miner, SystemEvent
from pylon_client._internal.pylon_commons.models import (
    AxonInfo,
    AxonProtocol,
    Block,
    Neuron,
    Stakes,
    SubnetNeurons,
)

@pytest.mark.django_db
def test_sync_miners_fresh_db(pylon_client_mock):
    hotkey1 = "hotkey1"
    hotkey2 = "hotkey2"

    dummy_stakes = Stakes(alpha=0, tao=0, total=0)
    neurons = {
        hotkey1: Neuron(
            uid=1,
            hotkey=hotkey1,
            coldkey="coldkey1",
            active=True,
            axon_info=AxonInfo(ip=IPv4Address("1.1.1.1"), port=8000, protocol=AxonProtocol.TCP),
            stake=0,
            rank=0,
            emission=0,
            incentive=0,
            consensus=0,
            trust=0,
            validator_trust=0,
            dividends=0,
            last_update=0,
            validator_permit=False,
            pruning_score=0,
            stakes=dummy_stakes,
        ),
        hotkey2: Neuron(
            uid=2,
            hotkey=hotkey2,
            coldkey="coldkey2",
            active=True,
            axon_info=AxonInfo(ip=IPv4Address("2.2.2.2"), port=8001, protocol=AxonProtocol.TCP),
            stake=0,
            rank=0,
            emission=0,
            incentive=0,
            consensus=0,
            trust=0,
            validator_trust=0,
            dividends=0,
            last_update=0,
            validator_permit=False,
            pruning_score=0,
            stakes=dummy_stakes,
        ),
    }

    pylon_client_mock.identity.get_recent_neurons.return_value = SubnetNeurons(
        block=Block(number=100, hash="0x123"),
        neurons=neurons,
    )

    sync_miners()

    expected_miners_data = [
        {
            "hotkey": hotkey1,
            "uid": 1,
            "coldkey": "coldkey1",
            "address": "1.1.1.1",
            "port": 8000,
            "ip_version": 4,
        },
        {
            "hotkey": hotkey2,
            "uid": 2,
            "coldkey": "coldkey2",
            "address": "2.2.2.2",
            "port": 8001,
            "ip_version": 4,
        },
    ]

    actual_miners_data = list(Miner.objects.values("hotkey", "uid", "coldkey", "address", "port", "ip_version").order_by("hotkey"))
    assert actual_miners_data == sorted(expected_miners_data, key=lambda x: x["hotkey"])

    event = SystemEvent.objects.get(
        type=SystemEvent.EventType.VALIDATOR_MINERS_REFRESH,
        subtype=SystemEvent.EventSubType.SUCCESS,
    )
    assert event.data == {
        "block": 100,
        "new_neurons": 2,
        "updated_axon_infos": 0,
    }
