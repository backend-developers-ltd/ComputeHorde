from contextlib import contextmanager
from ipaddress import IPv4Address, IPv6Address
from unittest.mock import Mock, create_autospec, patch

import pytest
from pylon_client.v1 import (
    AxonInfo,
    AxonProtocol,
    Block,
    BlockHash,
    BlockNumber,
    Neuron,
    PylonClient,
    Stakes,
    SubnetNeurons,
)

"""
This is a temporary solution until we have a proper test setup for PylonClient in pylon repo.
The way to use these convenience mocks in test cases is:

>>> def test_case(mock_pylon_client):
>>>    with mock_pylon_client_case_100(mock_pylon_client):
>>>        call_sth_youre_testing()
>>>    assert_stuff_or_sth
>>>
>>>    with mock_pylon_client_case_200(mock_pylon_client):
>>>        call_sth_youre_testing()
>>>    assert_stuff_or_sth
>>>
>>>    # if you need to compose your own test data:
>>>    with seed_neuron_list(mock_pylon_client, neurons=...)
>>>        ...
>>>    ...   
"""

#  This is a temporary solution until we have a proper test setup for PylonClient in pylon repo.

@pytest.fixture
def mock_pylon_client(mocker):
    """
    Use this fixure in all tests relying on PylonClient
    """
    mocked = create_autospec(PylonClient)
    mocked.__enter__.return_value = mocked
    mocked.open_access = create_autospec(PylonClient._open_access_api_cls, instance=True)
    mocked.identity = create_autospec(PylonClient._identity_api_cls, instance=True)
    mocker.patch("compute_horde.pylon.PylonClient", return_value=mocked)
    return mocked


@contextmanager
def seed_neuron_list(pylon_client_instance_mock: Mock, neurons: SubnetNeurons):
    """
    Seed the neron list returned by PylonClient with the provided response object.
    """
    with patch.object(
        pylon_client_instance_mock.identity, "get_recent_neurons", new=lambda *a, **kw: neurons
    ):
        with patch.object(
            pylon_client_instance_mock.identity, "get_latest_neurons", new=lambda *a, **kw: neurons
        ):
            with patch.object(
                pylon_client_instance_mock.identity, "get_neurons", new=lambda *a, **kw: neurons
            ):
                yield


@contextmanager
def mock_pylon_client_case_100(mock_pylon_client):
    neurons = {
        "hotkey1": Neuron(
            uid=1,
            hotkey="hotkey1",
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
            stakes=Stakes(alpha=0, tao=0, total=0),
        ),
        "hotkey2": Neuron(
            uid=2,
            hotkey="hotkey2",
            coldkey="coldkey2",
            active=True,
            axon_info=AxonInfo(ip=IPv4Address("2.2.2.2"), port=8001, protocol=AxonProtocol.TCP),
            stake=int(1e5),
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
            stakes=Stakes(alpha=int(1e5 / 2), tao=int(1e5 / 2), total=int(1e5)),
        ),
        "hotkey3": Neuron(
            uid=3,
            hotkey="hotkey3",
            coldkey="coldkey3",
            active=True,
            axon_info=AxonInfo(ip=IPv6Address("2001:db8::"), port=8003, protocol=AxonProtocol.TCP),
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
            stakes=Stakes(alpha=0, tao=0, total=0),
        ),
    }
    with seed_neuron_list(
        mock_pylon_client,
        SubnetNeurons(
            block=Block(
                number=BlockNumber(100),
                hash=BlockHash("0x123"),
            ),
            neurons=neurons,
        ),
    ):
        yield


@contextmanager
def mock_pylon_client_case_200(mock_pylon_client):
    neurons = {
        "hotkey1": Neuron(
            uid=1,
            hotkey="hotkey1",
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
            stakes=Stakes(alpha=0, tao=0, total=0),
        ),
        "hotkey2": Neuron(
            uid=2,
            hotkey="hotkey2",
            coldkey="coldkey2",
            active=True,
            axon_info=AxonInfo(ip=IPv6Address("2001:db9::"), port=8003, protocol=AxonProtocol.TCP),
            stake=int(1e5),
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
            stakes=Stakes(alpha=int(1e5 / 2), tao=int(1e5 / 2), total=int(1e5)),
        ),
        "hotkey4": Neuron(
            uid=3,
            hotkey="hotkey4",
            coldkey="coldkey4",
            active=True,
            axon_info=AxonInfo(ip=IPv6Address("2001:db8::"), port=8003, protocol=AxonProtocol.TCP),
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
            stakes=Stakes(alpha=0, tao=0, total=0),
        ),
    }
    with seed_neuron_list(
        mock_pylon_client,
        SubnetNeurons(
            block=Block(
                number=BlockNumber(200),
                hash=BlockHash("0x246"),
            ),
            neurons=neurons,
        ),
    ):
        yield
