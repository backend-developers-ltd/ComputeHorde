from ipaddress import IPv4Address
from typing import NamedTuple
from unittest.mock import MagicMock, patch

import pytest
from asgiref.sync import sync_to_async

from ..models import Channel, Validator
from ..tasks import sync_metagraph


class MockedAxonInfo(NamedTuple):
    is_serving: bool
    ip: IPv4Address = IPv4Address("0.0.0.0")
    port: int = 8000


class MockedNeuron(NamedTuple):
    hotkey: str
    axon_info: MockedAxonInfo


class MockedValidator(NamedTuple):
    hotkey: str


def configure_mock_pylon_client(
    mock_client: MagicMock,
    validators: list[str],
    neurons: dict[str, MockedNeuron],
) -> None:
    validators_response = MagicMock()
    validators_response.validators = [MockedValidator(hotkey=v) for v in validators]
    validators_response.block.number = 12345

    neurons_response = MagicMock()
    neurons_response.neurons = neurons

    mock_client.open_access.get_latest_validators.return_value = validators_response
    mock_client.open_access.get_neurons.return_value = neurons_response


@pytest.mark.django_db(transaction=True)
def test__sync_metagraph__activation(mock_pylon_client):
    Validator.objects.bulk_create(
        [
            Validator(ss58_address="remains_active", is_active=True),
            Validator(ss58_address="is_deactivated", is_active=True),
            Validator(ss58_address="remains_inactive", is_active=False),
            Validator(ss58_address="is_activated", is_active=False),
        ]
    )

    validator_hotkeys = ["remains_active", "is_activated", "new_validator"]
    neurons = {
        "remains_active": MockedNeuron(hotkey="remains_active", axon_info=MockedAxonInfo(is_serving=False)),
        "is_deactivated": MockedNeuron(hotkey="is_deactivated", axon_info=MockedAxonInfo(is_serving=True)),
        "remains_inactive": MockedNeuron(hotkey="remains_inactive", axon_info=MockedAxonInfo(is_serving=True)),
        "is_activated": MockedNeuron(hotkey="is_activated", axon_info=MockedAxonInfo(is_serving=False)),
        "new_validator": MockedNeuron(hotkey="new_validator", axon_info=MockedAxonInfo(is_serving=False)),
        "new_miner": MockedNeuron(hotkey="new_miner", axon_info=MockedAxonInfo(is_serving=True)),
    }

    configure_mock_pylon_client(mock_pylon_client, validator_hotkeys, neurons)

    with patch("project.core.tasks.pylon_client", return_value=mock_pylon_client):
        sync_metagraph()

    validators = Validator.objects.order_by("id").values_list("ss58_address", "is_active")
    assert list(validators) == [
        ("remains_active", True),
        ("is_deactivated", False),
        ("remains_inactive", False),
        ("is_activated", True),
        ("new_validator", True),
    ]


@pytest.mark.asyncio
@pytest.mark.django_db(transaction=True)
async def test__websocket__disconnect_validator_if_become_inactive(
    mock_pylon_client,
    communicator,
    authenticated,
    validator,
    job,
    dummy_job_params,
):
    await communicator.receive_json_from()
    assert await Channel.objects.filter(validator=validator).aexists()

    validator_hotkeys = []
    neurons = {
        validator.ss58_address: MockedNeuron(
            hotkey=validator.ss58_address,
            axon_info=MockedAxonInfo(is_serving=True),
        ),
    }

    configure_mock_pylon_client(mock_pylon_client, validator_hotkeys, neurons)

    with patch("project.core.tasks.pylon_client", return_value=mock_pylon_client):
        await sync_to_async(sync_metagraph)()

    assert (await communicator.receive_output())["type"] == "websocket.close"
    assert not await Channel.objects.filter(validator=validator).aexists()
