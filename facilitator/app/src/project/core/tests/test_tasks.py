from typing import NamedTuple

import pytest
from asgiref.sync import sync_to_async
from constance import config

from ..models import Channel, Validator
from ..tasks import sync_metagraph


class MockedAxonInfo(NamedTuple):
    is_serving: bool
    ip: str = ""
    port: int = 0


class MockedNeuron(NamedTuple):
    hotkey: str
    axon_info: MockedAxonInfo
    stake: float


validator_params = dict(
    axon_info=MockedAxonInfo(is_serving=False),
    stake=1.0,
)

miner_params = dict(
    axon_info=MockedAxonInfo(is_serving=True),
    stake=0.0,
)


@pytest.mark.django_db(transaction=True)
def test__sync_metagraph__activation(monkeypatch):
    import bittensor

    validators = Validator.objects.bulk_create(
        [
            Validator(ss58_address="remains_active", is_active=True),
            Validator(ss58_address="is_deactivated", is_active=True),
            Validator(ss58_address="remains_inactive", is_active=False),
            Validator(ss58_address="is_activated", is_active=False),
        ]
    )

    class MockedMetagraph:
        def __init__(self, *args, **kwargs):
            self.neurons = [
                MockedNeuron(hotkey="remains_active", **validator_params),
                MockedNeuron(hotkey="is_deactivated", **miner_params),
                MockedNeuron(hotkey="remains_inactive", **miner_params),
                MockedNeuron(hotkey="is_activated", **validator_params),
                MockedNeuron(hotkey="new_validator", **validator_params),
                MockedNeuron(hotkey="new_miner", **miner_params),
            ]

    with monkeypatch.context() as mp:
        mp.setattr(bittensor, "metagraph", MockedMetagraph)
        sync_metagraph()

    validators = Validator.objects.order_by("id").values_list("ss58_address", "is_active")
    assert list(validators) == [
        tuple(d.values())
        for d in [
            dict(ss58_address="remains_active", is_active=True),
            dict(ss58_address="is_deactivated", is_active=False),
            dict(ss58_address="remains_inactive", is_active=False),
            dict(ss58_address="is_activated", is_active=True),
            dict(ss58_address="new_validator", is_active=True),
        ]
    ]


@pytest.mark.django_db(transaction=True)
def test__sync_metagraph__limit__no_our_validator(monkeypatch):
    """When there is validator limit"""

    import bittensor

    class MockedMetagraph:
        def __init__(self, *args, **kwargs):
            self.neurons = [MockedNeuron(hotkey=str(i), **(validator_params | {"stake": 10 * i})) for i in range(1, 33)]

    config.VALIDATORS_LIMIT = 4

    with monkeypatch.context() as mp:
        mp.setattr(bittensor, "metagraph", MockedMetagraph)
        sync_metagraph()

    validators = Validator.objects.order_by("ss58_address").values_list("ss58_address")
    assert list(validators) == [
        tuple(d.values())
        for d in [
            dict(ss58_address="29"),
            dict(ss58_address="30"),
            dict(ss58_address="31"),
            dict(ss58_address="32"),
        ]
    ]


@pytest.mark.django_db(transaction=True)
def test__sync_metagraph__limit_and_our_validator__wrong(monkeypatch):
    """When there is validator limit and ours is not in validators list"""

    import bittensor

    class MockedMetagraph:
        def __init__(self, *args, **kwargs):
            self.neurons = [MockedNeuron(hotkey=str(i), **(validator_params | {"stake": 10 * i})) for i in range(1, 33)]

    config.VALIDATORS_LIMIT = 4
    config.OUR_VALIDATOR_SS58_ADDRESS = "99"

    with monkeypatch.context() as mp:
        mp.setattr(bittensor, "metagraph", MockedMetagraph)
        sync_metagraph()

    validators = Validator.objects.order_by("ss58_address").values_list("ss58_address")
    assert list(validators) == [
        tuple(d.values())
        for d in [
            dict(ss58_address="29"),
            dict(ss58_address="30"),
            dict(ss58_address="31"),
            dict(ss58_address="32"),
        ]
    ]


@pytest.mark.django_db(transaction=True)
def test__sync_metagraph__limit_and_our_validator__inside_limit(monkeypatch):
    """When there is validator limit and ours is one of best validators"""

    import bittensor

    class MockedMetagraph:
        def __init__(self, *args, **kwargs):
            self.neurons = [MockedNeuron(hotkey=str(i), **(validator_params | {"stake": 10 * i})) for i in range(1, 33)]

    config.VALIDATORS_LIMIT = 4
    config.OUR_VALIDATOR_SS58_ADDRESS = "30"

    with monkeypatch.context() as mp:
        mp.setattr(bittensor, "metagraph", MockedMetagraph)
        sync_metagraph()

    validators = Validator.objects.order_by("ss58_address").values_list("ss58_address")
    assert list(validators) == [
        tuple(d.values())
        for d in [
            dict(ss58_address="29"),
            dict(ss58_address="30"),
            dict(ss58_address="31"),
            dict(ss58_address="32"),
        ]
    ]


@pytest.mark.django_db(transaction=True)
def test__sync_metagraph__limit_and_our_validator__outside_limit(monkeypatch):
    """When there is validator limit and ours is not one of best validators"""

    import bittensor

    class MockedMetagraph:
        def __init__(self, *args, **kwargs):
            self.neurons = [MockedNeuron(hotkey=str(i), **(validator_params | {"stake": 10 * i})) for i in range(1, 33)]

    config.VALIDATORS_LIMIT = 4
    config.OUR_VALIDATOR_SS58_ADDRESS = "25"

    with monkeypatch.context() as mp:
        mp.setattr(bittensor, "metagraph", MockedMetagraph)
        sync_metagraph()

    validators = Validator.objects.order_by("ss58_address").values_list("ss58_address")
    assert list(validators) == [
        tuple(d.values())
        for d in [
            dict(ss58_address="25"),
            dict(ss58_address="30"),
            dict(ss58_address="31"),
            dict(ss58_address="32"),
        ]
    ]


@pytest.mark.asyncio
@pytest.mark.django_db(transaction=True)
async def test__websocket__disconnect_validator_if_become_inactive(
    monkeypatch,
    communicator,
    authenticated,
    validator,
    job,
    dummy_job_params,
):
    """Check that validator is disconnected if it becomes inactive"""
    import bittensor

    await communicator.receive_json_from()
    assert await Channel.objects.filter(validator=validator).aexists()

    class MockedMetagraph:
        def __init__(self, *args, **kwargs):
            self.neurons = [
                MockedNeuron(hotkey=validator.ss58_address, **miner_params),
            ]

    with monkeypatch.context() as mp:
        mp.setattr(bittensor, "metagraph", MockedMetagraph)
        await sync_to_async(sync_metagraph)()

    assert (await communicator.receive_output())["type"] == "websocket.close"
    assert not await Channel.objects.filter(validator=validator).aexists()
