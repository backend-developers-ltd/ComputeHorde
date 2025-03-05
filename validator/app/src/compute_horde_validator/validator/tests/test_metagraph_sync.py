from unittest.mock import patch

import pytest

from compute_horde_validator.validator.models import (
    MetagraphSnapshot,
    Miner,
    SystemEvent,
)
from compute_horde_validator.validator.tasks import sync_metagraph

from .helpers import MockMetagraph


@pytest.mark.django_db(databases=["default", "default_alias"], transaction=True)
def test_metagraph_sync__success():
    assert SystemEvent.objects.count() == 0
    assert Miner.objects.count() == 0

    n = 5
    block_num = 100
    with patch(
        "bittensor.metagraph",
        lambda *args, **kwargs: MockMetagraph(num_neurons=n, block_num=block_num),
    ):
        sync_metagraph()

    snapshot = MetagraphSnapshot.get_latest()
    assert snapshot.block == block_num
    assert len(snapshot.hotkeys) == n
    assert len(snapshot.stake) > 0

    # check metagraph sync success
    event = (
        SystemEvent.objects.order_by("-timestamp")
        .filter(
            type=SystemEvent.EventType.VALIDATOR_MINERS_REFRESH,
            subtype=SystemEvent.EventSubType.SUCCESS,
        )
        .first()
    )
    assert event.data["new_neurons"] == n
    assert event.data["updated_axon_infos"] == 5
    assert event.data["block"] == block_num

    # check new miners creation
    assert Miner.objects.count() == n
    for i in range(n):
        assert Miner.objects.get(hotkey=f"hotkey_{i}") is not None

    # check extra miner gets created
    n = 6
    block_num = 101
    with patch(
        "bittensor.metagraph",
        lambda *args, **kwargs: MockMetagraph(num_neurons=n, block_num=block_num),
    ):
        sync_metagraph()

    assert Miner.objects.count() == n
    assert Miner.objects.get(hotkey=f"hotkey_{n - 1}") is not None

    event = (
        SystemEvent.objects.order_by("-timestamp")
        .filter(
            type=SystemEvent.EventType.VALIDATOR_MINERS_REFRESH,
            subtype=SystemEvent.EventSubType.SUCCESS,
        )
        .first()
    )
    assert event.data["new_neurons"] == 1  # 6th miner
    assert event.data["updated_axon_infos"] == 1
    assert event.data["block"] == block_num

    # check metagraph syncing lagging warns
    block_num = 104
    with patch(
        "bittensor.metagraph",
        lambda *args, **kwargs: MockMetagraph(num_neurons=n, block_num=block_num),
    ):
        sync_metagraph()

    event = SystemEvent.objects.get(
        type=SystemEvent.EventType.METAGRAPH_SYNCING,
        subtype=SystemEvent.EventSubType.WARNING,
    )
    assert event.data["blocks_diff"] == 3

    event = (
        SystemEvent.objects.order_by("-timestamp")
        .filter(
            type=SystemEvent.EventType.VALIDATOR_MINERS_REFRESH,
            subtype=SystemEvent.EventSubType.SUCCESS,
        )
        .first()
    )
    assert event.data["updated_axon_infos"] == 0
    assert event.data["block"] == block_num


def raise_exception(*args, **kwargs):
    raise Exception("Simulated metagraph failure")


@pytest.mark.django_db(databases=["default", "default_alias"], transaction=True)
@patch("bittensor.metagraph", raise_exception)
def test_metagraph_sync__fetch_error():
    assert SystemEvent.objects.count() == 0, "No system events should be created before task"

    sync_metagraph()

    event = SystemEvent.objects.get(
        type=SystemEvent.EventType.METAGRAPH_SYNCING,
        subtype=SystemEvent.EventSubType.SUBTENSOR_CONNECTIVITY_ERROR,
    )
    assert "failure" in event.long_description
