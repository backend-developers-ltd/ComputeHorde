from unittest.mock import patch

import pytest

from compute_horde_validator.validator.models import (
    MetagraphSnapshot,
    Miner,
    SystemEvent,
)
from compute_horde_validator.validator.tasks import async_metagraph

from .helpers import MockAsyncSubtensor


@pytest.mark.asyncio
@pytest.mark.django_db(transaction=True)
async def test_async_metagraph__success():
    assert await SystemEvent.objects.acount() == 0
    assert await Miner.objects.acount() == 0

    n = 5
    block_num = 100
    with patch(
        "compute_horde_validator.validator.tasks.AsyncSubtensor",
        lambda network: MockAsyncSubtensor(num_neurons=n, block_num=block_num),
    ):
        await async_metagraph()

    snapshot = await MetagraphSnapshot.objects.aget(id=0)
    assert snapshot.block == block_num
    assert len(snapshot.hotkeys) == n
    assert len(snapshot.stake) > 0

    # check metagraphy sync success
    event = (
        await SystemEvent.objects.order_by("-timestamp")
        .filter(
            type=SystemEvent.EventType.VALIDATOR_MINERS_REFRESH,
            subtype=SystemEvent.EventSubType.SUCCESS,
        )
        .afirst()
    )
    assert event.data["new_neurons"] == n
    assert event.data["updated_axon_infos"] == 5
    assert event.data["block"] == block_num

    # check new miners creation
    assert await Miner.objects.acount() == n
    for i in range(n):
        assert await Miner.objects.aget(hotkey=f"hotkey_{i}") is not None

    # check extra miner gets created
    n = 6
    block_num = 101
    with patch(
        "compute_horde_validator.validator.tasks.AsyncSubtensor",
        lambda network: MockAsyncSubtensor(num_neurons=n, block_num=block_num),
    ):
        await async_metagraph()

    assert await Miner.objects.acount() == n
    assert await Miner.objects.aget(hotkey=f"hotkey_{n - 1}") is not None

    event = (
        await SystemEvent.objects.order_by("-timestamp")
        .filter(
            type=SystemEvent.EventType.VALIDATOR_MINERS_REFRESH,
            subtype=SystemEvent.EventSubType.SUCCESS,
        )
        .afirst()
    )
    assert event.data["new_neurons"] == 1  # 6th miner
    assert event.data["updated_axon_infos"] == 1
    assert event.data["block"] == block_num

    # check metagraph syncing lagging warns
    block_num = 104
    with patch(
        "compute_horde_validator.validator.tasks.AsyncSubtensor",
        lambda network: MockAsyncSubtensor(num_neurons=n, block_num=block_num),
    ):
        await async_metagraph()

    event = await SystemEvent.objects.aget(
        type=SystemEvent.EventType.METAGRAPH_SYNCING, subtype=SystemEvent.EventSubType.WARNING
    )
    assert event.data["blocks_diff"] == 3

    event = (
        await SystemEvent.objects.order_by("-timestamp")
        .filter(
            type=SystemEvent.EventType.VALIDATOR_MINERS_REFRESH,
            subtype=SystemEvent.EventSubType.SUCCESS,
        )
        .afirst()
    )
    assert event.data["updated_axon_infos"] == 0
    assert event.data["block"] == block_num


@pytest.mark.asyncio
@pytest.mark.django_db(transaction=True)
@patch(
    "compute_horde_validator.validator.tasks.AsyncSubtensor",
    lambda network: MockAsyncSubtensor(raise_on_metagraph=True),
)
async def test_async_metagraph__fetch_error():
    assert await SystemEvent.objects.acount() == 0, "No system events should be created before task"

    await async_metagraph()

    event = await SystemEvent.objects.aget(
        type=SystemEvent.EventType.METAGRAPH_SYNCING,
        subtype=SystemEvent.EventSubType.SUBTENSOR_CONNECTIVITY_ERROR,
    )
    assert "failure" in event.long_description
