from unittest.mock import MagicMock

import pytest

from compute_horde_validator.validator.models import (
    MetagraphSnapshot,
    Miner,
    SystemEvent,
)
from compute_horde_validator.validator.tasks import sync_metagraph


@pytest.mark.django_db(databases=["default", "default_alias"], transaction=True)
def test_metagraph_sync__success(bittensor):
    assert SystemEvent.objects.count() == 0
    assert Miner.objects.count() == 0

    neurons = bittensor.subnet.return_value.list_neurons.return_value
    n = 5
    override_block = 1099

    bittensor.block.side_effect = lambda block_number: MagicMock(
        **{
            "__aenter__.return_value": MagicMock(
                number=block_number or override_block,
            ),
        }
    )
    bittensor.subnet.return_value.list_neurons.return_value = neurons[:n]

    sync_metagraph()

    snapshot = MetagraphSnapshot.get_latest()
    assert snapshot.block == override_block
    assert len(snapshot.hotkeys) == n
    assert len(snapshot.stake) > 0

    snapshot = MetagraphSnapshot.get_cycle_start()
    assert snapshot.block == 708  # 1099 cycle start

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
    assert event.data["block"] == override_block

    # check new miners creation
    assert Miner.objects.count() == n
    for i in range(n):
        assert Miner.objects.get(hotkey=f"hotkey_{i}") is not None

    # check extra miner gets created
    n = 6
    override_block = 1100

    bittensor.block.side_effect = lambda block_number: MagicMock(
        **{
            "__aenter__.return_value": MagicMock(
                number=block_number or override_block,
            ),
        }
    )
    bittensor.subnet.return_value.list_neurons.return_value = neurons[:n]

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
    assert event.data["block"] == override_block

    snapshot = MetagraphSnapshot.get_cycle_start()
    assert snapshot.block == 708  # 708 cycle start

    # check metagraph syncing lagging warns
    override_block = 1431

    bittensor.block.side_effect = lambda block_number: MagicMock(
        **{
            "__aenter__.return_value": MagicMock(
                number=block_number or override_block,
            ),
        }
    )

    sync_metagraph()

    event = SystemEvent.objects.get(
        type=SystemEvent.EventType.METAGRAPH_SYNCING,
        subtype=SystemEvent.EventSubType.WARNING,
    )
    assert event.data["blocks_diff"] == 331

    event = (
        SystemEvent.objects.order_by("-timestamp")
        .filter(
            type=SystemEvent.EventType.VALIDATOR_MINERS_REFRESH,
            subtype=SystemEvent.EventSubType.SUCCESS,
        )
        .first()
    )
    assert event.data["updated_axon_infos"] == 0
    assert event.data["block"] == override_block

    snapshot = MetagraphSnapshot.get_cycle_start()
    assert snapshot.block == 1430  # 708 cycle start


@pytest.mark.django_db(databases=["default", "default_alias"], transaction=True)
def test_metagraph_sync__fetch_error(bittensor):
    assert SystemEvent.objects.count() == 0, "No system events should be created before task"

    bittensor.subnet.return_value.list_neurons.side_effect = Exception("Nope")

    sync_metagraph()

    event = SystemEvent.objects.get(
        type=SystemEvent.EventType.METAGRAPH_SYNCING,
        subtype=SystemEvent.EventSubType.SUBTENSOR_CONNECTIVITY_ERROR,
    )
    assert "Nope" in event.long_description
