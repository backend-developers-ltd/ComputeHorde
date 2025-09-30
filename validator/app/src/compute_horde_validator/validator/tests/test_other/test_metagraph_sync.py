from collections.abc import Iterable

import pytest

from compute_horde_validator.validator.allowance.types import MetagraphData
from compute_horde_validator.validator.models import (
    MetagraphSnapshot,
    Miner,
    SystemEvent,
)
from compute_horde_validator.validator.tasks import sync_metagraph


def _make_metagraph_data(
    neurons: Iterable,
    subnet_state: dict,
    block: int,
) -> MetagraphData:
    neuron_list = list(neurons)
    alpha_stake = list(subnet_state.get("alpha_stake", [0.0] * len(neuron_list)))
    tao_stake = list(subnet_state.get("tao_stake", [0.0] * len(neuron_list)))
    total_stake = list(subnet_state.get("total_stake", [0.0] * len(neuron_list)))

    uids = [getattr(neuron, "uid", index) for index, neuron in enumerate(neuron_list)]
    hotkeys = [
        getattr(neuron, "hotkey", f"hotkey_{index}") for index, neuron in enumerate(neuron_list)
    ]
    coldkeys = [getattr(neuron, "coldkey", None) for neuron in neuron_list]
    serving_hotkeys = [
        neuron.hotkey
        for neuron in neuron_list
        if getattr(getattr(neuron, "axon_info", None), "ip", None) not in (None, "0.0.0.0")
    ]

    subnet_state_payload = {
        "alpha_stake": alpha_stake,
        "tao_stake": tao_stake,
        "total_stake": total_stake,
    }

    return MetagraphData(
        block=block,
        neurons=neuron_list,
        subnet_state=subnet_state_payload,
        alpha_stake=alpha_stake,
        tao_stake=tao_stake,
        total_stake=total_stake,
        uids=uids,
        hotkeys=hotkeys,
        coldkeys=coldkeys,
        serving_hotkeys=serving_hotkeys,
    )


@pytest.mark.django_db(databases=["default", "default_alias"], transaction=True)
def test_metagraph_sync__success(bittensor, mocker):
    assert SystemEvent.objects.count() == 0
    assert Miner.objects.count() == 0

    neurons = bittensor.subnet.return_value.list_neurons.return_value
    n = 5
    override_block = 1099

    subnet_state = bittensor.subnet.return_value.get_state.return_value

    snapshots = [
        _make_metagraph_data(neurons[:n], subnet_state, override_block),
        _make_metagraph_data(neurons[:n], subnet_state, 708),
        _make_metagraph_data(neurons[: n + 1], subnet_state, override_block + 1),
        _make_metagraph_data(neurons[: n + 1], subnet_state, 1431),
        _make_metagraph_data(neurons[: n + 1], subnet_state, 1430),
    ]

    mocker.patch(
        "compute_horde_validator.validator.tasks.fetch_metagraph_snapshot",
        side_effect=snapshots,
    )

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
def test_metagraph_sync__fetch_error(bittensor, mocker):
    assert SystemEvent.objects.count() == 0, "No system events should be created before task"

    mocker.patch(
        "compute_horde_validator.validator.tasks.fetch_metagraph_snapshot",
        side_effect=Exception("Nope"),
    )

    sync_metagraph()

    event = SystemEvent.objects.get(
        type=SystemEvent.EventType.METAGRAPH_SYNCING,
        subtype=SystemEvent.EventSubType.SUBTENSOR_CONNECTIVITY_ERROR,
    )
    assert "Nope" in event.long_description
