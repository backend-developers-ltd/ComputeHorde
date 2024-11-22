from unittest.mock import (
    Mock,
    patch,
)

import pytest
from django.conf import settings

from compute_horde_miner.miner.executor_manager._internal.selector import (
    HistoricalRandomMinerSelector,
)


@pytest.mark.asyncio
@pytest.mark.django_db(transaction=True)
@patch("bittensor.Subtensor")
async def test_all_active_neurons(
    mock_subtensor,
):
    selector = HistoricalRandomMinerSelector(
        seed="SECRET",
    )
    mock_subtensor.return_value.get_current_block.return_value = 3023723
    mock_subtensor.return_value.neurons_lite.return_value = [
        Mock(
            hotkey="HotkeyA",
        ),
        Mock(
            hotkey="HotkeyB",
        ),
        Mock(
            hotkey="HotkeyC",
        ),
    ]

    res = await selector.active([
        "HotkeyA",
        "HotkeyB",
        "HotkeyC",
    ])

    assert res == "HotkeyC"

    mock_subtensor.reset_mock()


@pytest.mark.asyncio
@pytest.mark.django_db(transaction=True)
@patch("bittensor.Subtensor")
async def test_no_active_neurons(
    mock_subtensor,
):
    mock_subtensor.return_value.get_current_block.return_value = 3023723
    mock_subtensor.return_value.neurons_lite.return_value = [
        Mock(
            hotkey="HotkeyX",
        ),
        Mock(
            hotkey="HotkeyY",
        ),
        Mock(
            hotkey="HotkeyZ",
        ),
    ]

    selector = HistoricalRandomMinerSelector(
        seed="SECRET",
    )

    res = await selector.active([
        "HotkeyA",
        "HotkeyB",
        "HotkeyC",
    ])

    assert res is None


@pytest.mark.asyncio
@pytest.mark.django_db(transaction=True)
@patch("bittensor.Subtensor")
async def test_only_one_active_neurons(
    mock_subtensor,
):
    mock_subtensor.return_value.get_current_block.return_value = 3023723
    mock_subtensor.return_value.neurons_lite.return_value = [
        Mock(
            hotkey="HotkeyA",
        ),
    ]

    selector = HistoricalRandomMinerSelector(
        seed="SECRET",
    )

    res = await selector.active([
        "HotkeyA",
        "HotkeyB",
        "HotkeyC",
    ])

    assert res == "HotkeyA"

    mock_subtensor.return_value.get_current_block.return_value = 3023723 + 722

    res = await selector.active([
        "HotkeyA",
        "HotkeyB",
        "HotkeyC",
    ])

    assert res == "HotkeyA"


@pytest.mark.asyncio
@pytest.mark.django_db(transaction=True)
@patch("bittensor.Subtensor")
async def test_cache_active_neurons(
    mock_subtensor,
):
    mock_subtensor.return_value.get_current_block.return_value = 3023723
    mock_subtensor.return_value.neurons_lite.return_value = [
        Mock(
            hotkey="HotkeyA",
        ),
    ]

    selector = HistoricalRandomMinerSelector(
        seed="SECRET",
    )

    res1 = await selector.active([
        "HotkeyA",
        "HotkeyB",
        "HotkeyC",
    ])
    res2 = await selector.active([
        "HotkeyA",
        "HotkeyB",
        "HotkeyC",
    ])

    assert res1 == res2 == "HotkeyA"

    mock_subtensor.return_value.neurons_lite.assert_called_once_with(settings.BITTENSOR_NETUID, 3023722)
