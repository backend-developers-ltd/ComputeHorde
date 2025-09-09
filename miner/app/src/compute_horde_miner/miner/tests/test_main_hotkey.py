import json
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from compute_horde.test_wallet import get_test_miner_wallet
from compute_horde_core.executor_class import ExecutorClass
from django.test import AsyncClient

from compute_horde_miner.miner.executor_manager.v1 import BaseExecutorManager
from compute_horde_miner.miner.views import get_main_hotkey


@pytest.mark.django_db
@pytest.mark.asyncio
async def test_get_main_hotkey_success():
    """Test HTTP endpoint for getting main hotkey successfully."""
    expected_main_hotkey = "hotkey1"
    mock_executor_manager = AsyncMock()
    mock_executor_manager.get_main_hotkey = AsyncMock(return_value=expected_main_hotkey)

    with patch("compute_horde_miner.miner.views.current.executor_manager", mock_executor_manager):
        request = MagicMock()
        response = await get_main_hotkey(request)

        assert response.status_code == 200
        data = json.loads(response.content)
        assert data["main_hotkey"] == expected_main_hotkey


@pytest.mark.django_db
@pytest.mark.asyncio
async def test_get_main_hotkey_none_response():
    """Test HTTP endpoint for getting main hotkey when executor manager returns None."""
    mock_executor_manager = AsyncMock()
    mock_executor_manager.get_main_hotkey = AsyncMock(return_value=None)

    with patch("compute_horde_miner.miner.views.current.executor_manager", mock_executor_manager):
        request = MagicMock()
        response = await get_main_hotkey(request)

        assert response.status_code == 200
        data = json.loads(response.content)
        assert data["main_hotkey"] is None


@pytest.mark.django_db
@pytest.mark.asyncio
async def test_get_main_hotkey_empty_string_response():
    """Test HTTP endpoint for getting main hotkey when executor manager returns empty string."""
    mock_executor_manager = AsyncMock()
    mock_executor_manager.get_main_hotkey = AsyncMock(return_value="")

    with patch("compute_horde_miner.miner.views.current.executor_manager", mock_executor_manager):
        request = MagicMock()
        response = await get_main_hotkey(request)

        assert response.status_code == 200
        data = json.loads(response.content)
        assert data["main_hotkey"] == ""


@pytest.mark.django_db
@pytest.mark.asyncio
async def test_get_main_hotkey_exception():
    """Test HTTP endpoint for getting main hotkey when executor manager raises an exception."""
    mock_executor_manager = AsyncMock()
    mock_executor_manager.get_main_hotkey = AsyncMock(side_effect=Exception("Test error"))

    with patch("compute_horde_miner.miner.views.current.executor_manager", mock_executor_manager):
        request = MagicMock()
        response = await get_main_hotkey(request)

        assert response.status_code == 500
        data = json.loads(response.content)
        assert "error" in data
        assert data["error"] == "Could not get main hotkey"


@pytest.mark.django_db
@pytest.mark.asyncio
async def test_get_main_hotkey_url():
    """Test that the hotkey URL endpoint works correctly."""
    expected_main_hotkey = "hotkey1"
    mock_executor_manager = AsyncMock()
    mock_executor_manager.get_main_hotkey = AsyncMock(return_value=expected_main_hotkey)

    with patch("compute_horde_miner.miner.views.current.executor_manager", mock_executor_manager):
        client = AsyncClient()
        response = await client.get("/v0.1/hotkey")

        assert response.status_code == 200
        data = json.loads(response.content)
        assert data["main_hotkey"] == expected_main_hotkey


class DummyExecutorManager(BaseExecutorManager):
    def __init__(self, get_current_block_mock):
        subtensor_mock = MagicMock()
        subtensor_mock.get_current_block = get_current_block_mock
        super().__init__(subtensor=subtensor_mock)

    async def start_new_executor(self, token, executor_class, timeout): ...
    async def kill_executor(self, executor): ...
    async def wait_for_executor(self, executor, timeout): ...
    async def get_manifest(self) -> dict[ExecutorClass, int]: ...


@pytest.mark.asyncio
async def test_get_main_hotkey__hotkeys_not_configured(settings):
    settings.HOTKEYS_FOR_MAIN_HOTKEY_SELECTION = []
    miner_wallet = get_test_miner_wallet()
    executor_manager = DummyExecutorManager(AsyncMock(return_value=100))
    assert await executor_manager.get_main_hotkey() == miner_wallet.hotkey.ss58_address


@pytest.mark.asyncio
async def test_get_main_hotkey__hotkeys_configured__subtensor_error(settings):
    settings.HOTKEYS_FOR_MAIN_HOTKEY_SELECTION = ["a", "b", "c"]
    miner_wallet = get_test_miner_wallet()
    executor_manager = DummyExecutorManager(AsyncMock(side_effect=ConnectionError))
    assert await executor_manager.get_main_hotkey() == miner_wallet.hotkey.ss58_address


@pytest.mark.parametrize(
    ("current_block", "expected_hotkey"),
    [
        # cycle 0: [-3, 719)
        (0, "a"),
        (718, "a"),
        # cycle 1: [719, 1441)
        (719, "b"),
        # cycle 2: [1441, 2163)
        (2000, "c"),
        # cycle 3: [2163, 2885)
        (2200, "a"),
    ],
)
@pytest.mark.asyncio
async def test_get_main_hotkey__hotkeys_configured__success(
    settings, current_block, expected_hotkey
):
    settings.BITTENSOR_NETUID = 1
    settings.HOTKEYS_FOR_MAIN_HOTKEY_SELECTION = ["a", "b", "c"]
    executor_manager = DummyExecutorManager(AsyncMock(return_value=current_block))
    got_hotkey = await executor_manager.get_main_hotkey()
    assert got_hotkey == expected_hotkey
