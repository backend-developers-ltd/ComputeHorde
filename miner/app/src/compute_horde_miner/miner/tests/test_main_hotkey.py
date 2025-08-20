from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from compute_horde.protocol_messages import (
    GenericError,
    V0MainHotkeyMessage,
)
from compute_horde_core.executor_class import ExecutorClass

from compute_horde_miner.miner.executor_manager.v1 import BaseExecutorManager
from compute_horde_miner.miner.miner_consumer.validator_interface import (
    MinerValidatorConsumer,
)


@pytest.mark.django_db
@pytest.mark.asyncio
async def test_main_hotkey_request_authenticated():
    """Test handling V0MainHotkeyRequest when validator is authenticated."""
    consumer = MagicMock()
    consumer.validator_authenticated = True
    consumer.validator_key = "test_validator"
    consumer.send = AsyncMock()

    expected_main_hotkey = "hotkey1"
    mock_executor_manager = AsyncMock()
    mock_executor_manager.get_main_hotkey = AsyncMock(return_value=expected_main_hotkey)

    with patch(
        "compute_horde_miner.miner.executor_manager.current.executor_manager", mock_executor_manager
    ):
        real_consumer = MinerValidatorConsumer(MagicMock())
        real_consumer.validator_authenticated = True
        real_consumer.validator_key = "test_validator"
        real_consumer.send = AsyncMock()

        request = V0MainHotkeyMessage()
        await real_consumer.handle_main_hotkey_request(request)

        real_consumer.send.assert_called_once()
        call_args = real_consumer.send.call_args[0][0]
        response = V0MainHotkeyMessage.model_validate_json(call_args)
        assert response.main_hotkey == expected_main_hotkey


@pytest.mark.django_db
@pytest.mark.asyncio
async def test_main_hotkey_request_unauthenticated():
    """Test handling V0MainHotkeyRequest when validator is not authenticated."""
    with patch("compute_horde_miner.miner.executor_manager.current.executor_manager", MagicMock()):
        real_consumer = MinerValidatorConsumer(MagicMock())
        real_consumer.validator_authenticated = False
        real_consumer.validator_key = "test_validator"
        real_consumer.send = AsyncMock()

        # Send main hotkey request
        request = V0MainHotkeyMessage()
        await real_consumer.handle_main_hotkey_request(request)

        # Verify the error response
        real_consumer.send.assert_called_once()
        call_args = real_consumer.send.call_args[0][0]
        response = GenericError.model_validate_json(call_args)
        assert "Unauthenticated validator" in response.details


@pytest.mark.django_db
@pytest.mark.asyncio
async def test_main_hotkey_request_none_response():
    """Test handling V0MainHotkeyRequest when executor manager returns None."""
    mock_executor_manager = AsyncMock()
    mock_executor_manager.get_main_hotkey = AsyncMock(return_value=None)

    with patch(
        "compute_horde_miner.miner.executor_manager.current.executor_manager", mock_executor_manager
    ):
        real_consumer = MinerValidatorConsumer(MagicMock())
        real_consumer.validator_authenticated = True
        real_consumer.validator_key = "test_validator"
        real_consumer.send = AsyncMock()

        request = V0MainHotkeyMessage()
        await real_consumer.handle_main_hotkey_request(request)

        real_consumer.send.assert_called_once()
        call_args = real_consumer.send.call_args[0][0]
        response = V0MainHotkeyMessage.model_validate_json(call_args)
        assert response.main_hotkey is None


@pytest.mark.django_db
@pytest.mark.asyncio
async def test_main_hotkey_request_empty_string_response():
    """Test handling V0MainHotkeyRequest when executor manager returns empty string."""
    mock_executor_manager = AsyncMock()
    mock_executor_manager.get_main_hotkey = AsyncMock(return_value="")

    with patch(
        "compute_horde_miner.miner.executor_manager.current.executor_manager", mock_executor_manager
    ):
        real_consumer = MinerValidatorConsumer(MagicMock())
        real_consumer.validator_authenticated = True
        real_consumer.validator_key = "test_validator"
        real_consumer.send = AsyncMock()

        request = V0MainHotkeyMessage()
        await real_consumer.handle_main_hotkey_request(request)

        real_consumer.send.assert_called_once()
        call_args = real_consumer.send.call_args[0][0]
        response = V0MainHotkeyMessage.model_validate_json(call_args)
        assert response.main_hotkey == ""


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
async def test_get_main_hotkey__hotkeys_not_configured(settings, miner_wallet):
    settings.HOTKEYS_FOR_MAIN_HOTKEY_SELECTION = []
    executor_manager = DummyExecutorManager(AsyncMock(return_value=100))
    assert await executor_manager.get_main_hotkey() == miner_wallet.hotkey.ss58_address


@pytest.mark.asyncio
async def test_get_main_hotkey__hotkeys_configured__subtensor_error(settings, miner_wallet):
    settings.HOTKEYS_FOR_MAIN_HOTKEY_SELECTION = ["a", "b", "c"]
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
