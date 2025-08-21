from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from compute_horde.protocol_messages import (
    GenericError,
    V0MainHotkeyMessage,
)

from compute_horde_miner.miner.miner_consumer.validator_interface import (
    MinerValidatorConsumer,
)


@pytest.mark.django_db
@pytest.mark.asyncio
async def test_main_hotkey_request_authenticated():
    """Test handling V0MainHotkeyRequest when validator is authenticated."""
    consumer = MagicMock()
    consumer.validator_authenticated = True
    consumer.validator_key = "test_miner_validator"
    consumer.send = AsyncMock()

    expected_main_hotkey = "hotkey1"
    mock_executor_manager = AsyncMock()
    mock_executor_manager.get_main_hotkey = AsyncMock(return_value=expected_main_hotkey)

    with patch(
        "compute_horde_miner.miner.executor_manager.current.executor_manager", mock_executor_manager
    ):
        real_consumer = MinerValidatorConsumer(MagicMock())
        real_consumer.validator_authenticated = True
        real_consumer.validator_key = "test_miner_validator"
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
        real_consumer.validator_key = "test_miner_validator"
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
        real_consumer.validator_key = "test_miner_validator"
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
        real_consumer.validator_key = "test_miner_validator"
        real_consumer.send = AsyncMock()

        request = V0MainHotkeyMessage()
        await real_consumer.handle_main_hotkey_request(request)

        real_consumer.send.assert_called_once()
        call_args = real_consumer.send.call_args[0][0]
        response = V0MainHotkeyMessage.model_validate_json(call_args)
        assert response.main_hotkey == ""
