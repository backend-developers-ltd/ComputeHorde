from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from compute_horde.protocol_messages import (
    GenericError,
    V0MinerSplitDistributionRequest,
    V0SplitDistributionRequest,
)


@pytest.mark.django_db
@pytest.mark.asyncio
async def test_split_distribution_request_authenticated():
    """Test handling V0SplitDistributionRequest when validator is authenticated."""
    consumer = MagicMock()
    consumer.validator_authenticated = True
    consumer.validator_key = "test_validator"
    consumer.send = AsyncMock()

    expected_split_distribution = {"hotkey1": 0.3, "hotkey2": 0.4, "hotkey3": 0.2, "hotkey4": 0.1}
    mock_executor_manager = AsyncMock()
    mock_executor_manager.get_split_distribution = AsyncMock(
        return_value=expected_split_distribution
    )

    with patch(
        "compute_horde_miner.miner.executor_manager.current.executor_manager", mock_executor_manager
    ):
        from compute_horde_miner.miner.miner_consumer.validator_interface import (
            MinerValidatorConsumer,
        )

        real_consumer = MinerValidatorConsumer(MagicMock())
        real_consumer.validator_authenticated = True
        real_consumer.validator_key = "test_validator"
        real_consumer.send = AsyncMock()

        request = V0SplitDistributionRequest()
        await real_consumer.handle_split_distribution_request(request)

        real_consumer.send.assert_called_once()
        call_args = real_consumer.send.call_args[0][0]
        response = V0MinerSplitDistributionRequest.model_validate_json(call_args)
        assert response.split_distribution == expected_split_distribution


@pytest.mark.django_db
@pytest.mark.asyncio
async def test_split_distribution_request_unauthenticated():
    """Test handling V0SplitDistributionRequest when validator is not authenticated."""
    with patch("compute_horde_miner.miner.executor_manager.current.executor_manager", MagicMock()):
        from compute_horde_miner.miner.miner_consumer.validator_interface import (
            MinerValidatorConsumer,
        )

        real_consumer = MinerValidatorConsumer(MagicMock())
        real_consumer.validator_authenticated = False
        real_consumer.validator_key = "test_validator"
        real_consumer.send = AsyncMock()

        # Send split distribution request
        request = V0SplitDistributionRequest()
        await real_consumer.handle_split_distribution_request(request)

        # Verify the error response
        real_consumer.send.assert_called_once()
        call_args = real_consumer.send.call_args[0][0]
        response = GenericError.model_validate_json(call_args)
        assert "Unauthenticated validator" in response.details


@pytest.mark.django_db
@pytest.mark.asyncio
async def test_split_distribution_request_none_response():
    """Test handling V0SplitDistributionRequest when executor manager returns None."""
    mock_executor_manager = AsyncMock()
    mock_executor_manager.get_split_distribution = AsyncMock(return_value=None)

    with patch(
        "compute_horde_miner.miner.executor_manager.current.executor_manager", mock_executor_manager
    ):
        from compute_horde_miner.miner.miner_consumer.validator_interface import (
            MinerValidatorConsumer,
        )

        real_consumer = MinerValidatorConsumer(MagicMock())
        real_consumer.validator_authenticated = True
        real_consumer.validator_key = "test_validator"
        real_consumer.send = AsyncMock()

        request = V0SplitDistributionRequest()
        await real_consumer.handle_split_distribution_request(request)

        real_consumer.send.assert_called_once()
        call_args = real_consumer.send.call_args[0][0]
        response = V0MinerSplitDistributionRequest.model_validate_json(call_args)
        assert response.split_distribution == {}


@pytest.mark.django_db
@pytest.mark.asyncio
async def test_split_distribution_request_empty_response():
    """Test handling V0SplitDistributionRequest when executor manager returns empty dict."""
    mock_executor_manager = AsyncMock()
    mock_executor_manager.get_split_distribution = AsyncMock(return_value={})

    with patch(
        "compute_horde_miner.miner.executor_manager.current.executor_manager", mock_executor_manager
    ):
        from compute_horde_miner.miner.miner_consumer.validator_interface import (
            MinerValidatorConsumer,
        )

        real_consumer = MinerValidatorConsumer(MagicMock())
        real_consumer.validator_authenticated = True
        real_consumer.validator_key = "test_validator"
        real_consumer.send = AsyncMock()

        request = V0SplitDistributionRequest()
        await real_consumer.handle_split_distribution_request(request)

        real_consumer.send.assert_called_once()
        call_args = real_consumer.send.call_args[0][0]
        response = V0MinerSplitDistributionRequest.model_validate_json(call_args)
        assert response.split_distribution == {}
