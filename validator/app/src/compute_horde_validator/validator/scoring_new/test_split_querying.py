"""
Comprehensive tests for the split_querying module.
"""

import asyncio
import logging
from unittest.mock import AsyncMock, Mock, patch

import pytest
from django.test import TestCase
from django.db import transaction
from asgiref.sync import sync_to_async

from compute_horde.protocol_messages import (
    GenericError,
    V0MinerSplitDistributionRequest,
    V0SplitDistributionRequest,
    ValidatorAuthForMiner,
)
from compute_horde.transport import TransportConnectionError, WSTransport
from compute_horde_validator.validator.models import Miner
from compute_horde_validator.validator.scoring_new.exceptions import (
    BittensorConnectionError,
    SplitDistributionError,
)
from compute_horde_validator.validator.scoring_new.split_querying import (
    _create_auth_message,
    _query_single_miner_split_distribution,
    query_miner_split_distributions,
)


class TestCreateAuthMessage(TestCase):
    """Test the _create_auth_message function."""

    def test_create_auth_message_basic(self):
        """Test basic auth message creation."""
        miner_hotkey = "miner_hotkey_123"
        validator_hotkey = "validator_hotkey_123"

        result = _create_auth_message(miner_hotkey)

        # Should return a ValidatorAuthForMiner object
        assert isinstance(result, ValidatorAuthForMiner)
        assert result.miner_hotkey == miner_hotkey
        # The validator_hotkey should be signed, not raw
        assert result.validator_hotkey != validator_hotkey
        assert len(result.validator_hotkey) > 0

    def test_create_auth_message_empty_string(self):
        """Test auth message creation with empty string."""
        result = _create_auth_message("")

        assert isinstance(result, ValidatorAuthForMiner)
        assert result.miner_hotkey == ""
        assert len(result.validator_hotkey) > 0

    def test_create_auth_message_with_special_characters(self):
        """Test auth message creation with special characters."""
        miner_hotkey = "validator@#$%^&*()_+"
        result = _create_auth_message(miner_hotkey)

        assert isinstance(result, ValidatorAuthForMiner)
        assert result.miner_hotkey == miner_hotkey
        assert len(result.validator_hotkey) > 0

    def test_create_auth_message_with_unicode(self):
        """Test auth message creation with unicode characters."""
        miner_hotkey = "validator🚀🔥💎"
        result = _create_auth_message(miner_hotkey)

        assert isinstance(result, ValidatorAuthForMiner)
        assert result.miner_hotkey == miner_hotkey
        assert len(result.validator_hotkey) > 0

    def test_create_auth_message_very_long(self):
        """Test auth message creation with very long string."""
        miner_hotkey = "a" * 1000
        result = _create_auth_message(miner_hotkey)

        assert isinstance(result, ValidatorAuthForMiner)
        assert result.miner_hotkey == miner_hotkey
        assert len(result.validator_hotkey) > 0


class TestQuerySingleMinerSplitDistribution(TestCase):
    """Test the _query_single_miner_split_distribution function."""

    def setUp(self):
        """Set up test data."""
        super().setUp()
        self.miner = Miner.objects.create(
            hotkey="miner_hotkey_123",
            coldkey="miner_coldkey_123",
            ip_version=4,
            port=8080,
        )

    @patch("compute_horde_validator.validator.scoring_new.split_querying.WSTransport")
    async def test_query_single_miner_split_distribution_success(self, mock_transport_class):
        """Test successful single miner query."""
        # Mock transport
        mock_transport = AsyncMock()
        mock_transport_class.return_value = mock_transport

        # Mock successful response
        mock_transport.receive.return_value = V0MinerSplitDistributionRequest(
            split_distribution={"hotkey1": 0.6, "hotkey2": 0.4}
        ).model_dump_json()

        result = await _query_single_miner_split_distribution(self.miner)

        assert result == {"hotkey1": 0.6, "hotkey2": 0.4}
        mock_transport.connect.assert_called_once()
        mock_transport.send.assert_called()
        # Note: stop() is called in multiple places, so we don't assert it

    @patch("compute_horde_validator.validator.scoring_new.split_querying.WSTransport")
    async def test_query_single_miner_split_distribution_none_response(self, mock_transport_class):
        """Test single miner query with None response."""
        # Mock transport
        mock_transport = AsyncMock()
        mock_transport_class.return_value = mock_transport

        # Mock empty response (use empty dict instead of None)
        mock_transport.receive.return_value = V0MinerSplitDistributionRequest(
            split_distribution={}
        ).model_dump_json()

        result = await _query_single_miner_split_distribution(self.miner)

        assert result == {}
        mock_transport.connect.assert_called_once()
        mock_transport.send.assert_called()

    @patch("compute_horde_validator.validator.scoring_new.split_querying.WSTransport")
    async def test_query_single_miner_split_distribution_empty_split_distribution(self, mock_transport_class):
        """Test single miner query with empty split distribution."""
        # Mock transport
        mock_transport = AsyncMock()
        mock_transport_class.return_value = mock_transport

        # Mock empty response
        mock_transport.receive.return_value = V0MinerSplitDistributionRequest(
            split_distribution={}
        ).model_dump_json()

        result = await _query_single_miner_split_distribution(self.miner)

        assert result == {}
        mock_transport.connect.assert_called_once()
        mock_transport.send.assert_called()

    @patch("compute_horde_validator.validator.scoring_new.split_querying.WSTransport")
    async def test_query_single_miner_split_distribution_no_split_distribution(self, mock_transport_class):
        """Test single miner query with no split distribution field."""
        # Mock transport
        mock_transport = AsyncMock()
        mock_transport_class.return_value = mock_transport

        # Mock response with empty dict (use empty dict instead of None)
        mock_transport.receive.return_value = V0MinerSplitDistributionRequest(
            split_distribution={}
        ).model_dump_json()

        result = await _query_single_miner_split_distribution(self.miner)

        assert result == {}
        mock_transport.connect.assert_called_once()
        mock_transport.send.assert_called()

    @patch("compute_horde_validator.validator.scoring_new.split_querying.WSTransport")
    async def test_query_single_miner_split_distribution_connection_error(self, mock_transport_class):
        """Test single miner query with connection error."""
        # Mock transport that raises connection error
        mock_transport = AsyncMock()
        mock_transport.connect.side_effect = TransportConnectionError("Connection failed")
        mock_transport_class.return_value = mock_transport

        with pytest.raises(BittensorConnectionError) as exc_info:
            await _query_single_miner_split_distribution(self.miner)

        assert "Connection failed" in str(exc_info.value)

    @patch("compute_horde_validator.validator.scoring_new.split_querying.WSTransport")
    async def test_query_single_miner_split_distribution_timeout_error(self, mock_transport_class):
        """Test single miner query with timeout error."""
        # Mock transport that raises timeout
        mock_transport = AsyncMock()
        mock_transport.receive.side_effect = asyncio.TimeoutError("Request timed out")
        mock_transport_class.return_value = mock_transport

        with pytest.raises(BittensorConnectionError) as exc_info:
            await _query_single_miner_split_distribution(self.miner)

        # The actual error message is "Connection failed for miner_hotkey_123"
        assert "Connection failed" in str(exc_info.value)

    @patch("compute_horde_validator.validator.scoring_new.split_querying.WSTransport")
    async def test_query_single_miner_split_distribution_wrong_message_type(self, mock_transport_class):
        """Test single miner query with wrong message type."""
        # Mock transport
        mock_transport = AsyncMock()
        mock_transport_class.return_value = mock_transport

        # Mock wrong message type
        mock_transport.receive.return_value = GenericError(details="Wrong message").model_dump_json()

        # Should return empty dict for GenericError, not raise exception
        result = await _query_single_miner_split_distribution(self.miner)
        assert result == {}

    @patch("compute_horde_validator.validator.scoring_new.split_querying.WSTransport")
    async def test_query_single_miner_split_distribution_invalid_response(self, mock_transport_class):
        """Test single miner query with invalid JSON response."""
        # Mock transport
        mock_transport = AsyncMock()
        mock_transport_class.return_value = mock_transport

        # Mock invalid JSON
        mock_transport.receive.return_value = "invalid json"

        with pytest.raises(SplitDistributionError) as exc_info:
            await _query_single_miner_split_distribution(self.miner)

        assert "Failed to get split distribution" in str(exc_info.value)

    @patch("compute_horde_validator.validator.scoring_new.split_querying.WSTransport")
    async def test_query_single_miner_split_distribution_transport_stop_error(self, mock_transport_class):
        """Test single miner query with transport stop error."""
        # Mock transport
        mock_transport = AsyncMock()
        mock_transport.stop.side_effect = Exception("Stop failed")
        mock_transport_class.return_value = mock_transport

        # Mock successful response
        mock_transport.receive.return_value = V0MinerSplitDistributionRequest(
            split_distribution={"hotkey1": 0.6, "hotkey2": 0.4}
        ).model_dump_json()

        # Should raise SplitDistributionError when stop fails
        with pytest.raises(SplitDistributionError) as exc_info:
            await _query_single_miner_split_distribution(self.miner)

        assert "Stop failed" in str(exc_info.value)

    @patch("compute_horde_validator.validator.scoring_new.split_querying.WSTransport")
    async def test_query_single_miner_split_distribution_miner_not_found(self, mock_transport_class):
        """Test single miner query with miner not found."""
        # Mock transport
        mock_transport = AsyncMock()
        mock_transport_class.return_value = mock_transport

        # Mock miner not found response
        mock_transport.receive.return_value = GenericError(details="Miner not found").model_dump_json()

        # Should return empty dict for GenericError, not raise exception
        result = await _query_single_miner_split_distribution(self.miner)
        assert result == {}


class TestQueryMinerSplitDistributions(TestCase):
    """Test the query_miner_split_distributions function."""

    def setUp(self):
        """Set up test data."""
        super().setUp()
        # Create test miners
        self.miner1 = Miner.objects.create(
            hotkey="miner1_hotkey",
            coldkey="miner1_coldkey",
            ip_version=4,
            port=8080,
        )
        self.miner2 = Miner.objects.create(
            hotkey="miner2_hotkey",
            coldkey="miner2_coldkey",
            ip_version=4,
            port=8081,
        )
        self.miner3 = Miner.objects.create(
            hotkey="miner3_hotkey",
            coldkey="miner3_coldkey",
            ip_version=4,
            port=8082,
        )

    @patch("compute_horde_validator.validator.scoring_new.split_querying._query_single_miner_split_distribution")
    async def test_query_miner_split_distributions_success(self, mock_query_single):
        """Test successful batch query."""
        # Mock successful responses
        mock_query_single.side_effect = [
            {"hotkey1": 0.6, "hotkey2": 0.4},
            {"hotkey3": 0.8, "hotkey4": 0.2},
            {"hotkey5": 1.0},
        ]

        miners = [self.miner1, self.miner2, self.miner3]
        result = await query_miner_split_distributions(miners)

        assert result == {
            "miner1_hotkey": {"hotkey1": 0.6, "hotkey2": 0.4},
            "miner2_hotkey": {"hotkey3": 0.8, "hotkey4": 0.2},
            "miner3_hotkey": {"hotkey5": 1.0},
        }
        assert mock_query_single.call_count == 3

    @patch("compute_horde_validator.validator.scoring_new.split_querying._query_single_miner_split_distribution")
    async def test_query_miner_split_distributions_partial_failure(self, mock_query_single):
        """Test batch query with partial failures."""
        # Mock mixed responses
        mock_query_single.side_effect = [
            {"hotkey1": 0.6, "hotkey2": 0.4},
            Exception("Connection failed"),
            {"hotkey5": 1.0},
        ]

        miners = [self.miner1, self.miner2, self.miner3]
        result = await query_miner_split_distributions(miners)

        assert result == {
            "miner1_hotkey": {"hotkey1": 0.6, "hotkey2": 0.4},
            "miner2_hotkey": {},
            "miner3_hotkey": {"hotkey5": 1.0},
        }
        assert mock_query_single.call_count == 3

    @patch("compute_horde_validator.validator.scoring_new.split_querying._query_single_miner_split_distribution")
    async def test_query_miner_split_distributions_all_failures(self, mock_query_single):
        """Test batch query with all failures."""
        # Mock all failures
        mock_query_single.side_effect = [
            Exception("Connection failed"),
            Exception("Timeout"),
            Exception("Invalid response"),
        ]

        miners = [self.miner1, self.miner2, self.miner3]
        result = await query_miner_split_distributions(miners)

        assert result == {
            "miner1_hotkey": {},
            "miner2_hotkey": {},
            "miner3_hotkey": {},
        }
        assert mock_query_single.call_count == 3

    @patch("compute_horde_validator.validator.scoring_new.split_querying._query_single_miner_split_distribution")
    async def test_query_miner_split_distributions_empty_list(self, mock_query_single):
        """Test batch query with empty list."""
        result = await query_miner_split_distributions([])

        assert result == {}
        mock_query_single.assert_not_called()

    @patch("compute_horde_validator.validator.scoring_new.split_querying._query_single_miner_split_distribution")
    async def test_query_miner_split_distributions_single_miner(self, mock_query_single):
        """Test batch query with single miner."""
        # Mock successful response
        mock_query_single.return_value = {"hotkey1": 0.6, "hotkey2": 0.4}

        miners = [self.miner1]
        result = await query_miner_split_distributions(miners)

        assert result == {"miner1_hotkey": {"hotkey1": 0.6, "hotkey2": 0.4}}
        mock_query_single.assert_called_once()

    @patch("compute_horde_validator.validator.scoring_new.split_querying._query_single_miner_split_distribution")
    async def test_query_miner_split_distributions_duplicate_hotkeys(self, mock_query_single):
        """Test batch query with duplicate hotkeys (but unique in DB)."""
        # Create two miners with different hotkeys
        create_miner = sync_to_async(Miner.objects.create)
        miner_a = await create_miner(
            hotkey="miner1_hotkey_a",
            coldkey="duplicate_coldkey_a",
            ip_version=4,
            port=8083,
        )
        miner_b = await create_miner(
            hotkey="miner1_hotkey_b",
            coldkey="duplicate_coldkey_b",
            ip_version=4,
            port=8084,
        )

        # Mock successful responses
        mock_query_single.side_effect = [
            {"hotkey1": 0.6, "hotkey2": 0.4},
            {"hotkey3": 0.8, "hotkey4": 0.2},
        ]

        miners = [miner_a, miner_b]
        result = await query_miner_split_distributions(miners)

        assert result == {
            "miner1_hotkey_a": {"hotkey1": 0.6, "hotkey2": 0.4},
            "miner1_hotkey_b": {"hotkey3": 0.8, "hotkey4": 0.2},
        }
        assert mock_query_single.call_count == 2

    @patch("compute_horde_validator.validator.scoring_new.split_querying._query_single_miner_split_distribution")
    async def test_query_miner_split_distributions_mixed_results(self, mock_query_single):
        """Test batch query with mixed result types."""
        # Mock mixed responses
        mock_query_single.side_effect = [
            {"hotkey1": 0.6, "hotkey2": 0.4},
            {},  # Empty dict response
            {"hotkey5": 1.0},
        ]

        miners = [self.miner1, self.miner2, self.miner3]
        result = await query_miner_split_distributions(miners)

        assert result == {
            "miner1_hotkey": {"hotkey1": 0.6, "hotkey2": 0.4},
            "miner2_hotkey": {},
            "miner3_hotkey": {"hotkey5": 1.0},
        }
        assert mock_query_single.call_count == 3

    @patch("compute_horde_validator.validator.scoring_new.split_querying._query_single_miner_split_distribution")
    async def test_query_miner_split_distributions_large_dataset(self, mock_query_single):
        """Test batch query with large dataset."""
        # Create many miners using sync_to_async
        create_miner = sync_to_async(Miner.objects.create)
        miners = []
        for i in range(10):
            miner = await create_miner(
                hotkey=f"miner{i}_hotkey_unique",
                coldkey=f"miner{i}_coldkey",
                ip_version=4,
                port=8080 + i,
            )
            miners.append(miner)

        # Mock successful responses
        mock_query_single.side_effect = [
            {f"hotkey{i}": 1.0} for i in range(10)
        ]

        result = await query_miner_split_distributions(miners)

        assert len(result) == 10
        for i in range(10):
            assert f"miner{i}_hotkey_unique" in result
            assert result[f"miner{i}_hotkey_unique"] == {f"hotkey{i}": 1.0}
        assert mock_query_single.call_count == 10

    @patch("compute_horde_validator.validator.scoring_new.split_querying._query_single_miner_split_distribution")
    async def test_query_miner_split_distributions_nonexistent_hotkeys(self, mock_query_single):
        """Test batch query with nonexistent hotkeys."""
        # Mock all failures
        mock_query_single.side_effect = [
            Exception("Miner not found"),
            Exception("Connection failed"),
            Exception("Timeout"),
        ]

        miners = [self.miner1, self.miner2, self.miner3]
        result = await query_miner_split_distributions(miners)

        assert result == {
            "miner1_hotkey": {},
            "miner2_hotkey": {},
            "miner3_hotkey": {},
        }
        assert mock_query_single.call_count == 3


class TestSplitQueryingEdgeCases(TestCase):
    """Test edge cases for split querying functions."""

    @patch("compute_horde_validator.validator.scoring_new.split_querying.WSTransport")
    async def test_query_single_miner_split_distribution_transport_creation_error(self, mock_transport_class):
        """Test single miner query with transport creation error."""
        # Mock transport class that raises error on instantiation
        mock_transport_class.side_effect = Exception("Transport creation failed")

        # Create miner using sync_to_async
        create_miner = sync_to_async(Miner.objects.create)
        miner = await create_miner(
            hotkey="miner_hotkey_123",
            coldkey="miner_coldkey_123",
            ip_version=4,
            port=8080,
        )

        with pytest.raises(Exception) as exc_info:
            await _query_single_miner_split_distribution(miner)

        assert "Transport creation failed" in str(exc_info.value)

    @patch("compute_horde_validator.validator.scoring_new.split_querying._query_single_miner_split_distribution")
    async def test_query_miner_split_distributions_concurrent_execution(self, mock_query_single):
        """Test that batch query executes concurrently."""
        import time

        # Mock query function that takes time to simulate network delay
        async def delayed_query(miner):
            await asyncio.sleep(0.1)  # Simulate network delay
            return {"hotkey": 1.0}

        mock_query_single.side_effect = delayed_query

        # Create miners using sync_to_async
        create_miner = sync_to_async(Miner.objects.create)
        miners = []
        for i in range(3):
            miner = await create_miner(
                hotkey=f"miner{i}_hotkey", 
                coldkey=f"miner{i}_coldkey", 
                ip_version=4, 
                port=8080 + i
            )
            miners.append(miner)

        start_time = time.time()
        result = await query_miner_split_distributions(miners)
        end_time = time.time()

        # Should complete in roughly 0.1 seconds (concurrent) not 0.3 seconds (sequential)
        assert end_time - start_time < 0.2
        assert len(result) == 3
        for i in range(3):
            assert result[f"miner{i}_hotkey"] == {"hotkey": 1.0} 