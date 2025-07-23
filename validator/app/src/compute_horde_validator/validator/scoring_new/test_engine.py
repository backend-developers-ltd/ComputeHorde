"""
Tests for the scoring engine.
"""

from datetime import timedelta
from unittest.mock import MagicMock, patch

import pytest
from asgiref.sync import async_to_sync
from django.test import TestCase
from django.utils import timezone

from ..models import Cycle, Miner, OrganicJob, SyntheticJob, SyntheticJobBatch
from .engine import DefaultScoringEngine  # Internal access for testing
from .factory import create_scoring_engine
from .interface import ScoringEngine
from .models import MinerSplit, MinerSplitDistribution


class MockScoringEngine(ScoringEngine):
    """Mock implementation of ScoringEngine for testing."""

    def __init__(self, return_scores: dict[str, float]):
        self.return_scores = return_scores

    async def calculate_scores_for_cycles(
        self, current_cycle_start: int, previous_cycle_start: int, validator_hotkey: str
    ) -> dict[str, float]:
        """Return predefined scores for testing."""
        return self.return_scores


class TestScoringEngineInterface(TestCase):
    """Test the ScoringEngine interface with mocks."""

    async def test_mock_scoring_engine(self):
        """Test that we can easily mock the ScoringEngine interface."""
        # Create a mock that returns predefined scores
        mock_scores = {"hotkey1": 10.0, "hotkey2": 5.0}
        mock_engine = MockScoringEngine(mock_scores)

        # Test the interface
        result = await mock_engine.calculate_scores_for_cycles(
            current_cycle_start=1000, previous_cycle_start=278, validator_hotkey="validator_hotkey"
        )

        self.assertEqual(result, mock_scores)

    def test_mock_with_patch(self):
        """Test mocking with patch decorator."""
        with patch.object(DefaultScoringEngine, "calculate_scores_for_cycles") as mock_calculate:
            mock_calculate.return_value = {"hotkey1": 10.0, "hotkey2": 5.0}

            engine = DefaultScoringEngine()
            # Fix: The method is async, so we need to await it
            import asyncio

            result = asyncio.run(engine.calculate_scores_for_cycles(1000, 278, "validator_hotkey"))

            # Fix: Use positional arguments in the assertion
            mock_calculate.assert_called_once_with(1000, 278, "validator_hotkey")
            self.assertEqual(result, {"hotkey1": 10.0, "hotkey2": 5.0})


class TestFactory(TestCase):
    """Test the factory functionality."""

    def test_create_scoring_engine_default(self):
        """Test creating default scoring engine."""
        engine = create_scoring_engine()
        self.assertIsInstance(engine, DefaultScoringEngine)

    def test_create_scoring_engine_explicit_default(self):
        """Test creating default scoring engine with explicit type."""
        engine = create_scoring_engine("default")
        self.assertIsInstance(engine, DefaultScoringEngine)

    def test_create_scoring_engine_invalid_type(self):
        """Test creating scoring engine with invalid type."""
        with self.assertRaises(ValueError) as cm:
            create_scoring_engine("invalid")
        self.assertIn("Unsupported engine type: invalid", str(cm.exception))

    def test_get_available_engine_types(self):
        """Test getting available engine types."""
        from .factory import get_available_engine_types

        types = get_available_engine_types()
        self.assertEqual(types, ["default"])


@pytest.mark.django_db
class TestScoringEngineIntegration(TestCase):
    """Test the scoring engine integration with real data."""

    def setUp(self):
        """Set up test data."""
        super().setUp()

        # Create miners
        self.miner1 = Miner.objects.create(hotkey="hotkey1", coldkey="coldkey1")
        self.miner2 = Miner.objects.create(hotkey="hotkey2", coldkey="coldkey2")
        self.miner3 = Miner.objects.create(hotkey="hotkey3")  # No coldkey

        # Create cycles
        self.current_cycle = Cycle.objects.create(start=1000, stop=1722)
        self.previous_cycle = Cycle.objects.create(start=278, stop=1000)

        # Create batches
        self.current_batch = SyntheticJobBatch.objects.create(
            accepting_results_until=timezone.now() + timedelta(hours=1),
            block=1000,
            cycle=self.current_cycle,
        )
        self.previous_batch = SyntheticJobBatch.objects.create(
            accepting_results_until=timezone.now() + timedelta(hours=1),
            block=278,
            cycle=self.previous_cycle,
        )

    def test_calculate_scores_for_cycles_basic(self):
        """Test basic scoring for two cycles."""
        engine = DefaultScoringEngine()

        # Create organic jobs
        OrganicJob.objects.create(
            miner=self.miner1,
            block=1000,
            status=OrganicJob.Status.COMPLETED,
            cheated=False,
            miner_address_ip_version=4,
            miner_port=8080,
        )
        OrganicJob.objects.create(
            miner=self.miner2,
            block=1001,
            status=OrganicJob.Status.COMPLETED,
            cheated=False,
            miner_address_ip_version=4,
            miner_port=8080,
        )

        # Create synthetic jobs
        SyntheticJob.objects.create(
            miner=self.miner1,
            batch=self.current_batch,
            status=SyntheticJob.Status.COMPLETED,
            score=10.0,
            miner_address_ip_version=4,
            miner_port=8080,
        )
        SyntheticJob.objects.create(
            miner=self.miner3,
            batch=self.current_batch,
            status=SyntheticJob.Status.COMPLETED,
            score=15.0,
            miner_address_ip_version=4,
            miner_port=8080,
        )

        scores = async_to_sync(engine.calculate_scores_for_cycles)(
            current_cycle_start=1000, previous_cycle_start=278, validator_hotkey="validator_hotkey"
        )

        # Should have scores for hotkeys that had jobs
        assert "hotkey1" in scores
        assert "hotkey2" in scores
        assert "hotkey3" in scores

        # hotkey1 should have both organic and synthetic scores
        assert scores["hotkey1"] > 0
        # hotkey2 should have only organic score
        assert scores["hotkey2"] > 0
        # hotkey3 should have only synthetic score
        assert scores["hotkey3"] > 0

    def test_calculate_scores_for_cycles_no_jobs(self):
        """Test scoring when there are no jobs."""
        engine = DefaultScoringEngine()

        scores = async_to_sync(engine.calculate_scores_for_cycles)(
            current_cycle_start=1000, previous_cycle_start=278, validator_hotkey="validator_hotkey"
        )

        # Should return empty dict when no jobs
        assert scores == {}

    def test_calculate_scores_for_cycles_with_splits(self):
        """Test scoring with split distributions."""
        engine = DefaultScoringEngine()

        # Create jobs for miner1 (has coldkey)
        OrganicJob.objects.create(
            miner=self.miner1,
            block=1000,
            status=OrganicJob.Status.COMPLETED,
            cheated=False,
            miner_address_ip_version=4,
            miner_port=8080,
        )
        SyntheticJob.objects.create(
            miner=self.miner1,
            batch=self.current_batch,
            status=SyntheticJob.Status.COMPLETED,
            score=10.0,
            miner_address_ip_version=4,
            miner_port=8080,
        )

        # Create a split for miner1's coldkey
        split = MinerSplit.objects.create(
            coldkey="coldkey1", cycle_start=1000, validator_hotkey="validator_hotkey"
        )
        MinerSplitDistribution.objects.create(split=split, hotkey="hotkey1", percentage=0.6)
        # Add another hotkey to the split (even though it doesn't exist in our test)
        MinerSplitDistribution.objects.create(split=split, hotkey="hotkey1_alt", percentage=0.4)

        scores = async_to_sync(engine.calculate_scores_for_cycles)(
            current_cycle_start=1000, previous_cycle_start=278, validator_hotkey="validator_hotkey"
        )

        # Should have a score for hotkey1
        assert "hotkey1" in scores
        assert scores["hotkey1"] > 0

    def test_calculate_scores_for_cycles_with_dancing_bonus(self):
        """Test scoring with dancing bonus when splits change."""
        engine = DefaultScoringEngine()

        # Create jobs for miner1
        OrganicJob.objects.create(
            miner=self.miner1,
            block=1000,
            status=OrganicJob.Status.COMPLETED,
            cheated=False,
            miner_address_ip_version=4,
            miner_port=8080,
        )

        # Create current split
        current_split = MinerSplit.objects.create(
            coldkey="coldkey1", cycle_start=1000, validator_hotkey="validator_hotkey"
        )
        MinerSplitDistribution.objects.create(split=current_split, hotkey="hotkey1", percentage=0.6)

        # Create previous split with different distribution
        previous_split = MinerSplit.objects.create(
            coldkey="coldkey1", cycle_start=278, validator_hotkey="validator_hotkey"
        )
        MinerSplitDistribution.objects.create(
            split=previous_split,
            hotkey="hotkey1",
            percentage=0.5,  # Different from current (0.6)
        )

        scores = async_to_sync(engine.calculate_scores_for_cycles)(
            current_cycle_start=1000, previous_cycle_start=278, validator_hotkey="validator_hotkey"
        )

        # Should have a score for hotkey1
        assert "hotkey1" in scores
        assert scores["hotkey1"] > 0


@pytest.mark.django_db
class TestScoringEngineUnit:
    """Test individual components of the scoring engine."""

    def test_group_scores_by_coldkey(self):
        """Test grouping scores by coldkey."""
        engine = DefaultScoringEngine()

        # Create test miners
        Miner.objects.create(hotkey="hotkey1", coldkey="coldkey1")
        Miner.objects.create(hotkey="hotkey2", coldkey="coldkey1")
        Miner.objects.create(hotkey="hotkey3", coldkey="coldkey2")
        Miner.objects.create(hotkey="hotkey4")  # No coldkey

        scores = {"hotkey1": 10.0, "hotkey2": 15.0, "hotkey3": 5.0, "hotkey4": 3.0}

        # Test the grouping logic
        coldkey_scores, hotkey_to_coldkey = async_to_sync(engine._group_scores_by_coldkey)(scores)

        # Should group by coldkey
        assert "coldkey1" in coldkey_scores
        assert "coldkey2" in coldkey_scores
        assert "hotkey4" in coldkey_scores  # Individual hotkey without coldkey

        # Check totals
        assert coldkey_scores["coldkey1"] == 25.0  # 10 + 15
        assert coldkey_scores["coldkey2"] == 5.0  # 5
        assert coldkey_scores["hotkey4"] == 3.0  # Individual

        # Check mappings
        assert hotkey_to_coldkey["hotkey1"] == "coldkey1"
        assert hotkey_to_coldkey["hotkey2"] == "coldkey1"
        assert hotkey_to_coldkey["hotkey3"] == "coldkey2"
        assert hotkey_to_coldkey["hotkey4"] == "hotkey4"  # Maps to itself

    def test_get_split_distributions_batch(self):
        """Test batch fetching of split distributions."""
        engine = DefaultScoringEngine()

        # Create test splits
        split1 = MinerSplit.objects.create(
            coldkey="coldkey1", cycle_start=1000, validator_hotkey="validator_hotkey"
        )
        split2 = MinerSplit.objects.create(
            coldkey="coldkey2", cycle_start=1000, validator_hotkey="validator_hotkey"
        )

        # Create distributions
        MinerSplitDistribution.objects.create(split=split1, hotkey="hotkey1", percentage=0.6)
        MinerSplitDistribution.objects.create(split=split1, hotkey="hotkey2", percentage=0.4)
        MinerSplitDistribution.objects.create(split=split2, hotkey="hotkey3", percentage=1.0)

        # Test batch fetch
        coldkeys = ["coldkey1", "coldkey2", "coldkey3"]  # coldkey3 doesn't exist
        result = async_to_sync(engine._get_split_distributions_batch)(
            coldkeys, 1000, "validator_hotkey"
        )

        # Should return splits for existing coldkeys
        assert "coldkey1" in result
        assert "coldkey2" in result
        assert "coldkey3" not in result  # Doesn't exist

        # Check distributions
        assert result["coldkey1"].distributions == {"hotkey1": 0.6, "hotkey2": 0.4}
        assert result["coldkey2"].distributions == {"hotkey3": 1.0}

    def test_process_coldkey_split(self):
        """Test processing a single coldkey split."""
        engine = DefaultScoringEngine()

        # Create test data
        coldkey = "coldkey1"
        total_score = 20.0
        hotkey_to_coldkey = {"hotkey1": "coldkey1", "hotkey2": "coldkey1"}

        # Test with split
        current_split = MagicMock()
        current_split.distributions = {"hotkey1": 0.6, "hotkey2": 0.4}
        previous_split = MagicMock()
        previous_split.distributions = {"hotkey1": 0.5, "hotkey2": 0.5}  # Different

        result = engine._process_coldkey_split(
            coldkey, total_score, hotkey_to_coldkey, current_split, previous_split
        )

        # Should apply split distribution and bonus (since distributions changed)
        assert "hotkey1" in result
        assert "hotkey2" in result
        # With dancing bonus (1.1x multiplier), hotkey1 gets 20 * 0.6 * 1.1 = 13.2
        assert result["hotkey1"] == pytest.approx(13.2, rel=1e-10)  # 20 * 0.6 * 1.1 (dancing bonus)
        assert result["hotkey2"] == pytest.approx(8.8, rel=1e-10)  # 20 * 0.4 * 1.1 (dancing bonus)

        # Test without split
        result = engine._process_coldkey_split(coldkey, total_score, hotkey_to_coldkey, None, None)

        # Should distribute evenly
        assert "hotkey1" in result
        assert "hotkey2" in result
        assert result["hotkey1"] == 10.0  # 20 / 2
        assert result["hotkey2"] == 10.0  # 20 / 2
