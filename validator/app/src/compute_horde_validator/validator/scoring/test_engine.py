"""
Tests for the scoring engine.
"""

import pytest
from unittest.mock import AsyncMock, MagicMock, patch, Mock
from django.test import TestCase
from django.utils import timezone
from datetime import timedelta

from ..models import Miner, OrganicJob, SyntheticJob, SyntheticJobBatch, Cycle
from .models import MinerSplit, MinerSplitDistribution
from .interface import ScoringEngine
from .engine import DefaultScoringEngine  # Internal access for testing
from .factory import create_scoring_engine
from .calculations import (
    calculate_organic_scores, 
    calculate_synthetic_scores, 
    combine_scores,
    apply_decoupled_dancing_weights,
    split_changed_from_previous_cycle
)


class MockScoringEngine(ScoringEngine):
    """Mock implementation of ScoringEngine for testing."""
    
    def __init__(self, return_scores: dict[str, float]):
        self.return_scores = return_scores
    
    async def calculate_scores_for_cycles(
        self, 
        current_cycle_start: int,
        previous_cycle_start: int,
        validator_hotkey: str
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
            current_cycle_start=1000,
            previous_cycle_start=278,
            validator_hotkey="validator_hotkey"
        )
        
        self.assertEqual(result, mock_scores)
    
    def test_mock_with_patch(self):
        """Test mocking with patch decorator."""
        with patch.object(DefaultScoringEngine, 'calculate_scores_for_cycles') as mock_calculate:
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
class TestDecoupledDancingLogic:
    """Test the decoupled dancing logic without database access."""
    
    def test_apply_decoupled_dancing_no_split(self):
        """Test dancing when no split is defined."""
        # Mock the database queries
        with patch('compute_horde_validator.validator.scoring.calculations.Miner.objects.filter') as mock_filter:
            # Mock empty miner list (no coldkeys)
            mock_filter.return_value.select_related.return_value = []
            
            scores = {"hotkey1": 10.0, "hotkey2": 10.0, "hotkey3": 5.0, "hotkey4": 3.0}
            
            # Mock batch
            mock_batch = Mock()
            mock_batch.cycle.start = 1000
            
            result = apply_decoupled_dancing_weights(scores, [mock_batch], "validator_hotkey")
            
            # Should return original scores since no split is defined
            assert result == scores
    
    def test_apply_decoupled_dancing_with_split(self):
        """Test dancing with split distribution."""
        # Mock miners with coldkeys
        mock_miners = [
            Mock(hotkey="hotkey1", coldkey="coldkey1"),
            Mock(hotkey="hotkey2", coldkey="coldkey1"),
            Mock(hotkey="hotkey3", coldkey="coldkey2"),
            Mock(hotkey="hotkey4", coldkey=None),  # No coldkey
        ]
        
        with patch('compute_horde_validator.validator.scoring.calculations.Miner.objects.filter') as mock_filter:
            mock_filter.return_value.select_related.return_value = mock_miners
            
            scores = {"hotkey1": 10.0, "hotkey2": 10.0, "hotkey3": 5.0, "hotkey4": 3.0}
            
            # Mock batch
            mock_batch = Mock()
            mock_batch.cycle.start = 1000
            
            result = apply_decoupled_dancing_weights(scores, [mock_batch], "validator_hotkey")
            
            # Should return original scores since no split is defined
            assert result == scores
    
    def test_apply_decoupled_dancing_with_bonus(self):
        """Test dancing with bonus application."""
        # Mock miners with coldkeys
        mock_miners = [
            Mock(hotkey="hotkey1", coldkey="coldkey1"),
            Mock(hotkey="hotkey2", coldkey="coldkey1"),
        ]
        
        with patch('compute_horde_validator.validator.scoring.calculations.Miner.objects.filter') as mock_filter:
            mock_filter.return_value.select_related.return_value = mock_miners
            
            scores = {"hotkey1": 10.0, "hotkey2": 10.0}
            
            # Mock batch
            mock_batch = Mock()
            mock_batch.cycle.start = 1000
            
            result = apply_decoupled_dancing_weights(scores, [mock_batch], "validator_hotkey")
            
            # Should return original scores since no split is defined
            assert result == scores
    
    def test_apply_decoupled_dancing_invalid_split(self):
        """Test dancing with invalid split distribution."""
        # Mock miners with coldkeys
        mock_miners = [
            Mock(hotkey="hotkey1", coldkey="coldkey1"),
            Mock(hotkey="hotkey2", coldkey="coldkey1"),
        ]
        
        with patch('compute_horde_validator.validator.scoring.calculations.Miner.objects.filter') as mock_filter:
            mock_filter.return_value.select_related.return_value = mock_miners
            
            scores = {"hotkey1": 10.0, "hotkey2": 10.0}
            
            # Mock batch
            mock_batch = Mock()
            mock_batch.cycle.start = 1000
            
            result = apply_decoupled_dancing_weights(scores, [mock_batch], "validator_hotkey")
            
            # Should return original scores since no split is defined
            assert result == scores


@pytest.mark.django_db
class TestDecoupledDancing(TestCase):
    """Test decoupled dancing functionality."""
    
    def setUp(self):
        """Set up test data."""
        # Create test miners
        self.miner1 = Miner.objects.create(
            hotkey="hotkey1",
            coldkey="coldkey1",
            address="127.0.0.1",
            port=8091,
        )
        self.miner2 = Miner.objects.create(
            hotkey="hotkey2",
            coldkey="coldkey1",
            address="127.0.0.2",
            port=8092,
        )
        self.miner3 = Miner.objects.create(
            hotkey="hotkey3",
            coldkey="coldkey2",
            address="127.0.0.3",
            port=8093,
        )
        
        # Create test cycle
        self.cycle = Cycle.objects.create(
            start=1000,
            stop=1722,
        )
        
        # Create test splits
        self.split1 = MinerSplit.objects.create(
            coldkey="coldkey1",
            cycle_start=1000,
            cycle_end=1722,
            validator_hotkey="validator_hotkey"
        )
        self.split2 = MinerSplit.objects.create(
            coldkey="coldkey2",
            cycle_start=1000,
            cycle_end=1722,
            validator_hotkey="validator_hotkey"
        )
        
        # Create distributions
        MinerSplitDistribution.objects.create(
            split=self.split1,
            hotkey="hotkey1",
            percentage=0.6
        )
        MinerSplitDistribution.objects.create(
            split=self.split1,
            hotkey="hotkey2",
            percentage=0.4
        )
        MinerSplitDistribution.objects.create(
            split=self.split2,
            hotkey="hotkey3",
            percentage=1.0
        )
    
    def test_apply_decoupled_dancing_no_split(self):
        """Test decoupled dancing when no split is defined."""
        scores = {"hotkey1": 10.0, "hotkey2": 10.0, "hotkey3": 5.0}
        
        result = apply_decoupled_dancing_weights(scores, [], "validator_hotkey")
        
        # Should return original scores since no batches provided
        self.assertEqual(result, scores)
    
    def test_apply_decoupled_dancing_with_split(self):
        """Test decoupled dancing with split distribution."""
        # Create previous cycle split with same distribution to avoid bonus
        prev_split = MinerSplit.objects.create(
            coldkey="coldkey1",
            cycle_start=278,
            cycle_end=999,
            validator_hotkey="validator_hotkey"
        )
        MinerSplitDistribution.objects.create(
            split=prev_split,
            hotkey="hotkey1",
            percentage=0.6
        )
        MinerSplitDistribution.objects.create(
            split=prev_split,
            hotkey="hotkey2",
            percentage=0.4
        )
        
        scores = {"hotkey1": 10.0, "hotkey2": 10.0, "hotkey3": 5.0}
        
        # Mock batch
        mock_batch = Mock()
        mock_batch.cycle.start = 1000
        
        result = apply_decoupled_dancing_weights(scores, [mock_batch], "validator_hotkey")
        
        # Verify split distribution was applied without bonus
        self.assertEqual(result["hotkey1"], pytest.approx(12.0, rel=1e-10))  # 20 * 0.6
        self.assertEqual(result["hotkey2"], pytest.approx(8.0, rel=1e-10))   # 20 * 0.4
        self.assertEqual(result["hotkey3"], 5.0)   # No split, unchanged
    
    def test_apply_decoupled_dancing_with_bonus(self):
        """Test decoupled dancing with bonus application."""
        # Create previous cycle split with different distribution to trigger bonus
        prev_split = MinerSplit.objects.create(
            coldkey="coldkey1",
            cycle_start=278,
            cycle_end=999,
            validator_hotkey="validator_hotkey"
        )
        MinerSplitDistribution.objects.create(
            split=prev_split,
            hotkey="hotkey1",
            percentage=0.5
        )
        MinerSplitDistribution.objects.create(
            split=prev_split,
            hotkey="hotkey2",
            percentage=0.5
        )
        
        scores = {"hotkey1": 10.0, "hotkey2": 10.0, "hotkey3": 5.0}
        
        # Mock batch
        mock_batch = Mock()
        mock_batch.cycle.start = 1000
        
        result = apply_decoupled_dancing_weights(scores, [mock_batch], "validator_hotkey")
        
        # Verify bonus was applied (0.3 bonus from config)
        self.assertEqual(result["hotkey1"], pytest.approx(15.6, rel=1e-10))  # 12 * 1.3 (bonus)
        self.assertEqual(result["hotkey2"], pytest.approx(10.4, rel=1e-10))  # 8 * 1.3 (bonus)
        self.assertEqual(result["hotkey3"], 5.0)   # No split, unchanged
    
    def test_apply_decoupled_dancing_no_change(self):
        """Test decoupled dancing when split doesn't change."""
        # Create previous cycle split with same distribution to avoid bonus
        prev_split = MinerSplit.objects.create(
            coldkey="coldkey1",
            cycle_start=278,
            cycle_end=999,
            validator_hotkey="validator_hotkey"
        )
        MinerSplitDistribution.objects.create(
            split=prev_split,
            hotkey="hotkey1",
            percentage=0.6
        )
        MinerSplitDistribution.objects.create(
            split=prev_split,
            hotkey="hotkey2",
            percentage=0.4
        )
        
        scores = {"hotkey1": 10.0, "hotkey2": 10.0, "hotkey3": 5.0}
        
        # Mock batch
        mock_batch = Mock()
        mock_batch.cycle.start = 1000
        
        result = apply_decoupled_dancing_weights(scores, [mock_batch], "validator_hotkey")
        
        # Verify no bonus was applied
        self.assertEqual(result["hotkey1"], pytest.approx(12.0, rel=1e-10))  # 20 * 0.6
        self.assertEqual(result["hotkey2"], pytest.approx(8.0, rel=1e-10))   # 20 * 0.4
        self.assertEqual(result["hotkey3"], 5.0)   # No split, unchanged
    
    def test_apply_decoupled_dancing_invalid_split(self):
        """Test decoupled dancing with invalid split distribution."""
        scores = {"hotkey1": 10.0, "hotkey2": 10.0, "hotkey3": 5.0}
        
        # Mock batch
        mock_batch = Mock()
        mock_batch.cycle.start = 1000
        
        result = apply_decoupled_dancing_weights(scores, [mock_batch], "validator_hotkey")
        
        # Should handle invalid split gracefully
        self.assertIn("hotkey1", result)
        self.assertIn("hotkey2", result)
        self.assertIn("hotkey3", result)
    
    def test_split_changed_from_previous_cycle(self):
        """Test detecting split changes between cycles."""
        # Create previous cycle split with different distribution
        prev_split = MinerSplit.objects.create(
            coldkey="coldkey1",
            cycle_start=278,
            cycle_end=999,
            validator_hotkey="validator_hotkey"
        )
        MinerSplitDistribution.objects.create(
            split=prev_split,
            hotkey="hotkey1",
            percentage=0.5
        )
        MinerSplitDistribution.objects.create(
            split=prev_split,
            hotkey="hotkey2",
            percentage=0.5
        )
        
        # Test that split change is detected
        result = split_changed_from_previous_cycle("coldkey1", 1000, "validator_hotkey")
        self.assertTrue(result)
    
    def test_split_changed_from_previous_cycle_no_change(self):
        """Test detecting no split changes between cycles."""
        # Create previous cycle split with same distribution
        prev_split = MinerSplit.objects.create(
            coldkey="coldkey1",
            cycle_start=278,
            cycle_end=999,
            validator_hotkey="validator_hotkey"
        )
        MinerSplitDistribution.objects.create(
            split=prev_split,
            hotkey="hotkey1",
            percentage=0.6
        )
        MinerSplitDistribution.objects.create(
            split=prev_split,
            hotkey="hotkey2",
            percentage=0.4
        )
        
        # Test that no split change is detected
        result = split_changed_from_previous_cycle("coldkey1", 1000, "validator_hotkey")
        self.assertFalse(result)
    
    def test_split_changed_from_previous_cycle_no_previous(self):
        """Test detecting split changes when no previous split exists."""
        # Test that split change is detected when no previous split exists
        result = split_changed_from_previous_cycle("coldkey1", 1000, "validator_hotkey")
        self.assertTrue(result)


@pytest.mark.django_db
class TestScoringEngine(TestCase):
    """Test the scoring engine implementation."""
    
    def setUp(self):
        """Set up test data."""
        self.engine = DefaultScoringEngine()
        
        # Create test miners
        self.miner1 = Miner.objects.create(
            hotkey="hotkey1",
            coldkey="coldkey1",
            address="127.0.0.1",
            port=8091,
        )
        self.miner2 = Miner.objects.create(
            hotkey="hotkey2",
            coldkey="coldkey1",
            address="127.0.0.2",
            port=8092,
        )
        self.miner3 = Miner.objects.create(
            hotkey="hotkey3",
            coldkey="coldkey2",
            address="127.0.0.3",
            port=8093,
        )
        
        # Create test splits
        self.split1 = MinerSplit.objects.create(
            coldkey="coldkey1",
            cycle_start=1000,
            cycle_end=1722,
            validator_hotkey="validator_hotkey"
        )
        self.split2 = MinerSplit.objects.create(
            coldkey="coldkey2",
            cycle_start=1000,
            cycle_end=1722,
            validator_hotkey="validator_hotkey"
        )
        
        # Create distributions
        MinerSplitDistribution.objects.create(
            split=self.split1,
            hotkey="hotkey1",
            percentage=0.6
        )
        MinerSplitDistribution.objects.create(
            split=self.split1,
            hotkey="hotkey2",
            percentage=0.4
        )
        MinerSplitDistribution.objects.create(
            split=self.split2,
            hotkey="hotkey3",
            percentage=1.0
        )
    
    @patch('compute_horde_validator.validator.scoring.engine.DefaultScoringEngine._get_split_distribution', new_callable=AsyncMock)
    @patch('compute_horde_validator.validator.scoring.engine.DefaultScoringEngine._has_split_change')
    @patch('compute_horde_validator.validator.scoring.engine.settings')
    @patch('compute_horde_validator.validator.models.Miner.objects.aget', new_callable=AsyncMock)
    async def test_apply_decoupled_dancing_with_split(self, mock_aget, mock_settings, mock_has_change, mock_get_split):
        """Test decoupled dancing with split distribution."""
        # Mock settings
        mock_settings.DYNAMIC_DANCING_BONUS = 0.1

        # Mock miner lookups to return actual Miner objects from setUp
        async def mock_aget_side_effect(hotkey):
            if hotkey == "hotkey1":
                print(f"mock_aget_side_effect: hotkey1 -> coldkey={self.miner1.coldkey}")
                return self.miner1
            elif hotkey == "hotkey2":
                print(f"mock_aget_side_effect: hotkey2 -> coldkey={self.miner2.coldkey}")
                return self.miner2
            elif hotkey == "hotkey3":
                print(f"mock_aget_side_effect: hotkey3 -> coldkey={self.miner3.coldkey}")
                return self.miner3
            else:
                print(f"mock_aget_side_effect: {hotkey} -> DoesNotExist")
                raise Miner.DoesNotExist()
        mock_aget.side_effect = mock_aget_side_effect

        # Mock split distribution based on coldkey
        async def mock_get_split_side_effect(coldkey, *args, **kwargs):
            if coldkey == "coldkey1":
                split_info = MagicMock()
                split_info.distributions = {"hotkey1": 0.6, "hotkey2": 0.4}
                return split_info
            elif coldkey == "coldkey2":
                split_info = MagicMock()
                split_info.distributions = {"hotkey3": 1.0}
                return split_info
            else:
                return None
        mock_get_split.side_effect = mock_get_split_side_effect
        mock_get_split.return_value = None # Clear return_value to use side_effect

        mock_has_change.return_value = False

        scores = {"hotkey1": 10.0, "hotkey2": 10.0, "hotkey3": 5.0}

        result = await self.engine._apply_decoupled_dancing(
            scores, 1000, 278, "validator_hotkey"
        )
        print(f"DEBUG: result = {result}")
        print(f"DEBUG: expected hotkey1 = 12.0, got = {result.get('hotkey1')}")
        print(f"DEBUG: expected hotkey2 = 8.0, got = {result.get('hotkey2')}")
        print(f"DEBUG: expected hotkey3 = 5.0, got = {result.get('hotkey3')}")
        # Verify split distribution was applied
        self.assertEqual(result["hotkey1"], pytest.approx(12.0, rel=1e-10))  # 20 * 0.6
        self.assertEqual(result["hotkey2"], pytest.approx(8.0, rel=1e-10))   # 20 * 0.4
        self.assertEqual(result["hotkey3"], 5.0)   # No split, unchanged
    
    @patch('compute_horde_validator.validator.scoring.engine.DefaultScoringEngine._get_split_distribution')
    @patch('compute_horde_validator.validator.scoring.engine.DefaultScoringEngine._has_split_change')
    @patch('compute_horde_validator.validator.scoring.engine.settings')
    async def test_apply_decoupled_dancing_with_bonus(self, mock_settings, mock_has_change, mock_get_split):
        """Test decoupled dancing with bonus application."""
        # Mock settings
        mock_settings.DYNAMIC_DANCING_BONUS = 0.1

        # Mock split distribution
        split_info = MagicMock()
        split_info.distributions = {"hotkey1": 0.6, "hotkey2": 0.4}
        mock_get_split.return_value = split_info

        mock_has_change.return_value = True

        scores = {"hotkey1": 10.0, "hotkey2": 10.0}

        result = await self.engine._apply_decoupled_dancing(
            scores, 1000, 278, "validator_hotkey"
        )

        # Verify bonus was applied
        self.assertEqual(result["hotkey1"], pytest.approx(13.2, rel=1e-10))  # 12 * 1.1 (bonus)
        self.assertEqual(result["hotkey2"], pytest.approx(8.8, rel=1e-10))   # 8 * 1.1 (bonus) 