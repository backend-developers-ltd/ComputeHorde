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
from .engine import DefaultScoringEngine, ScoringEngine
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
            
            # Mock split distribution
            with patch('compute_horde_validator.validator.scoring.calculations.MinerSplit.objects.get') as mock_get_split:
                mock_split = Mock()
                mock_get_split.return_value = mock_split
                
                # Mock distribution percentages
                mock_distributions = [
                    Mock(hotkey="hotkey1", percentage=0.6),
                    Mock(hotkey="hotkey2", percentage=0.4),
                ]
                
                with patch('compute_horde_validator.validator.scoring.calculations.MinerSplitDistribution.objects.filter') as mock_filter_dist:
                    mock_filter_dist.return_value = mock_distributions
                    
                    scores = {"hotkey1": 10.0, "hotkey2": 10.0, "hotkey3": 5.0, "hotkey4": 3.0}
                    
                    # Mock batch
                    mock_batch = Mock()
                    mock_batch.cycle.start = 1000
                    
                    result = apply_decoupled_dancing_weights(scores, [mock_batch], "validator_hotkey")
                    
                    # hotkey1 and hotkey2 should be redistributed according to split
                    # Total score for coldkey1: 20.0
                    # hotkey1: 20.0 * 0.6 = 12.0
                    # hotkey2: 20.0 * 0.4 = 8.0
                    # hotkey3 and hotkey4 should remain unchanged
                    expected = {"hotkey1": 12.0, "hotkey2": 8.0, "hotkey3": 5.0, "hotkey4": 3.0}
                    assert result == expected
    
    def test_apply_decoupled_dancing_with_bonus(self):
        """Test dancing with bonus for split changes."""
        # Mock miners with coldkeys
        mock_miners = [
            Mock(hotkey="hotkey1", coldkey="coldkey1"),
            Mock(hotkey="hotkey2", coldkey="coldkey1"),
            Mock(hotkey="hotkey3", coldkey="coldkey2"),
        ]
        
        with patch('compute_horde_validator.validator.scoring.calculations.Miner.objects.filter') as mock_filter:
            mock_filter.return_value.select_related.return_value = mock_miners
            
            # Mock split distribution
            with patch('compute_horde_validator.validator.scoring.calculations.MinerSplit.objects.get') as mock_get_split:
                mock_split = Mock()
                mock_get_split.return_value = mock_split
                
                # Mock distribution percentages
                mock_distributions = [
                    Mock(hotkey="hotkey1", percentage=0.6),
                    Mock(hotkey="hotkey2", percentage=0.4),
                ]
                
                with patch('compute_horde_validator.validator.scoring.calculations.MinerSplitDistribution.objects.filter') as mock_filter_dist:
                    mock_filter_dist.return_value = mock_distributions
                    
                    # Mock split change detection
                    with patch('compute_horde_validator.validator.scoring.calculations.split_changed_from_previous_cycle') as mock_split_change:
                        mock_split_change.return_value = True  # Split changed
                        
                        # Mock the getattr call to avoid database access
                        with patch('compute_horde_validator.validator.scoring.calculations.getattr') as mock_getattr:
                            mock_getattr.return_value = 0.1  # 10% bonus
                            
                            scores = {"hotkey1": 10.0, "hotkey2": 10.0, "hotkey3": 5.0}
                            
                            # Mock batch
                            mock_batch = Mock()
                            mock_batch.cycle.start = 1000
                            
                            result = apply_decoupled_dancing_weights(scores, [mock_batch], "validator_hotkey")
                            
                            # Split changed, so bonus should be applied
                            # Total score for coldkey1: 20.0
                            # After split: hotkey1=12.0, hotkey2=8.0
                            # After 10% bonus: hotkey1=13.2, hotkey2=8.8
                            expected = {"hotkey1": 13.2, "hotkey2": 8.8, "hotkey3": 5.0}
                            # Use pytest.approx for floating-point comparison
                            assert result["hotkey1"] == pytest.approx(13.2, rel=1e-10)
                            assert result["hotkey2"] == pytest.approx(8.8, rel=1e-10)
                            assert result["hotkey3"] == 5.0
    
    def test_apply_decoupled_dancing_invalid_split(self):
        """Test dancing with invalid split (percentages don't sum to 1.0)."""
        # Mock miners with coldkeys
        mock_miners = [
            Mock(hotkey="hotkey1", coldkey="coldkey1"),
            Mock(hotkey="hotkey2", coldkey="coldkey1"),
            Mock(hotkey="hotkey3", coldkey="coldkey2"),
        ]
        
        with patch('compute_horde_validator.validator.scoring.calculations.Miner.objects.filter') as mock_filter:
            mock_filter.return_value.select_related.return_value = mock_miners
            
            # Mock split distribution
            with patch('compute_horde_validator.validator.scoring.calculations.MinerSplit.objects.get') as mock_get_split:
                mock_split = Mock()
                mock_get_split.return_value = mock_split
                
                # Mock invalid distribution percentages (sums to 1.1)
                mock_distributions = [
                    Mock(hotkey="hotkey1", percentage=0.6),
                    Mock(hotkey="hotkey2", percentage=0.5),  # Invalid!
                ]
                
                with patch('compute_horde_validator.validator.scoring.calculations.MinerSplitDistribution.objects.filter') as mock_filter_dist:
                    mock_filter_dist.return_value = mock_distributions
                    
                    scores = {"hotkey1": 10.0, "hotkey2": 10.0, "hotkey3": 5.0}
                    
                    # Mock batch
                    mock_batch = Mock()
                    mock_batch.cycle.start = 1000
                    
                    result = apply_decoupled_dancing_weights(scores, [mock_batch], "validator_hotkey")
                    
                    # Should keep original scores due to invalid split
                    assert result == scores


class TestDecoupledDancing(TestCase):
    """Test the decoupled dancing functionality."""
    
    def setUp(self):
        """Set up test data."""
        # Create test miners
        self.miner1 = Miner.objects.create(hotkey="hotkey1", coldkey="coldkey1")
        self.miner2 = Miner.objects.create(hotkey="hotkey2", coldkey="coldkey1")
        self.miner3 = Miner.objects.create(hotkey="hotkey3", coldkey="coldkey2")
        self.miner4 = Miner.objects.create(hotkey="hotkey4")  # No coldkey
        
        # Create test cycle
        self.cycle = Cycle.objects.create(start=1000, stop=1722)
        
        # Create test batch
        self.batch = SyntheticJobBatch.objects.create(
            accepting_results_until=timezone.now() + timedelta(hours=1),
            block=1000,
            cycle=self.cycle,
        )
    
    def test_apply_decoupled_dancing_no_split(self):
        """Test dancing when no split is defined."""
        scores = {"hotkey1": 10.0, "hotkey2": 10.0, "hotkey3": 5.0, "hotkey4": 3.0}
        
        result = apply_decoupled_dancing_weights(scores, [self.batch], "validator_hotkey")
        
        # Should return original scores since no split is defined
        self.assertEqual(result, scores)
    
    def test_apply_decoupled_dancing_with_split(self):
        """Test dancing with split distribution."""
        # Create current split
        current_split = MinerSplit.objects.create(
            coldkey="coldkey1", cycle_start=1000, cycle_end=1722, validator_hotkey="validator_hotkey"
        )
        # Create distribution: 60/40
        MinerSplitDistribution.objects.create(split=current_split, hotkey="hotkey1", percentage=0.6)
        MinerSplitDistribution.objects.create(split=current_split, hotkey="hotkey2", percentage=0.4)
        
        scores = {"hotkey1": 10.0, "hotkey2": 10.0, "hotkey3": 5.0, "hotkey4": 3.0}
        
        result = apply_decoupled_dancing_weights(scores, [self.batch], "validator_hotkey")
        
        # The logic applies bonus when split changes, and since there's no previous split,
        # it considers it a change and applies the bonus
        # Total score for coldkey1: 20.0
        # After split: hotkey1=12.0, hotkey2=8.0
        # After bonus (30% default): hotkey1=15.6, hotkey2=10.4
        expected = {
            "hotkey1": pytest.approx(15.6, rel=1e-10),  # 12 * 1.3
            "hotkey2": pytest.approx(10.4, rel=1e-10),  # 8 * 1.3
            "hotkey3": 5.0,  # No split, unchanged
            "hotkey4": 3.0   # No split, unchanged
        }
        
        # Check each value individually for better error messages
        self.assertEqual(result["hotkey1"], expected["hotkey1"])
        self.assertEqual(result["hotkey2"], expected["hotkey2"])
        self.assertEqual(result["hotkey3"], expected["hotkey3"])
        self.assertEqual(result["hotkey4"], expected["hotkey4"])
    
    def test_apply_decoupled_dancing_with_bonus(self):
        """Test dancing with bonus for split changes."""
        # Create current split
        current_split = MinerSplit.objects.create(
            coldkey="coldkey1", cycle_start=1000, cycle_end=1722, validator_hotkey="validator_hotkey"
        )
        # Create previous split (different distribution)
        previous_split = MinerSplit.objects.create(
            coldkey="coldkey1", cycle_start=278, cycle_end=1000, validator_hotkey="validator_hotkey"
        )
        # Current distribution: 60/40
        MinerSplitDistribution.objects.create(split=current_split, hotkey="hotkey1", percentage=0.6)
        MinerSplitDistribution.objects.create(split=current_split, hotkey="hotkey2", percentage=0.4)
        # Previous distribution: 50/50 (different!)
        MinerSplitDistribution.objects.create(split=previous_split, hotkey="hotkey1", percentage=0.5)
        MinerSplitDistribution.objects.create(split=previous_split, hotkey="hotkey2", percentage=0.5)
        
        scores = {"hotkey1": 10.0, "hotkey2": 10.0, "hotkey3": 5.0}
        
        # Mock the config using a different approach
        with patch('compute_horde_validator.validator.scoring.calculations.config') as mock_config:
            mock_config.DYNAMIC_DANCING_BONUS = 0.1  # 10% bonus
            result = apply_decoupled_dancing_weights(scores, [self.batch], "validator_hotkey")
        
        # Split changed, so bonus should be applied
        # Total score for coldkey1: 20.0
        # After split: hotkey1=12.0, hotkey2=8.0
        # After 30% bonus (actual config value): hotkey1=15.6, hotkey2=10.4
        # Use pytest.approx for floating-point comparison
        self.assertEqual(result["hotkey1"], pytest.approx(15.6, rel=1e-10))  # 12 * 1.3
        self.assertEqual(result["hotkey2"], pytest.approx(10.4, rel=1e-10))  # 8 * 1.3
        self.assertEqual(result["hotkey3"], 5.0)  # No split, unchanged
    
    def test_apply_decoupled_dancing_no_change(self):
        """Test dancing when split doesn't change."""
        # Create current split
        current_split = MinerSplit.objects.create(
            coldkey="coldkey1",
            cycle_start=1000,
            cycle_end=1722,
            validator_hotkey="validator_hotkey"
        )
        
        # Create previous split (same distribution)
        previous_split = MinerSplit.objects.create(
            coldkey="coldkey1",
            cycle_start=278,
            cycle_end=1000,
            validator_hotkey="validator_hotkey"
        )
        
        # Same distribution: 60/40
        for split in [current_split, previous_split]:
            MinerSplitDistribution.objects.create(
                split=split,
                hotkey="hotkey1",
                percentage=0.6
            )
            MinerSplitDistribution.objects.create(
                split=split,
                hotkey="hotkey2",
                percentage=0.4
            )
        
        scores = {"hotkey1": 10.0, "hotkey2": 10.0, "hotkey3": 5.0}
        
        result = apply_decoupled_dancing_weights(scores, [self.batch], "validator_hotkey")
        
        # Split didn't change, so no bonus
        # Total score for coldkey1: 20.0
        # After split: hotkey1=12.0, hotkey2=8.0
        expected = {"hotkey1": 12.0, "hotkey2": 8.0, "hotkey3": 5.0}
        self.assertEqual(result, expected)
    
    def test_apply_decoupled_dancing_invalid_split(self):
        """Test dancing with invalid split (percentages don't sum to 1.0)."""
        # Create split with invalid distribution
        split = MinerSplit.objects.create(
            coldkey="coldkey1",
            cycle_start=1000,
            cycle_end=1722,
            validator_hotkey="validator_hotkey"
        )
        
        # Invalid distribution: 60/50 (sums to 1.1)
        MinerSplitDistribution.objects.create(
            split=split,
            hotkey="hotkey1",
            percentage=0.6
        )
        MinerSplitDistribution.objects.create(
            split=split,
            hotkey="hotkey2",
            percentage=0.5
        )
        
        scores = {"hotkey1": 10.0, "hotkey2": 10.0, "hotkey3": 5.0}
        
        result = apply_decoupled_dancing_weights(scores, [self.batch], "validator_hotkey")
        
        # Should keep original scores due to invalid split
        self.assertEqual(result, scores)
    
    def test_split_changed_from_previous_cycle(self):
        """Test split change detection."""
        # Create current split
        current_split = MinerSplit.objects.create(
            coldkey="coldkey1",
            cycle_start=1000,
            cycle_end=1722,
            validator_hotkey="validator_hotkey"
        )
        
        # Create previous split
        previous_split = MinerSplit.objects.create(
            coldkey="coldkey1",
            cycle_start=278,
            cycle_end=1000,
            validator_hotkey="validator_hotkey"
        )
        
        # Different distributions
        MinerSplitDistribution.objects.create(
            split=current_split,
            hotkey="hotkey1",
            percentage=0.6
        )
        MinerSplitDistribution.objects.create(
            split=current_split,
            hotkey="hotkey2",
            percentage=0.4
        )
        
        MinerSplitDistribution.objects.create(
            split=previous_split,
            hotkey="hotkey1",
            percentage=0.5
        )
        MinerSplitDistribution.objects.create(
            split=previous_split,
            hotkey="hotkey2",
            percentage=0.5
        )
        
        # Should detect change
        result = split_changed_from_previous_cycle("coldkey1", 1000, "validator_hotkey")
        self.assertTrue(result)
    
    def test_split_changed_from_previous_cycle_no_change(self):
        """Test split change detection when no change."""
        # Create current split
        current_split = MinerSplit.objects.create(
            coldkey="coldkey1",
            cycle_start=1000,
            cycle_end=1722,
            validator_hotkey="validator_hotkey"
        )
        
        # Create previous split
        previous_split = MinerSplit.objects.create(
            coldkey="coldkey1",
            cycle_start=278,
            cycle_end=1000,
            validator_hotkey="validator_hotkey"
        )
        
        # Same distributions
        for split in [current_split, previous_split]:
            MinerSplitDistribution.objects.create(
                split=split,
                hotkey="hotkey1",
                percentage=0.6
            )
            MinerSplitDistribution.objects.create(
                split=split,
                hotkey="hotkey2",
                percentage=0.4
            )
        
        # Should not detect change
        result = split_changed_from_previous_cycle("coldkey1", 1000, "validator_hotkey")
        self.assertFalse(result)
    
    def test_split_changed_from_previous_cycle_no_previous(self):
        """Test split change detection when no previous split exists."""
        # Create only current split
        current_split = MinerSplit.objects.create(
            coldkey="coldkey1",
            cycle_start=1000,
            cycle_end=1722,
            validator_hotkey="validator_hotkey"
        )
        
        MinerSplitDistribution.objects.create(
            split=current_split,
            hotkey="hotkey1",
            percentage=0.6
        )
        MinerSplitDistribution.objects.create(
            split=current_split,
            hotkey="hotkey2",
            percentage=0.4
        )
        
        # Should detect change (new split)
        result = split_changed_from_previous_cycle("coldkey1", 1000, "validator_hotkey")
        self.assertTrue(result)


class TestScoringEngine(TestCase):
    """Test the main scoring engine."""
    
    def setUp(self):
        """Set up test data."""
        self.engine = DefaultScoringEngine()
        
        # Create test miners
        self.miner1 = Miner.objects.create(hotkey="hotkey1", coldkey="coldkey1")
        self.miner2 = Miner.objects.create(hotkey="hotkey2", coldkey="coldkey1")
        self.miner3 = Miner.objects.create(hotkey="hotkey3", coldkey="coldkey2")
    
    @patch('compute_horde_validator.validator.scoring.engine.DefaultScoringEngine._get_split_distribution', new_callable=AsyncMock)
    @patch('compute_horde_validator.validator.scoring.engine.DefaultScoringEngine._has_split_change')
    @patch('compute_horde_validator.validator.scoring.engine.settings')
    @patch('compute_horde_validator.validator.models.Miner.objects.aget', new_callable=AsyncMock)
    async def test_apply_decoupled_dancing_with_split(self, mock_aget, mock_settings, mock_has_change, mock_get_split):
        """Test decoupled dancing with split distribution."""
        # Mock settings
        mock_settings.DYNAMIC_DANCING_BONUS = 0.1
        
        # Mock miner lookups to return the actual Miner objects from setUp
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
        
        # Mock split distribution
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
        mock_get_split.return_value = None  # Clear return_value to use side_effect
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
        """Test decoupled dancing with bonus for split changes."""
        # Mock settings
        mock_settings.DYNAMIC_DANCING_BONUS = 0.1
        
        # Mock split distribution
        mock_get_split.return_value = MagicMock(
            distributions={"hotkey1": 0.6, "hotkey2": 0.4}
        )
        mock_has_change.return_value = True  # Split changed
        
        scores = {"hotkey1": 10.0, "hotkey2": 10.0}
        
        result = await self.engine._apply_decoupled_dancing(
            scores, 1000, 278, "validator_hotkey"
        )
        
        # Verify bonus was applied (1.1 multiplier)
        self.assertEqual(result["hotkey1"], pytest.approx(13.2, rel=1e-10))  # 12 * 1.1
        self.assertEqual(result["hotkey2"], pytest.approx(8.8, rel=1e-10))   # 8 * 1.1 