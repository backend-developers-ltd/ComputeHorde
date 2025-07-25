import asyncio
from datetime import timedelta
from unittest.mock import AsyncMock, MagicMock, patch
from typing import Any

import pytest
from asgiref.sync import async_to_sync, sync_to_async
from django.test import TestCase
from django.utils import timezone

from compute_horde.subtensor import get_cycle_containing_block
from compute_horde_validator.validator.models import (
    Cycle,
    Miner,
    OrganicJob,
    SyntheticJob,
    SyntheticJobBatch,
)
from compute_horde_validator.validator.scoring_new.calculations import (
    calculate_organic_scores,
    calculate_synthetic_scores,
    combine_scores,
)
from compute_horde_validator.validator.scoring_new.engine import DefaultScoringEngine
from compute_horde_validator.validator.scoring_new.exceptions import (
    InvalidSplitPercentageError,
    ScoringConfigurationError,
    SplitDistributionError,
    BittensorConnectionError,
)
from compute_horde_validator.validator.scoring_new.factory import (
    create_scoring_engine,
    get_available_engine_types,
)
from compute_horde_validator.validator.scoring_new.interface import ScoringEngine
from compute_horde_validator.validator.scoring_new.models import (
    MinerSplit,
    MinerSplitDistribution,
    SplitInfo,
)


class MockScoringEngine(ScoringEngine):
    """Mock implementation of ScoringEngine."""

    def __init__(self, return_scores: dict[str, float]):
        self.return_scores = return_scores

    async def calculate_scores_for_cycles(
        self, current_cycle_start: int, previous_cycle_start: int, validator_hotkey: str
    ) -> dict[str, float]:
        """Return predefined scores for testing."""
        return self.return_scores


class TestScoringEngineInterface(TestCase):
    def test_mock_with_patch(self):
        with patch.object(DefaultScoringEngine, "calculate_scores_for_cycles") as mock_calculate:
            mock_calculate.return_value = {"hotkey1": 10.0, "hotkey2": 5.0}

            engine = DefaultScoringEngine()

            result = asyncio.run(
                engine.calculate_scores_for_cycles(
                    current_cycle_start=1000,
                    previous_cycle_start=278,
                    validator_hotkey="validator_hotkey",
                )
            )

            mock_calculate.assert_called_once_with(
                current_cycle_start=1000,
                previous_cycle_start=278,
                validator_hotkey="validator_hotkey",
            )
            self.assertEqual(result, {"hotkey1": 10.0, "hotkey2": 5.0})


@pytest.mark.django_db
class TestScoringEngineIntegration(TestCase):
    def setUp(self):
        super().setUp()

        # Create miners
        self.miner1 = Miner.objects.create(hotkey="hotkey1", coldkey="coldkey1")
        self.miner2 = Miner.objects.create(hotkey="hotkey2", coldkey="coldkey2")
        self.miner3 = Miner.objects.create(hotkey="hotkey3")  # No coldkey

        self.current_cycle = Cycle.objects.create(start=1000, stop=1722)
        self.previous_cycle = Cycle.objects.create(start=278, stop=1000)

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

    @patch("compute_horde_validator.validator.scoring_new.engine.query_miner_split_distributions")
    def test_calculate_scores_for_cycles_basic(self, mock_query_splits):
        """Test basic scoring for two cycles."""
        # Mock split querying to avoid network calls
        mock_query_splits.return_value = {}
        
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
            current_cycle_start=1000,
            previous_cycle_start=278,
            validator_hotkey="validator_hotkey",
        )

        # Validate the exact scores we discovered
        assert scores["hotkey1"] == 89.1
        assert scores["hotkey2"] == 49.5
        assert scores["hotkey3"] == 59.4


    @patch("compute_horde_validator.validator.scoring_new.engine.query_miner_split_distributions")
    def test_calculate_scores_for_cycles_no_jobs(self, mock_query_splits):
        """Test scoring when there are no jobs."""
        mock_query_splits.return_value = {}
        
        engine = DefaultScoringEngine()

        scores = async_to_sync(engine.calculate_scores_for_cycles)(
            current_cycle_start=1000,
            previous_cycle_start=278,
            validator_hotkey="validator_hotkey",
        )

        assert scores == {}

    @patch("compute_horde_validator.validator.scoring_new.engine.query_miner_split_distributions")
    def test_calculate_scores_for_cycles_with_splits(self, mock_query_splits):
        """Test scoring with split distributions."""
        mock_query_splits.return_value = {}
        
        engine = DefaultScoringEngine()

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

        split = MinerSplit.objects.create(
            coldkey="coldkey1", cycle_start=1000, validator_hotkey="validator_hotkey"
        )
        MinerSplitDistribution.objects.create(split=split, hotkey="hotkey1", percentage=0.6)
        # Add another hotkey to the split (even though it doesn't exist in our test)
        MinerSplitDistribution.objects.create(split=split, hotkey="hotkey1_alt", percentage=0.4)

        scores = async_to_sync(engine.calculate_scores_for_cycles)(
            current_cycle_start=1000,
            previous_cycle_start=278,
            validator_hotkey="validator_hotkey",
        )

        assert "hotkey1" in scores
        assert scores["hotkey1"] > 0

    @patch("compute_horde_validator.validator.scoring_new.engine.query_miner_split_distributions")
    def test_calculate_scores_for_cycles_with_dancing_bonus(self, mock_query_splits):
        """Test scoring with dancing bonus when splits change."""
        mock_query_splits.return_value = {}
        
        engine = DefaultScoringEngine()

        OrganicJob.objects.create(
            miner=self.miner1,
            block=1000,
            status=OrganicJob.Status.COMPLETED,
            cheated=False,
            miner_address_ip_version=4,
            miner_port=8080,
        )

        current_split = MinerSplit.objects.create(
            coldkey="coldkey1", cycle_start=1000, validator_hotkey="validator_hotkey"
        )
        MinerSplitDistribution.objects.create(split=current_split, hotkey="hotkey1", percentage=0.6)

        previous_split = MinerSplit.objects.create(
            coldkey="coldkey1", cycle_start=278, validator_hotkey="validator_hotkey"
        )
        MinerSplitDistribution.objects.create(
            split=previous_split,
            hotkey="hotkey1",
            percentage=0.5,
        )

        scores = async_to_sync(engine.calculate_scores_for_cycles)(
            current_cycle_start=1000,
            previous_cycle_start=278,
            validator_hotkey="validator_hotkey",
        )

        assert "hotkey1" in scores
        assert scores["hotkey1"] > 0
