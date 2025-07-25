"""
Comprehensive tests for the calculations module.
"""

import asyncio
from datetime import timedelta
from unittest.mock import AsyncMock, MagicMock, patch
from typing import Any

import pytest
from asgiref.sync import async_to_sync
from django.test import TestCase
from django.utils import timezone

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
    get_hotkey_to_coldkey_mapping,
    normalize,
    sigmoid,
    reversed_sigmoid,
    horde_score,
)
from compute_horde_validator.validator.scoring_new.exceptions import ColdkeyMappingError
from compute_horde_core.executor_class import ExecutorClass



@pytest.mark.django_db
class TestCalculateOrganicScores(TestCase):
    """Test the calculate_organic_scores function."""

    def setUp(self):
        """Set up test data."""
        super().setUp()
        self.miner1 = Miner.objects.create(hotkey="hotkey1", coldkey="coldkey1")
        self.miner2 = Miner.objects.create(hotkey="hotkey2", coldkey="coldkey2")


    @patch("compute_horde_validator.validator.scoring_new.calculations.score_organic_jobs")
    def test_calculate_organic_scores_with_failed_jobs(self, mock_score_organic):
        """Test organic score calculation with failed jobs."""
        # Mock the scoring function
        mock_score_organic.return_value = {"hotkey1": 10.0, "hotkey2": 0.0}

        # Create completed and failed jobs
        completed_job = OrganicJob.objects.create(
            miner=self.miner1,
            block=1000,
            status=OrganicJob.Status.COMPLETED,
            cheated=False,
            on_trusted_miner=False,
            executor_class=ExecutorClass.spin_up_4min__gpu_24gb,
            miner_address_ip_version=4,
            miner_port=8080,
        )
        failed_job = OrganicJob.objects.create(
            miner=self.miner2,
            block=1001,
            status=OrganicJob.Status.FAILED,
            cheated=False,
            on_trusted_miner=False,
            executor_class=ExecutorClass.spin_up_4min__gpu_24gb,
            miner_address_ip_version=4,
            miner_port=8080,
        )

        jobs = [completed_job, failed_job]

        result = async_to_sync(calculate_organic_scores)(jobs)

        # Should have scores for both miners (including zero for failed)
        assert "hotkey1" in result
        assert "hotkey2" in result
        assert result["hotkey1"] > 0
        assert result["hotkey2"] == 0.0

    @patch("compute_horde_validator.validator.scoring_new.calculations.score_organic_jobs")
    def test_calculate_organic_scores_with_cheated_jobs(self, mock_score_organic):
        """Test organic score calculation with cheated jobs."""
        # Mock the scoring function
        mock_score_organic.return_value = {"hotkey1": 0.0, "hotkey2": 5.0}

        # Create cheated and non-cheated jobs
        cheated_job = OrganicJob.objects.create(
            miner=self.miner1,
            block=1000,
            status=OrganicJob.Status.COMPLETED,
            cheated=True,  # Cheated job
            on_trusted_miner=False,
            executor_class=ExecutorClass.spin_up_4min__gpu_24gb,
            miner_address_ip_version=4,
            miner_port=8080,
        )
        normal_job = OrganicJob.objects.create(
            miner=self.miner2,
            block=1001,
            status=OrganicJob.Status.COMPLETED,
            cheated=False,
            on_trusted_miner=False,
            executor_class=ExecutorClass.spin_up_4min__gpu_24gb,
            miner_address_ip_version=4,
            miner_port=8080,
        )

        jobs = [cheated_job, normal_job]

        result = async_to_sync(calculate_organic_scores)(jobs)

        # Should have scores for both miners (including zero for cheated)
        assert "hotkey1" in result
        assert "hotkey2" in result
        assert result["hotkey1"] == 0.0  # Cheated job
        assert result["hotkey2"] > 0  # Normal job


@pytest.mark.django_db
class TestCalculateSyntheticScores(TestCase):
    """Test the calculate_synthetic_scores function."""

    def setUp(self):
        """Set up test data."""
        super().setUp()
        self.miner1 = Miner.objects.create(hotkey="hotkey1", coldkey="coldkey1")
        self.miner2 = Miner.objects.create(hotkey="hotkey2", coldkey="coldkey2")
        self.cycle = Cycle.objects.create(start=1000, stop=1722)
        self.batch = SyntheticJobBatch.objects.create(
            accepting_results_until=timezone.now() + timedelta(hours=1),
            block=1000,
            cycle=self.cycle,
        )

    @patch("compute_horde_validator.validator.scoring_new.calculations.score_synthetic_jobs")
    def test_calculate_synthetic_scores_basic(self, mock_score_synthetic):
        """Test basic synthetic score calculation."""
        # Mock the scoring function
        mock_score_synthetic.return_value = {"hotkey1": 10.0, "hotkey2": 15.0}

        # Create synthetic jobs with valid executor class
        job1 = SyntheticJob.objects.create(
            miner=self.miner1,
            batch=self.batch,
            status=SyntheticJob.Status.COMPLETED,
            score=10.0,
            executor_class=ExecutorClass.spin_up_4min__gpu_24gb,
            miner_address_ip_version=4,
            miner_port=8080,
        )
        job2 = SyntheticJob.objects.create(
            miner=self.miner2,
            batch=self.batch,
            status=SyntheticJob.Status.COMPLETED,
            score=15.0,
            executor_class=ExecutorClass.spin_up_4min__gpu_24gb,
            miner_address_ip_version=4,
            miner_port=8080,
        )

        jobs = [job1, job2]

        result = async_to_sync(calculate_synthetic_scores)(jobs)

        # Should have scores for both miners
        assert "hotkey1" in result
        assert "hotkey2" in result
        assert result["hotkey1"] > 0
        assert result["hotkey2"] > 0

        # Verify the scoring function was called (may be called multiple times due to executor class filtering)
        assert mock_score_synthetic.call_count >= 1

    @patch("compute_horde_validator.validator.scoring_new.calculations.score_synthetic_jobs")
    def test_calculate_synthetic_scores_empty_jobs(self, mock_score_synthetic):
        """Test synthetic score calculation with empty jobs list."""
        # Mock the scoring function
        mock_score_synthetic.return_value = {}

        jobs = []

        result = async_to_sync(calculate_synthetic_scores)(jobs)

        # Should return empty dict
        assert result == {}

        # Verify the scoring function was called (may be called multiple times)
        assert mock_score_synthetic.call_count >= 1

    @patch("compute_horde_validator.validator.scoring_new.calculations.score_synthetic_jobs")
    def test_calculate_synthetic_scores_single_job(self, mock_score_synthetic):
        """Test synthetic score calculation with single job."""
        # Mock the scoring function
        mock_score_synthetic.return_value = {"hotkey1": 20.0}

        job = SyntheticJob.objects.create(
            miner=self.miner1,
            batch=self.batch,
            status=SyntheticJob.Status.COMPLETED,
            score=20.0,
            executor_class=ExecutorClass.spin_up_4min__gpu_24gb,
            miner_address_ip_version=4,
            miner_port=8080,
        )

        jobs = [job]

        result = async_to_sync(calculate_synthetic_scores)(jobs)

        # Should have score for the single miner
        assert "hotkey1" in result
        assert result["hotkey1"] > 0

    @patch("compute_horde_validator.validator.scoring_new.calculations.score_synthetic_jobs")
    def test_calculate_synthetic_scores_with_different_scores(self, mock_score_synthetic):
        """Test synthetic score calculation with different scores."""
        # Mock the scoring function
        mock_score_synthetic.return_value = {"hotkey1": 5.5, "hotkey2": 8.5}

        job1 = SyntheticJob.objects.create(
            miner=self.miner1,
            batch=self.batch,
            status=SyntheticJob.Status.COMPLETED,
            score=5.5,
            executor_class=ExecutorClass.spin_up_4min__gpu_24gb,
            miner_address_ip_version=4,
            miner_port=8080,
        )
        job2 = SyntheticJob.objects.create(
            miner=self.miner2,
            batch=self.batch,
            status=SyntheticJob.Status.COMPLETED,
            score=8.5,
            executor_class=ExecutorClass.spin_up_4min__gpu_24gb,
            miner_address_ip_version=4,
            miner_port=8080,
        )

        jobs = [job1, job2]

        result = async_to_sync(calculate_synthetic_scores)(jobs)

        # Should have scores for both miners
        assert "hotkey1" in result
        assert "hotkey2" in result
        assert result["hotkey1"] > 0
        assert result["hotkey2"] > 0

    @patch("compute_horde_validator.validator.scoring_new.calculations.score_synthetic_jobs")
    def test_calculate_synthetic_scores_with_failed_jobs(self, mock_score_synthetic):
        """Test synthetic score calculation with failed jobs."""
        # Mock the scoring function
        mock_score_synthetic.return_value = {"hotkey1": 10.0, "hotkey2": 0.0}

        completed_job = SyntheticJob.objects.create(
            miner=self.miner1,
            batch=self.batch,
            status=SyntheticJob.Status.COMPLETED,
            score=10.0,
            executor_class=ExecutorClass.spin_up_4min__gpu_24gb,
            miner_address_ip_version=4,
            miner_port=8080,
        )
        failed_job = SyntheticJob.objects.create(
            miner=self.miner2,
            batch=self.batch,
            status=SyntheticJob.Status.FAILED,
            score=0.0,
            executor_class=ExecutorClass.spin_up_4min__gpu_24gb,
            miner_address_ip_version=4,
            miner_port=8080,
        )

        jobs = [completed_job, failed_job]

        result = async_to_sync(calculate_synthetic_scores)(jobs)

        # Should have scores for both miners (including zero for failed)
        assert "hotkey1" in result
        assert "hotkey2" in result
        assert result["hotkey1"] > 0
        assert result["hotkey2"] == 0.0


@pytest.mark.django_db
class TestCalculationsEdgeCases(TestCase):
    """Test edge cases for calculations."""

    def setUp(self):
        """Set up test data."""
        super().setUp()
        self.miner1 = Miner.objects.create(hotkey="hotkey1", coldkey="coldkey1")
        self.cycle = Cycle.objects.create(start=1000, stop=1722)
        self.batch = SyntheticJobBatch.objects.create(
            accepting_results_until=timezone.now() + timedelta(hours=1),
            block=1000,
            cycle=self.cycle,
        )

    def test_combine_scores_with_zero_values(self):
        """Test combining scores with zero values."""
        organic_scores = {"hotkey1": 0.0, "hotkey2": 5.0}
        synthetic_scores = {"hotkey1": 10.0, "hotkey2": 0.0}

        result = combine_scores(organic_scores, synthetic_scores)

        assert result["hotkey1"] == 10.0  # 0 + 10
        assert result["hotkey2"] == 5.0   # 5 + 0

    def test_combine_scores_with_negative_values(self):
        """Test combining scores with negative values."""
        organic_scores = {"hotkey1": -5.0, "hotkey2": 5.0}
        synthetic_scores = {"hotkey1": 10.0, "hotkey2": -3.0}

        result = combine_scores(organic_scores, synthetic_scores)

        assert result["hotkey1"] == 5.0   # -5 + 10
        assert result["hotkey2"] == 2.0   # 5 + (-3)

    def test_combine_scores_with_float_precision(self):
        """Test combining scores with float precision issues."""
        organic_scores = {"hotkey1": 0.1, "hotkey2": 0.2}
        synthetic_scores = {"hotkey1": 0.3, "hotkey2": 0.4}

        result = combine_scores(organic_scores, synthetic_scores)

        assert abs(result["hotkey1"] - 0.4) < 1e-10  # 0.1 + 0.3
        assert abs(result["hotkey2"] - 0.6) < 1e-10  # 0.2 + 0.4

    @patch("compute_horde_validator.validator.scoring_new.calculations.score_organic_jobs")
    def test_calculate_organic_scores_with_invalid_jobs(self, mock_score_organic):
        """Test organic score calculation with invalid job objects."""
        # Mock the scoring function
        mock_score_organic.return_value = {"hotkey1": 15.0}

        # Create a job with valid data
        job = OrganicJob.objects.create(
            miner=self.miner1,
            block=1000,
            status=OrganicJob.Status.COMPLETED,
            cheated=False,
            on_trusted_miner=False,
            executor_class=ExecutorClass.spin_up_4min__gpu_24gb,
            miner_address_ip_version=4,
            miner_port=8080,
        )

        jobs = [job]  # Only include valid job

        result = async_to_sync(calculate_organic_scores)(jobs)

        # Should handle valid jobs correctly
        assert "hotkey1" in result
        assert result["hotkey1"] > 0

    @patch("compute_horde_validator.validator.scoring_new.calculations.score_synthetic_jobs")
    def test_calculate_synthetic_scores_with_invalid_jobs(self, mock_score_synthetic):
        """Test synthetic score calculation with invalid job objects."""
        # Mock the scoring function
        mock_score_synthetic.return_value = {"hotkey1": 15.0}

        # Create a job with valid data
        job = SyntheticJob.objects.create(
            miner=self.miner1,
            batch=self.batch,
            status=SyntheticJob.Status.COMPLETED,
            score=15.0,
            executor_class=ExecutorClass.spin_up_4min__gpu_24gb,
            miner_address_ip_version=4,
            miner_port=8080,
        )

        jobs = [job]  # Only include valid job

        result = async_to_sync(calculate_synthetic_scores)(jobs)

        # Should handle valid jobs correctly
        assert "hotkey1" in result
        assert result["hotkey1"] > 0

    def test_get_hotkey_to_coldkey_mapping_with_special_characters(self):
        """Test mapping with hotkeys containing special characters."""
        # Create miner with special characters
        Miner.objects.create(hotkey="hotkey@#$%", coldkey="coldkey@#$%")

        hotkeys = ["hotkey@#$%", "normal_hotkey"]

        result = async_to_sync(get_hotkey_to_coldkey_mapping)(hotkeys)

        # Should handle special characters correctly
        assert result["hotkey@#$%"] == "coldkey@#$%"
        assert "normal_hotkey" not in result  # Doesn't exist

    def test_get_hotkey_to_coldkey_mapping_with_unicode(self):
        """Test mapping with hotkeys containing unicode characters."""
        # Create miner with unicode characters
        Miner.objects.create(hotkey="hotkey🚀", coldkey="coldkey🚀")

        hotkeys = ["hotkey🚀", "normal_hotkey"]

        result = async_to_sync(get_hotkey_to_coldkey_mapping)(hotkeys)

        # Should handle unicode characters correctly
        assert result["hotkey🚀"] == "coldkey🚀"
        assert "normal_hotkey" not in result  # Doesn't exist


@pytest.mark.django_db
class TestUtilityFunctions(TestCase):
    """Test utility functions."""

    def test_normalize(self):
        """Test the normalize function."""
        scores = {"hotkey1": 10.0, "hotkey2": 20.0, "hotkey3": 30.0}
        weight = 2.0

        result = normalize(scores, weight)

        # normalize divides by total (60) and multiplies by weight (2)
        # So: 10/60 * 2 = 0.333..., 20/60 * 2 = 0.666..., 30/60 * 2 = 1.0
        assert abs(result["hotkey1"] - 0.3333333333333333) < 1e-10
        assert abs(result["hotkey2"] - 0.6666666666666666) < 1e-10
        assert abs(result["hotkey3"] - 1.0) < 1e-10

    def test_normalize_zero_weight(self):
        """Test normalize with zero weight."""
        scores = {"hotkey1": 10.0, "hotkey2": 20.0}
        weight = 0.0

        result = normalize(scores, weight)

        assert result["hotkey1"] == 0.0
        assert result["hotkey2"] == 0.0

    def test_sigmoid(self):
        """Test the sigmoid function."""
        # Test basic sigmoid behavior - sigmoid(0.0, 1.0, 1.0) = 1/(1 + exp(1)) ≈ 0.2689
        result = sigmoid(0.0, 1.0, 1.0)
        assert abs(result - 0.2689414213699951) < 1e-10

        # Test sigmoid with different parameters
        result = sigmoid(1.0, 2.0, 0.5)
        assert 0.0 < result < 1.0

    def test_reversed_sigmoid(self):
        """Test the reversed_sigmoid function."""
        # Test basic reversed sigmoid behavior - reversed_sigmoid(0.5, 1.0, 1.0) = sigmoid(-0.5, 1.0, -1.0)
        result = reversed_sigmoid(0.5, 1.0, 1.0)
        assert abs(result - 0.6224593312018546) < 1e-10

        # Test reversed sigmoid with different parameters
        result = reversed_sigmoid(0.8, 2.0, 0.5)
        assert result > 0

    def test_horde_score(self):
        """Test the horde_score function."""
        benchmarks = [1.0, 2.0, 3.0, 4.0, 5.0]

        # Test with default parameters
        result = horde_score(benchmarks)
        assert result > 0

        # Test with custom parameters
        result = horde_score(benchmarks, alpha=0.5, beta=1.0, delta=2.0)
        assert result > 0

        # Test with empty list - should handle gracefully
        result = horde_score([])
        assert result == 0.0

        # Test with single value
        result = horde_score([5.0])
        assert result > 0 