"""Tests for score_organic_jobs in calculations.py.

These tests verify that the organic job score cap (DYNAMIC_SCORE_ORGANIC_JOBS_LIMIT)
is correctly applied and that the variable-shadowing bug that previously made the cap
a no-op is fixed.
"""
from unittest.mock import MagicMock, patch

import pytest

from compute_horde_validator.validator.scoring.calculations import score_organic_jobs


def _make_job(hotkey: str) -> MagicMock:
    """Return a minimal OrganicJob-like mock with the given miner hotkey."""
    job = MagicMock()
    job.miner.hotkey = hotkey
    return job


def _patch_config(per_job_score: float, limit: int):
    """Patch get_config to return the provided organic job scoring settings."""
    config_values = {
        "DYNAMIC_ORGANIC_JOB_SCORE": per_job_score,
        "DYNAMIC_SCORE_ORGANIC_JOBS_LIMIT": limit,
    }
    return patch(
        "compute_horde_validator.validator.scoring.calculations.get_config",
        side_effect=lambda key: config_values[key],
    )


class TestScoreOrganicJobsNoCap:
    """limit < 0 means unlimited — no cap should be applied."""

    def test_empty_job_list_returns_empty(self):
        with _patch_config(per_job_score=1.0, limit=-1):
            result = score_organic_jobs([])
        assert result == {}

    def test_single_miner_single_job(self):
        jobs = [_make_job("hotkey_a")]
        with _patch_config(per_job_score=1.0, limit=-1):
            result = score_organic_jobs(jobs)
        assert result == {"hotkey_a": 1.0}

    def test_single_miner_multiple_jobs_accumulate(self):
        jobs = [_make_job("hotkey_a")] * 5
        with _patch_config(per_job_score=1.0, limit=-1):
            result = score_organic_jobs(jobs)
        assert result == {"hotkey_a": 5.0}

    def test_multiple_miners_scored_independently(self):
        jobs = [_make_job("hotkey_a")] * 3 + [_make_job("hotkey_b")] * 2
        with _patch_config(per_job_score=1.0, limit=-1):
            result = score_organic_jobs(jobs)
        assert result == {"hotkey_a": 3.0, "hotkey_b": 2.0}

    def test_custom_per_job_score(self):
        jobs = [_make_job("hotkey_a")] * 4
        with _patch_config(per_job_score=2.5, limit=-1):
            result = score_organic_jobs(jobs)
        assert result["hotkey_a"] == pytest.approx(10.0)


class TestScoreOrganicJobsWithCap:
    """limit >= 0 means a score cap of (limit * per_job_score) should be enforced."""

    def test_cap_applied_when_jobs_exceed_limit(self):
        """Miner with 5 jobs but limit=3 should be capped at 3 * score."""
        jobs = [_make_job("hotkey_a")] * 5
        with _patch_config(per_job_score=1.0, limit=3):
            result = score_organic_jobs(jobs)
        assert result["hotkey_a"] == pytest.approx(3.0)

    def test_cap_not_applied_when_jobs_below_limit(self):
        """Miner with 2 jobs and limit=5 should keep the full accumulated score."""
        jobs = [_make_job("hotkey_a")] * 2
        with _patch_config(per_job_score=1.0, limit=5):
            result = score_organic_jobs(jobs)
        assert result["hotkey_a"] == pytest.approx(2.0)

    def test_cap_at_exact_limit(self):
        """Miner with exactly limit jobs should receive exactly cap score."""
        jobs = [_make_job("hotkey_a")] * 4
        with _patch_config(per_job_score=1.0, limit=4):
            result = score_organic_jobs(jobs)
        assert result["hotkey_a"] == pytest.approx(4.0)

    def test_cap_with_nonunit_per_job_score(self):
        """Cap = limit * per_job_score, not just limit."""
        # 10 jobs * 2.0 score = 20.0 accumulated; cap = 5 * 2.0 = 10.0
        jobs = [_make_job("hotkey_a")] * 10
        with _patch_config(per_job_score=2.0, limit=5):
            result = score_organic_jobs(jobs)
        assert result["hotkey_a"] == pytest.approx(10.0)

    def test_cap_zero_limit_clamps_to_zero(self):
        """limit=0 means cap=0; all accumulated scores should be clamped to 0."""
        jobs = [_make_job("hotkey_a")] * 3
        with _patch_config(per_job_score=1.0, limit=0):
            result = score_organic_jobs(jobs)
        assert result["hotkey_a"] == pytest.approx(0.0)

    def test_cap_applied_independently_per_miner(self):
        """Each miner's accumulated score is capped independently."""
        # hotkey_a has 6 jobs (exceeds limit=3), hotkey_b has 2 jobs (below limit)
        jobs = [_make_job("hotkey_a")] * 6 + [_make_job("hotkey_b")] * 2
        with _patch_config(per_job_score=1.0, limit=3):
            result = score_organic_jobs(jobs)
        assert result["hotkey_a"] == pytest.approx(3.0)
        assert result["hotkey_b"] == pytest.approx(2.0)

    def test_regression_cap_was_previously_noop(self):
        """Regression test for the variable-shadowing bug.

        The original code shadowed the per-job `score` variable with the loop
        variable in ``for hotkey, score in batch_scores.items()``, causing
        ``min(score, limit * score)`` to always equal `score` when limit >= 1.
        The cap was never enforced.

        With the fix, a miner accumulating more jobs than the limit allows must
        have their score clamped to ``limit * per_job_score``.
        """
        # 10 jobs with per_job_score=1.0 and limit=3 → must be capped at 3.0, not 10.0
        jobs = [_make_job("hotkey_a")] * 10
        with _patch_config(per_job_score=1.0, limit=3):
            result = score_organic_jobs(jobs)
        assert result["hotkey_a"] == pytest.approx(3.0), (
            "Cap was not enforced — likely the variable-shadowing bug is present"
        )
