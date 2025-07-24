import logging
from collections import defaultdict
from collections.abc import Callable, Sequence
from functools import partial

import numpy as np
from asgiref.sync import sync_to_async
from compute_horde_core.executor_class import ExecutorClass
from constance import config
from django.conf import settings

from compute_horde_validator.validator.dynamic_config import get_executor_class_weights
from compute_horde_validator.validator.models import Miner, OrganicJob, SyntheticJob

logger = logging.getLogger(__name__)


def normalize(scores: dict[str, float], weight: float = 1) -> dict[str, float]:
    """Normalize scores by total and apply weight multiplier."""
    total = sum(scores.values())
    if total == 0:
        return scores
    return {hotkey: weight * score / total for hotkey, score in scores.items()}


def sigmoid(x: float, beta: float, delta: float) -> float:
    """Sigmoid function for horde scoring."""
    return 1 / (1 + float(np.exp(beta * (-x + delta))))


def reversed_sigmoid(x: float, beta: float, delta: float) -> float:
    """Reversed sigmoid function for horde scoring."""
    return sigmoid(-x, beta=beta, delta=-delta)


def horde_score(
    benchmarks: list[float], alpha: float = 0, beta: float = 0, delta: float = 0
) -> float:
    """
    Proportionally scores horde benchmarks allowing increasing significance for chosen features

    By default, scores are proportional to horde "strength" - having 10 executors would have the same
    score as separate 10 single executor miners. Subnet owner can control significance of defined features:

    alpha - controls significance of average score, so smaller horde can have higher score if executors are stronger;
            the best values are from range [0, 1], with 0 meaning no effect
    beta - controls sigmoid function steepness; sigmoid function is over `-(1 / horde_size)`, so larger hordes can be
           more significant than smaller ones, even if summary strength of a horde is the same;
           the best values are from range [0,5] (or more, but higher values does not change sigmoid steepness much),
           with 0 meaning no effect
    delta - controls where sigmoid function has 0.5 value allowing for better control over effect of beta param;
            the best values are from range [0, 1]
    """
    sum_agent = sum(benchmarks)
    inverted_n = 1 / len(benchmarks)
    avg_benchmark = sum_agent * inverted_n
    scaled_inverted_n = reversed_sigmoid(inverted_n, beta=10**beta, delta=delta)
    scaled_avg_benchmark = float(avg_benchmark**alpha)
    return scaled_avg_benchmark * sum_agent * scaled_inverted_n


async def score_synthetic_jobs(
    jobs: Sequence[SyntheticJob],
    score_aggregation: Callable[[list[float]], float] = sum,
) -> dict[str, float]:
    """Score synthetic jobs using the provided aggregation function."""
    batch_scores = defaultdict(list)
    score_per_hotkey = {}
    for job in jobs:
        hotkey = job.miner.hotkey
        batch_scores[hotkey].append(job.score)
    for hotkey, hotkey_batch_scores in batch_scores.items():
        score_per_hotkey[hotkey] = score_aggregation(hotkey_batch_scores)
    return score_per_hotkey


async def score_organic_jobs(jobs: Sequence[OrganicJob]) -> dict[str, float]:
    """Score organic jobs with configurable limits."""
    batch_scores: defaultdict[str, float] = defaultdict(float)
    score = await sync_to_async(lambda: config.DYNAMIC_ORGANIC_JOB_SCORE)()
    limit = await sync_to_async(lambda: config.DYNAMIC_SCORE_ORGANIC_JOBS_LIMIT)()

    for job in jobs:
        batch_scores[job.miner.hotkey] += score

    if limit >= 0:
        for hotkey, score in batch_scores.items():
            batch_scores[hotkey] = min(score, limit * score)

    return batch_scores


async def calculate_organic_scores(organic_jobs: list[OrganicJob]) -> dict[str, float]:
    """
    Calculate scores from organic jobs using executor class-based logic.

    Args:
        organic_jobs: List of organic jobs

    Returns:
        Dictionary mapping hotkey to score
    """
    executor_class_weights = await sync_to_async(get_executor_class_weights)()
    executor_class_organic_jobs = defaultdict(list)

    # Group organic jobs by executor class
    for job in organic_jobs:
        if (
            job.status == OrganicJob.Status.COMPLETED
            and not job.cheated
            and not job.on_trusted_miner
            and job.executor_class in executor_class_weights
        ):
            executor_class = ExecutorClass(job.executor_class)
            executor_class_organic_jobs[executor_class].append(job)

    # Calculate scores per executor class
    batch_scores: defaultdict[str, float] = defaultdict(float)
    for executor_class, executor_class_weight in executor_class_weights.items():
        organic_jobs_for_class = executor_class_organic_jobs.get(executor_class, [])
        executor_class_organic_scores = await score_organic_jobs(organic_jobs_for_class)

        # Normalize scores for executor class weight
        normalized_scores = normalize(executor_class_organic_scores, executor_class_weight)
        for hotkey, score in normalized_scores.items():
            batch_scores[hotkey] += score

    return dict(batch_scores)


async def calculate_synthetic_scores(synthetic_jobs: list[SyntheticJob]) -> dict[str, float]:
    """
    Calculate scores from synthetic jobs using executor class-based logic.

    Args:
        synthetic_jobs: List of synthetic jobs

    Returns:
        Dictionary mapping hotkey to score
    """
    executor_class_weights = await sync_to_async(get_executor_class_weights)()
    executor_class_synthetic_jobs = defaultdict(list)

    # Group synthetic jobs by executor class
    for job in synthetic_jobs:
        if job.executor_class in executor_class_weights:
            executor_class = ExecutorClass(job.executor_class)
            executor_class_synthetic_jobs[executor_class].append(job)

    # Create parameterized horde score function
    parameterized_horde_score: Callable[[list[float]], float] = partial(
        horde_score,
        # scaling factor for avg_score of a horde - best in range [0, 1] (0 means no effect on score)
        alpha=settings.HORDE_SCORE_AVG_PARAM,
        # sigmoid steepness param - best in range [0, 5] (0 means no effect on score)
        beta=settings.HORDE_SCORE_SIZE_PARAM,
        # horde size for 0.5 value of sigmoid - sigmoid is for 1 / horde_size
        delta=1 / settings.HORDE_SCORE_CENTRAL_SIZE_PARAM,
    )

    # Calculate scores per executor class
    batch_scores: defaultdict[str, float] = defaultdict(float)
    for executor_class, executor_class_weight in executor_class_weights.items():
        synthetic_jobs_for_class = executor_class_synthetic_jobs.get(executor_class, [])

        # Use horde scoring for specific executor class, sum for others
        if executor_class == ExecutorClass.spin_up_4min__gpu_24gb:
            score_aggregation = parameterized_horde_score
        else:
            score_aggregation = sum

        executor_class_synthetic_scores = await score_synthetic_jobs(
            synthetic_jobs_for_class,
            score_aggregation=score_aggregation,
        )

        # Normalize scores for executor class weight
        normalized_scores = normalize(executor_class_synthetic_scores, executor_class_weight)
        for hotkey, score in normalized_scores.items():
            batch_scores[hotkey] += score

    return dict(batch_scores)


def combine_scores(
    organic_scores: dict[str, float], synthetic_scores: dict[str, float]
) -> dict[str, float]:
    """
    Combine organic and synthetic scores.

    Args:
        organic_scores: Scores from organic jobs
        synthetic_scores: Scores from synthetic jobs

    Returns:
        Combined scores by hotkey
    """
    combined: defaultdict[str, float] = defaultdict(float)

    # Add organic scores
    for hotkey, score in organic_scores.items():
        combined[hotkey] += score

    # Add synthetic scores
    for hotkey, score in synthetic_scores.items():
        combined[hotkey] += score

    return dict(combined)


def get_coldkey_to_hotkey_mapping(miners: list[Miner]) -> dict[str, list[str]]:
    """
    Create a mapping from coldkey to list of hotkeys for all miners.

    Args:
        miners: List of Miner objects

    Returns:
        Dictionary mapping coldkey to list of hotkeys
    """
    mapping: dict[str, list[str]] = {}
    for miner in miners:
        if miner.coldkey:
            if miner.coldkey not in mapping:
                mapping[miner.coldkey] = []
            mapping[miner.coldkey].append(miner.hotkey)
    return mapping


async def get_hotkey_to_coldkey_mapping(hotkeys: list[str]) -> dict[str, str]:
    """
    Get hotkey to coldkey mapping from database.
    Missing coldkeys will be logged and synced during next metagraph sync.

    Args:
        hotkeys: List of hotkeys to get coldkeys for

    Returns:
        Dictionary mapping hotkeys to coldkeys
    """
    # Get from database only
    miners: list[Miner] = await sync_to_async(
        lambda: list(Miner.objects.filter(hotkey__in=hotkeys).select_related())
    )()
    hotkey_to_coldkey: dict[str, str] = {}
    missing_hotkeys: list[str] = []

    for miner in miners:
        if miner.coldkey:
            hotkey_to_coldkey[miner.hotkey] = miner.coldkey
        else:
            missing_hotkeys.append(miner.hotkey)

    if missing_hotkeys:
        logger.warning(
            f"Missing coldkeys for hotkeys: {missing_hotkeys}. Will be synced during next metagraph sync."
        )

    return hotkey_to_coldkey
