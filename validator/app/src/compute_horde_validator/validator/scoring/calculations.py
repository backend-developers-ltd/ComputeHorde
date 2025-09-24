import logging
from collections import defaultdict
from collections.abc import Callable, Sequence

import numpy as np
from compute_horde_core.executor_class import ExecutorClass
from constance import config

from compute_horde_validator.validator.allowance.default import allowance
from compute_horde_validator.validator.allowance.types import CannotSpend, ErrorWhileSpending
from compute_horde_validator.validator.allowance.utils.spending import Triplet
from compute_horde_validator.validator.models import Miner, OrganicJob, SyntheticJob
from compute_horde_validator.validator.receipts import receipts

logger = logging.getLogger(__name__)


def calculate_allowance_paid_job_scores(
    start_block: int, end_block: int
) -> dict[str, dict[str, float]]:
    """
    Give scores based on executor seconds of finished jobs that were properly paid for with allowance.
    End block is exclusive.
    """
    scores: defaultdict[ExecutorClass, defaultdict[str, float]] = defaultdict(
        lambda: defaultdict(float)
    )

    for executor_class in ExecutorClass:
        # Find out what jobs finished within the time and get the associated spending info
        job_spendings = receipts().get_finished_jobs_for_block_range(
            start_block, end_block, executor_class
        )

        logger.info(
            f"Found {len(job_spendings)} jobs "
            f"for {executor_class} "
            f"finished between blocks {start_block} and {end_block}"
        )

        if not job_spendings:
            continue

        # Ask allowance module for a bookkeeper, which will be used to validate the spendings in-memory
        bookkeeper = allowance().get_temporary_bookkeeper(start_block, end_block)

        # Try to execute the spendings, score successfully paid jobs, log invalid ones
        for spending in job_spendings:
            triplet = Triplet(
                spending.validator_hotkey, spending.miner_hotkey, spending.executor_class
            )
            job_cost = spending.executor_seconds_cost
            logger.debug(
                "Validating spending job=%s validator=%s miner=%s value=%s blocks=%s",
                spending.job_uuid,
                spending.validator_hotkey,
                spending.miner_hotkey,
                job_cost,
                spending.paid_with_blocks,
            )
            try:
                bookkeeper.spend(triplet, spending.started_at, spending.paid_with_blocks, job_cost)
                scores[triplet.executor_class][triplet.miner] += job_cost
                logger.info(
                    f"Registered valid spending for job {spending.job_uuid} with value {job_cost}"
                )
            except CannotSpend as e:
                # TODO(new scoring): System event / metric
                logger.warning(e)
            except ErrorWhileSpending as e:
                # TODO(new scoring): System event / metric
                logger.warning(e)
            finally:
                # Just so that we return something for triplets that failed everything
                scores[triplet.executor_class].setdefault(triplet.miner, 0)

    # Convert back to regular dicts and return
    return {
        executor_class: {miner: score for miner, score in miner_scores.items()}
        for executor_class, miner_scores in scores.items()
    }


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
    if not benchmarks:
        return 0.0

    sum_agent = sum(benchmarks)
    inverted_n = 1 / len(benchmarks)
    avg_benchmark = sum_agent * inverted_n
    scaled_inverted_n = reversed_sigmoid(inverted_n, beta=10**beta, delta=delta)
    scaled_avg_benchmark = float(avg_benchmark**alpha)
    return scaled_avg_benchmark * sum_agent * scaled_inverted_n


def score_synthetic_jobs(
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


def score_organic_jobs(jobs: Sequence[OrganicJob]) -> dict[str, float]:
    """Score organic jobs."""
    batch_scores: defaultdict[str, float] = defaultdict(float)
    score = config.DYNAMIC_ORGANIC_JOB_SCORE
    limit = config.DYNAMIC_SCORE_ORGANIC_JOBS_LIMIT

    for job in jobs:
        batch_scores[job.miner.hotkey] += score

    if limit >= 0:
        for hotkey, score in batch_scores.items():
            batch_scores[hotkey] = min(score, limit * score)

    return batch_scores


def calculate_organic_scores(organic_jobs: list[OrganicJob]) -> dict[str, dict[str, float]]:
    """
    Calculate raw scores from organic jobs for each executor class.

    Args:
        organic_jobs: List of organic jobs

    Returns:
        Dictionary mapping executor class to hotkey scores
    """
    executor_class_organic_jobs = defaultdict(list)

    # Group organic jobs by executor class
    for job in organic_jobs:
        if (
            job.status == OrganicJob.Status.COMPLETED
            and not job.cheated
            and not job.on_trusted_miner
        ):
            executor_class_organic_jobs[job.executor_class].append(job)

    organic_scores_by_executor: dict[str, dict[str, float]] = {}
    organic_job_score = config.DYNAMIC_ORGANIC_JOB_SCORE

    for executor_class, jobs in executor_class_organic_jobs.items():
        executor_class_scores: dict[str, float] = {}
        for job in jobs:
            hotkey = job.miner.hotkey
            executor_class_scores[hotkey] = executor_class_scores.get(hotkey, 0) + organic_job_score

        if executor_class_scores:
            organic_scores_by_executor[executor_class] = executor_class_scores

    return organic_scores_by_executor


def calculate_synthetic_scores(
    synthetic_jobs: list[SyntheticJob],
) -> dict[str, dict[str, float]]:
    """
    Calculate raw scores from synthetic jobs for each executor class.

    Args:
        synthetic_jobs: List of synthetic jobs

    Returns:
        Dictionary mapping executor class to hotkey scores
    """
    executor_class_synthetic_jobs = defaultdict(list)

    # Group synthetic jobs by executor class
    for job in synthetic_jobs:
        if job.status == SyntheticJob.Status.COMPLETED:
            executor_class_synthetic_jobs[job.executor_class].append(job)

    synthetic_scores_by_executor: dict[str, dict[str, float]] = {}

    for executor_class, jobs in executor_class_synthetic_jobs.items():
        executor_class_scores: dict[str, float] = {}

        for job in jobs:
            hotkey = job.miner.hotkey
            executor_class_scores[hotkey] = executor_class_scores.get(hotkey, 0) + job.score

        if executor_class_scores:
            synthetic_scores_by_executor[executor_class] = executor_class_scores

    return synthetic_scores_by_executor


def combine_scores(
    organic_scores_by_executor: dict[str, dict[str, float]],
    synthetic_scores_by_executor: dict[str, dict[str, float]],
) -> dict[str, dict[str, float]]:
    """
    Combine organic and synthetic scores for each executor class.

    Args:
        organic_scores_by_executor: Raw scores from organic jobs grouped by executor class
        synthetic_scores_by_executor: Raw scores from synthetic jobs grouped by executor class

    Returns:
        Combined raw scores grouped by executor class (not normalized)
    """
    combined_scores_by_executor: dict[str, dict[str, float]] = {}

    all_executor_classes = set(organic_scores_by_executor.keys()) | set(
        synthetic_scores_by_executor.keys()
    )

    for executor_class in all_executor_classes:
        executor_class_scores: dict[str, float] = {}

        if executor_class in organic_scores_by_executor:
            for hotkey, score in organic_scores_by_executor[executor_class].items():
                executor_class_scores[hotkey] = executor_class_scores.get(hotkey, 0) + score

        if executor_class in synthetic_scores_by_executor:
            for hotkey, score in synthetic_scores_by_executor[executor_class].items():
                executor_class_scores[hotkey] = executor_class_scores.get(hotkey, 0) + score

        if executor_class_scores:
            combined_scores_by_executor[executor_class] = executor_class_scores

    return combined_scores_by_executor


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


def get_hotkey_to_coldkey_mapping(hotkeys: list[str]) -> dict[str, str]:
    """
    Get hotkey to coldkey mapping from database.

    Args:
        hotkeys: List of hotkeys to get coldkeys for

    Returns:
        Dictionary mapping hotkeys to coldkeys
    """
    # Get from database only
    miners: list[Miner] = list(Miner.objects.filter(hotkey__in=hotkeys).select_related())
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
