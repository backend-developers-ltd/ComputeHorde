import logging
from collections import defaultdict
from collections.abc import Callable, Sequence

import numpy as np
from asgiref.sync import sync_to_async
from constance import config
from django.db.models import QuerySet

from ..models import Miner, OrganicJob, SyntheticJob
from .models import MinerSplit, MinerSplitDistribution

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


def calculate_organic_scores(organic_jobs: QuerySet[OrganicJob]) -> dict[str, float]:
    """
    Calculate scores from organic jobs.

    Args:
        organic_jobs: QuerySet of organic jobs

    Returns:
        Dictionary mapping hotkey to score
    """
    scores = defaultdict(float)

    for job in organic_jobs:
        if job.status == OrganicJob.Status.COMPLETED and not job.cheated:
            scores[job.miner.hotkey] += 1.0

    return dict(scores)


def calculate_synthetic_scores(synthetic_jobs: QuerySet[SyntheticJob]) -> dict[str, float]:
    """
    Calculate scores from synthetic jobs.

    Args:
        synthetic_jobs: QuerySet of synthetic jobs

    Returns:
        Dictionary mapping hotkey to score
    """
    scores = defaultdict(float)

    for job in synthetic_jobs:
        if job.status == SyntheticJob.Status.COMPLETED:
            scores[job.miner.hotkey] += job.score

    return dict(scores)


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
    combined = defaultdict(float)

    # Add organic scores
    for hotkey, score in organic_scores.items():
        combined[hotkey] += score

    # Add synthetic scores
    for hotkey, score in synthetic_scores.items():
        combined[hotkey] += score

    return dict(combined)


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
    """Score organic jobs with configurable limits."""
    batch_scores: defaultdict[str, float] = defaultdict(float)
    score = config.DYNAMIC_ORGANIC_JOB_SCORE
    limit = config.DYNAMIC_SCORE_ORGANIC_JOBS_LIMIT

    for job in jobs:
        batch_scores[job.miner.hotkey] += score

    if limit >= 0:
        for hotkey, score in batch_scores.items():
            batch_scores[hotkey] = min(score, limit * score)

    return batch_scores


def get_coldkey_to_hotkey_mapping(miners: list) -> dict[str, list[str]]:
    """
    Create a mapping from coldkey to list of hotkeys for all miners.

    Args:
        miners: List of Miner objects

    Returns:
        Dictionary mapping coldkey to list of hotkeys
    """
    mapping = {}
    for miner in miners:
        if miner.coldkey:
            if miner.coldkey not in mapping:
                mapping[miner.coldkey] = []
            mapping[miner.coldkey].append(miner.hotkey)
    return mapping


async def get_hotkey_to_coldkey_mapping(hotkeys: list[str]) -> dict[str, str]:
    """
    Get hotkey to coldkey mapping with fallback to Bittensor if needed.

    Args:
        hotkeys: List of hotkeys to get coldkeys for

    Returns:
        Dictionary mapping hotkeys to coldkeys
    """
    # First try to get from database
    miners = await sync_to_async(list)(Miner.objects.filter(hotkey__in=hotkeys).select_related())
    hotkey_to_coldkey = {}
    missing_hotkeys = []

    for miner in miners:
        if miner.coldkey:
            hotkey_to_coldkey[miner.hotkey] = miner.coldkey
        else:
            missing_hotkeys.append(miner.hotkey)

    # If we have missing hotkeys, try to fetch from Bittensor
    if missing_hotkeys:
        try:
            import bittensor
            from django.conf import settings

            # Connect to Bittensor
            subtensor = bittensor.subtensor(network=settings.BITTENSOR_NETWORK)
            metagraph = subtensor.metagraph(netuid=settings.BITTENSOR_NETUID)

            # Get missing mappings
            for neuron in metagraph.neurons:
                if neuron.hotkey in missing_hotkeys:
                    hotkey_to_coldkey[neuron.hotkey] = neuron.coldkey

            # Update database with new mappings
            miners_to_update = []
            for miner in miners:
                if miner.hotkey in missing_hotkeys and miner.hotkey in hotkey_to_coldkey:
                    miner.coldkey = hotkey_to_coldkey[miner.hotkey]
                    miners_to_update.append(miner)

            if miners_to_update:
                Miner.objects.bulk_update(miners_to_update, fields=["coldkey"])
                logger.info(f"Updated {len(miners_to_update)} coldkey mappings from Bittensor")

        except Exception as e:
            # Log the error but don't raise - return partial mapping
            logger.warning(f"Failed to fetch coldkey mappings from Bittensor: {e}")
            # Continue with partial mapping from database

    return hotkey_to_coldkey


def split_changed_from_previous_cycle(
    coldkey: str, current_cycle_start: int, validator_hotkey: str
) -> bool:
    """
    Check if the split distribution changed from the previous cycle.

    Args:
        coldkey: Miner coldkey
        current_cycle_start: Current cycle start block
        validator_hotkey: Validator hotkey

    Returns:
        True if split changed, False otherwise
    """
    try:
        # Get previous cycle (722 blocks per cycle)
        previous_cycle_start = current_cycle_start - 722

        # Get current split
        current_split = MinerSplit.objects.get(
            coldkey=coldkey, cycle_start=current_cycle_start, validator_hotkey=validator_hotkey
        )
        current_distributions = MinerSplitDistribution.objects.filter(split=current_split)
        current_distribution = {d.hotkey: float(d.percentage) for d in current_distributions}

        # Get previous split
        previous_split = MinerSplit.objects.get(
            coldkey=coldkey, cycle_start=previous_cycle_start, validator_hotkey=validator_hotkey
        )
        previous_distributions = MinerSplitDistribution.objects.filter(split=previous_split)
        previous_distribution = {d.hotkey: float(d.percentage) for d in previous_distributions}

        # Compare distributions
        if current_distribution != previous_distribution:
            logger.debug(
                f"Split changed for {coldkey}: {previous_distribution} -> {current_distribution}"
            )
            return True
        else:
            logger.debug(f"No split change for {coldkey}")
            return False

    except MinerSplit.DoesNotExist:
        # If no previous split exists, consider it as "changed" (new split)
        logger.debug(f"No previous split found for {coldkey}, considering as changed")
        return True
    except MinerSplitDistribution.DoesNotExist:
        # If no distributions exist, consider it as "changed"
        logger.debug(f"No distributions found for {coldkey}, considering as changed")
        return True
    except Exception as e:
        # Log error but don't fail - assume no change
        logger.warning(f"Error checking split change for {coldkey}: {e}")
        return False
