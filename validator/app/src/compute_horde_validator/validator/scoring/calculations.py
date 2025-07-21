"""
Score calculation functions for organic and synthetic jobs.
"""

import logging
from collections import defaultdict
from collections.abc import Callable, Sequence
from functools import partial
from typing import Dict, List

import numpy as np
from compute_horde_core.executor_class import ExecutorClass
from constance import config
from django.conf import settings
from django.db.models import QuerySet

from ..models import OrganicJob, SyntheticJob, SyntheticJobBatch, MinerManifest, Miner
from ..dynamic_config import get_executor_class_weights, get_weights_version
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


def calculate_organic_scores(organic_jobs: QuerySet[OrganicJob]) -> Dict[str, float]:
    """
    Calculate scores from organic jobs (without dancing).
    
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


def calculate_synthetic_scores(synthetic_jobs: QuerySet[SyntheticJob]) -> Dict[str, float]:
    """
    Calculate scores from synthetic jobs (without dancing).
    
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


def combine_scores(organic_scores: Dict[str, float], synthetic_scores: Dict[str, float]) -> Dict[str, float]:
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


def get_executor_counts(batch: SyntheticJobBatch | None) -> dict[str, dict[ExecutorClass, int]]:
    """In a given batch, get the number of online executors per miner per executor class"""
    if not batch:
        return {}

    result: dict[str, dict[ExecutorClass, int]] = defaultdict(lambda: defaultdict(int))

    for manifest in MinerManifest.objects.select_related("miner").filter(batch_id=batch.id):
        executor_class = ExecutorClass(manifest.executor_class)
        result[manifest.miner.hotkey][executor_class] += manifest.online_executor_count

    return result


def get_base_synthetic_score(
    executor_counts: dict[ExecutorClass, int],
    executor_class_weights: dict[ExecutorClass, float],
) -> float:
    """Calculate base synthetic score from executor counts and weights."""
    score = 0.0
    for executor_class, weight in executor_class_weights.items():
        score += weight * executor_counts.get(executor_class, 0)
    return score


def get_manifest_multiplier(
    previous_base_synthetic_score: float | None,
    current_base_synthetic_score: float | None,
) -> float:
    """Calculate manifest multiplier based on previous and current scores."""
    weights_version = get_weights_version()
    multiplier = 1.0
    if weights_version >= 2:
        if previous_base_synthetic_score is None or current_base_synthetic_score is None:
            # give bonus if miner was not present in either of the previous peak cycles
            multiplier = config.DYNAMIC_MANIFEST_SCORE_MULTIPLIER
        else:
            low, high = sorted([previous_base_synthetic_score, current_base_synthetic_score])
            # low can be 0 if previous_online_executors == 0, but we make it that way to
            # make this function correct for any kind of input
            threshold = config.DYNAMIC_MANIFEST_DANCE_RATIO_THRESHOLD
            if low == 0 or high / low >= threshold:
                multiplier = config.DYNAMIC_MANIFEST_SCORE_MULTIPLIER
    return multiplier


def get_penalty_multiplier(
    peak_executor_counts: dict[ExecutorClass, int] | None,
    curr_executor_counts: dict[ExecutorClass, int] | None,
) -> float:
    """Calculate penalty multiplier for non-peak cycles."""
    if not peak_executor_counts:
        # Miner was not present during the peak cycle, so don't give penalty.
        return 1.0
    elif not curr_executor_counts:
        # Miner was present during peak, but not current cycle's synthetic jobs.
        return float(config.DYNAMIC_NON_PEAK_CYCLE_PENALTY_MULTIPLIER)

    for executor_class, peak_count in peak_executor_counts.items():
        if peak_count == 0:
            continue

        if executor_class not in curr_executor_counts:
            return float(config.DYNAMIC_NON_PEAK_CYCLE_PENALTY_MULTIPLIER)

        curr_count = curr_executor_counts[executor_class]
        if curr_count / peak_count < config.DYNAMIC_NON_PEAK_CYCLE_EXECUTOR_MIN_RATIO:
            return float(config.DYNAMIC_NON_PEAK_CYCLE_PENALTY_MULTIPLIER)

    return 1.0


def get_coldkey_to_hotkey_mapping(miners: list) -> dict[str, str]:
    """
    Create a mapping from coldkey to hotkey for all miners.
    """
    mapping = {}
    for miner in miners:
        if miner.coldkey:
            mapping[miner.coldkey] = miner.hotkey
    return mapping


def get_hotkey_to_coldkey_mapping(hotkeys: list[str]) -> dict[str, str]:
    """
    Get hotkey to coldkey mapping with fallback to Bittensor if needed.
    
    Args:
        hotkeys: List of hotkeys to get coldkeys for
        
    Returns:
        Dictionary mapping hotkeys to coldkeys
    """
    # First try to get from database
    miners = list(Miner.objects.filter(hotkey__in=hotkeys).select_related())
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
                Miner.objects.bulk_update(miners_to_update, fields=['coldkey'])
                logger.info(f"Updated {len(miners_to_update)} coldkey mappings from Bittensor")
                
        except Exception as e:
            logger.warning(f"Failed to fetch coldkey mappings from Bittensor: {e}")
    
    return hotkey_to_coldkey


def split_changed_from_previous_cycle(coldkey: str, current_cycle_start: int, validator_hotkey: str) -> bool:
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
            coldkey=coldkey,
            cycle_start=current_cycle_start,
            validator_hotkey=validator_hotkey
        )
        current_distributions = MinerSplitDistribution.objects.filter(split=current_split)
        current_distribution = {d.hotkey: float(d.percentage) for d in current_distributions}
        
        # Get previous split
        previous_split = MinerSplit.objects.get(
            coldkey=coldkey,
            cycle_start=previous_cycle_start,
            validator_hotkey=validator_hotkey
        )
        previous_distributions = MinerSplitDistribution.objects.filter(split=previous_split)
        previous_distribution = {d.hotkey: float(d.percentage) for d in previous_distributions}
        
        # Compare distributions
        if current_distribution != previous_distribution:
            logger.debug(f"Split changed for {coldkey}: {previous_distribution} -> {current_distribution}")
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


def apply_decoupled_dancing_weights(
    scores: dict[str, float], 
    batches: Sequence[SyntheticJobBatch],
    validator_hotkey: str
) -> dict[str, float]:
    """
    Apply decoupled dancing weight adjustments.
    This splits weights between hotkeys in the same group (coldkey).
    
    Args:
        scores: Base scores by hotkey
        batches: Sequence of batches being scored
        validator_hotkey: Validator hotkey for split retrieval
        
    Returns:
        Final scores with dancing adjustments applied
    """
    if not batches:
        return scores
    
    # Get the latest batch to determine the cycle
    latest_batch = batches[-1]
    cycle_start = latest_batch.cycle.start
    
    # Get all miners in one query for efficiency
    miners = list(Miner.objects.filter(hotkey__in=scores.keys()).select_related())
    hotkey_to_coldkey = get_hotkey_to_coldkey_mapping(list(scores.keys()))
    
    # Group scores by coldkey
    coldkey_groups: dict[str, dict[str, float]] = defaultdict(dict)
    for hotkey, score in scores.items():
        coldkey = hotkey_to_coldkey.get(hotkey)
        if coldkey:
            coldkey_groups[coldkey][hotkey] = score
    
    # Apply decoupled dancing adjustments
    adjusted_scores = {}
    
    for coldkey, group_scores in coldkey_groups.items():
        if len(group_scores) > 1:
            # Multiple hotkeys in this coldkey group - apply split distribution
            try:
                # Get split distribution for this coldkey
                split = MinerSplit.objects.get(
                    coldkey=coldkey,
                    cycle_start=cycle_start,
                    validator_hotkey=validator_hotkey
                )
                
                # Get the distribution percentages
                distributions = MinerSplitDistribution.objects.filter(split=split)
                split_distribution = {d.hotkey: float(d.percentage) for d in distributions}
                
                # Validate that percentages sum to 1.0 (with small tolerance for floating point)
                total_percentage = sum(split_distribution.values())
                if abs(total_percentage - 1.0) > 0.0001:
                    logger.warning(f"Split distribution for {coldkey} doesn't sum to 1.0: {total_percentage}")
                    # Keep original scores for this group
                    adjusted_scores.update(group_scores)
                    continue
                
                # Calculate total score for the group
                total_group_score = sum(group_scores.values())
                
                # Apply the split distribution to hotkeys that exist in the group
                group_adjusted_scores = {}
                for hotkey, percentage in split_distribution.items():
                    if hotkey in group_scores:
                        group_adjusted_scores[hotkey] = total_group_score * percentage
                
                # Check if split changed from previous cycle
                if split_changed_from_previous_cycle(coldkey, cycle_start, validator_hotkey):
                    # Apply dancing bonus to this group only
                    from constance import config
                    dancing_bonus = getattr(config, 'DYNAMIC_DANCING_BONUS', 0.1)  # 10% bonus by default
                    
                    for hotkey in group_adjusted_scores:
                        group_adjusted_scores[hotkey] *= (1 + dancing_bonus)
                    
                    logger.info(f"Applied dancing bonus ({dancing_bonus}) to coldkey: {coldkey}")
                
                # Add the adjusted scores for this group
                adjusted_scores.update(group_adjusted_scores)
                
                # Add any hotkeys in the group that weren't in the split distribution
                for hotkey, score in group_scores.items():
                    if hotkey not in group_adjusted_scores:
                        adjusted_scores[hotkey] = score
                        
            except MinerSplit.DoesNotExist:
                # No split found for this coldkey, keep original scores
                adjusted_scores.update(group_scores)
                logger.debug(f"No split found for coldkey: {coldkey}")
                
        else:
            # Single hotkey in group, keep original score
            adjusted_scores.update(group_scores)
    
    # Add scores for hotkeys not in any group (no coldkey)
    for hotkey, score in scores.items():
        if hotkey not in adjusted_scores:
            adjusted_scores[hotkey] = score
    
    return adjusted_scores


def score_batch(batch: SyntheticJobBatch) -> dict[str, float]:
    """Score a single batch with all the complex logic."""
    executor_class_weights = get_executor_class_weights()
    executor_class_synthetic_jobs = defaultdict(list)
    for synthetic_job in batch.synthetic_jobs.select_related("miner"):
        if synthetic_job.executor_class in executor_class_weights:
            executor_class = ExecutorClass(synthetic_job.executor_class)
            executor_class_synthetic_jobs[executor_class].append(synthetic_job)

    batch_organic_jobs = OrganicJob.objects.select_related("miner").filter(
        block__gte=batch.cycle.start,
        block__lt=batch.cycle.stop,
        cheated=False,
        status=OrganicJob.Status.COMPLETED,
        on_trusted_miner=False,
    )
    executor_class_organic_jobs = defaultdict(list)
    for organic_job in batch_organic_jobs:
        if organic_job.executor_class in executor_class_weights:
            executor_class = ExecutorClass(organic_job.executor_class)
            executor_class_organic_jobs[executor_class].append(organic_job)

    parameterized_horde_score: Callable[[list[float]], float] = partial(
        horde_score,
        # scaling factor for avg_score of a horde - best in range [0, 1] (0 means no effect on score)
        alpha=settings.HORDE_SCORE_AVG_PARAM,
        # sigmoid steepness param - best in range [0, 5] (0 means no effect on score)
        beta=settings.HORDE_SCORE_SIZE_PARAM,
        # horde size for 0.5 value of sigmoid - sigmoid is for 1 / horde_size
        delta=1 / settings.HORDE_SCORE_CENTRAL_SIZE_PARAM,
    )

    batch_scores: defaultdict[str, float] = defaultdict(float)
    for executor_class, executor_class_weight in executor_class_weights.items():
        # score synthetic jobs
        synthetic_jobs = executor_class_synthetic_jobs.get(executor_class, [])
        if executor_class == ExecutorClass.spin_up_4min__gpu_24gb:
            score_aggregation = parameterized_horde_score
        else:
            score_aggregation = sum
        executor_class_synthetic_scores = score_synthetic_jobs(
            synthetic_jobs,
            score_aggregation=score_aggregation,
        )

        # score organic jobs
        organic_jobs = executor_class_organic_jobs.get(executor_class, [])
        executor_class_organic_scores = score_organic_jobs(organic_jobs)

        # combine synthetic and organic scores
        executor_class_scores: defaultdict[str, float] = defaultdict(float)
        for hotkey, score in executor_class_synthetic_scores.items():
            executor_class_scores[hotkey] += score
        for hotkey, score in executor_class_organic_scores.items():
            executor_class_scores[hotkey] += score

        # normalize scores for executor class weight
        normalized_scores = normalize(executor_class_scores, executor_class_weight)
        for hotkey, score in normalized_scores.items():
            batch_scores[hotkey] += score

    return dict(batch_scores)


def score_batches(batches: Sequence[SyntheticJobBatch], validator_hotkey: str) -> dict[str, float]:
    """
    Score multiple batches and apply decoupled dancing.
    
    This is the main entry point for scoring multiple batches.
    """
    hotkeys_scores: defaultdict[str, float] = defaultdict(float)
    for batch in batches:
        batch_scores = score_batch(batch)
        for hotkey, score in batch_scores.items():
            hotkeys_scores[hotkey] += score
    
    # Apply decoupled dancing weight adjustments
    hotkeys_scores = apply_decoupled_dancing_weights(hotkeys_scores, batches, validator_hotkey)
    
    return dict(hotkeys_scores) 