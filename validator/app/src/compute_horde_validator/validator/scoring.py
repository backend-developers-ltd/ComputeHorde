import logging
from collections import defaultdict
from collections.abc import Callable, Sequence
from functools import partial

import numpy as np
from compute_horde.executor_class import ExecutorClass
from constance import config
from django.conf import settings

from .dynamic_config import get_executor_class_weights, get_weights_version
from .models import MinerManifest, OrganicJob, SyntheticJob, SyntheticJobBatch

logger = logging.getLogger(__name__)


def normalize(scores: dict[str, float], weight: float = 1) -> dict[str, float]:
    total = sum(scores.values())
    if total == 0:
        return scores
    return {hotkey: weight * score / total for hotkey, score in scores.items()}


def sigmoid(x: float, beta: float, delta: float) -> float:
    return 1 / (1 + float(np.exp(beta * (-x + delta))))


def reversed_sigmoid(x: float, beta: float, delta: float) -> float:
    return sigmoid(-x, beta=beta, delta=-delta)


def horde_score(
    benchmarks: list[float], alpha: float = 0, beta: float = 0, delta: float = 0
) -> float:
    """Proportionally scores horde benchmarks allowing increasing significance for chosen features

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


def score_synthetic_jobs(
    jobs: Sequence[SyntheticJob],
    score_aggregation: Callable[[list[float]], float] = sum,
) -> dict[str, float]:
    batch_scores = defaultdict(list)
    score_per_hotkey = {}
    for job in jobs:
        hotkey = job.miner.hotkey
        batch_scores[hotkey].append(job.score)
    for hotkey, hotkey_batch_scores in batch_scores.items():
        score_per_hotkey[hotkey] = score_aggregation(hotkey_batch_scores)
    return score_per_hotkey


def score_organic_jobs(jobs: Sequence[OrganicJob]) -> dict[str, float]:
    batch_scores: defaultdict[str, float] = defaultdict(float)
    score = config.DYNAMIC_ORGANIC_JOB_SCORE
    limit = config.DYNAMIC_SCORE_ORGANIC_JOBS_LIMIT

    for job in jobs:
        batch_scores[job.miner.hotkey] += score

    if limit >= 0:
        for hotkey, score in batch_scores.items():
            batch_scores[hotkey] = min(score, limit * score)

    return batch_scores


def score_batch(batch: SyntheticJobBatch) -> dict[str, float]:
    executor_class_weights = get_executor_class_weights()
    executor_class_synthetic_jobs = defaultdict(list)
    for synthetic_job in batch.synthetic_jobs.select_related("miner"):
        if synthetic_job.executor_class in executor_class_weights:
            executor_class = ExecutorClass(synthetic_job.executor_class)
            executor_class_synthetic_jobs[executor_class].append(synthetic_job)

    if batch.cycle is not None:
        batch_organic_jobs = OrganicJob.objects.select_related("miner").filter(
            block__gte=batch.cycle.start,
            block__lt=batch.cycle.stop,
            status=OrganicJob.Status.COMPLETED,
        )
    else:
        batch_organic_jobs = OrganicJob.objects.none()

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

    # apply manifest bonus
    previous_batch = get_previous_batch(batch)
    previous_executor_counts = get_executor_counts(previous_batch)
    current_executor_counts = get_executor_counts(batch)
    for hotkey in batch_scores:
        previous_base_synthetic_score: float | None = None
        if hotkey in previous_executor_counts:
            previous_base_synthetic_score = get_base_synthetic_score(
                previous_executor_counts[hotkey],
                executor_class_weights,
            )
        current_base_synthetic_score = get_base_synthetic_score(
            current_executor_counts.get(hotkey, {}),
            executor_class_weights,
        )
        multiplier = get_manifest_multiplier(
            previous_base_synthetic_score,
            current_base_synthetic_score,
        )
        batch_scores[hotkey] *= multiplier

    return dict(batch_scores)


def score_batches(batches: Sequence[SyntheticJobBatch]) -> dict[str, float]:
    hotkeys_scores: defaultdict[str, float] = defaultdict(float)
    for batch in batches:
        batch_scores = score_batch(batch)
        for hotkey, score in batch_scores.items():
            hotkeys_scores[hotkey] += score
    return dict(hotkeys_scores)


def get_previous_batch(current_batch: SyntheticJobBatch) -> SyntheticJobBatch | None:
    """Get the synthetic job batch of the previous cycle of current_batch"""
    if current_batch.cycle is None:
        return None
    block_in_prev_cycle = current_batch.cycle.start - 1
    return SyntheticJobBatch.objects.filter(
        cycle__start__lte=block_in_prev_cycle,
        cycle__stop__gt=block_in_prev_cycle,
        should_be_scored=True,
    ).first()


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
    score = 0.0
    for executor_class, weight in executor_class_weights.items():
        score += weight * executor_counts.get(executor_class, 0)
    return score


def get_manifest_multiplier(
    previous_base_synthetic_score: float | None,
    current_base_synthetic_score: float,
) -> float:
    weights_version = get_weights_version()
    multiplier = 1.0
    if weights_version >= 2:
        if previous_base_synthetic_score is None:
            multiplier = config.DYNAMIC_MANIFEST_SCORE_MULTIPLIER
        else:
            low, high = sorted([previous_base_synthetic_score, current_base_synthetic_score])
            # low can be 0 if previous_online_executors == 0, but we make it that way to
            # make this function correct for any kind of input
            threshold = config.DYNAMIC_MANIFEST_DANCE_RATIO_THRESHOLD
            if low == 0 or high / low >= threshold:
                multiplier = config.DYNAMIC_MANIFEST_SCORE_MULTIPLIER
    return multiplier
