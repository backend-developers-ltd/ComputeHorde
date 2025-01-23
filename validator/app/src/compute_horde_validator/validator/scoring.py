import logging
from collections import defaultdict
from collections.abc import Callable, Sequence
from functools import partial

import numpy as np
from compute_horde.executor_class import ExecutorClass
from constance import config
from django.conf import settings
from django.db.models import Count

from .dynamic_config import get_executor_class_weights, get_weights_version
from .models import Cycle, OrganicJob, SyntheticJob, SyntheticJobBatch

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
    normalization_weight: float = 1,
) -> dict[str, float]:
    batch_scores = defaultdict(list)
    score_per_hotkey = {}
    for job in jobs:
        hotkey = job.miner.hotkey
        batch_scores[hotkey].append(job.score)
    for hotkey, hotkey_batch_scores in batch_scores.items():
        score_per_hotkey[hotkey] = score_aggregation(hotkey_batch_scores)
    return normalize(score_per_hotkey, weight=normalization_weight)


def score_organic_jobs(cycle: Cycle | None) -> dict[str, float]:
    if not cycle:
        return {}

    batch_scores: defaultdict[str, float] = defaultdict(float)
    organic_job_counts = (
        OrganicJob.objects.filter(
            block__gte=cycle.start,
            block__lt=cycle.stop,
            status=OrganicJob.Status.COMPLETED,
        )
        .values("miner__hotkey")
        .annotate(count=Count("*"))
    )
    score = config.DYNAMIC_ORGANIC_JOB_SCORE
    limit = config.DYNAMIC_SCORE_ORGANIC_JOBS_LIMIT
    for organic_job_count in organic_job_counts:
        if limit < 0:
            count = organic_job_count["count"]
        else:
            count = min(limit, organic_job_count["count"])
        hotkey = organic_job_count["miner__hotkey"]
        batch_scores[hotkey] += count * score

    return batch_scores


def get_base_synthetic_scores(batch: SyntheticJobBatch | None) -> dict[str, float]:
    if not batch:
        return {}

    executor_class_weights = get_executor_class_weights()
    executor_class_jobs = defaultdict(list)
    rejected_jobs = []
    for job in batch.synthetic_jobs.select_related("miner"):
        if job.status == "PROPERLY_REJECTED":  # FIXME: update with proper value
            rejected_jobs.append(job)
        if job.executor_class in executor_class_weights:
            executor_class = ExecutorClass(job.executor_class)
            executor_class_jobs[executor_class].append(job)

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
    for executor_class, jobs in executor_class_jobs.items():
        executor_class_weight = executor_class_weights[executor_class]
        if executor_class == ExecutorClass.spin_up_4min__gpu_24gb:
            score_aggregation = parameterized_horde_score
        else:
            score_aggregation = sum

        executors_class_scores = score_synthetic_jobs(
            jobs,
            score_aggregation=score_aggregation,
            normalization_weight=executor_class_weight,
        )
        for hotkey, score in executors_class_scores.items():
            batch_scores[hotkey] += score

    # TODO: Properly rejected jobs should have their scores set in batch_run.
    #       This code block will be unnecessary when scoring is implemented there.
    rejected_score = config.DYNAMIC_REJECTED_SYNTHETIC_JOB_SCORE
    for job in rejected_jobs:
        batch_scores[job.miner.hotkey] += rejected_score

    return batch_scores


def score_batch(batch: SyntheticJobBatch) -> dict[str, float]:
    previous_batch = SyntheticJobBatch.objects.order_by("-id").exclude(id=batch.id).first()
    previous_batch_scores = get_base_synthetic_scores(previous_batch)
    batch_scores = get_base_synthetic_scores(batch)

    manifest_multipliers = {}
    for hotkey, current_base_synthetic_score in batch_scores.items():
        previous_base_synthetic_score = previous_batch_scores.get(hotkey)
        manifest_multipliers[hotkey] = get_manifest_multiplier(
            previous_base_synthetic_score, current_base_synthetic_score
        )

    organic_job_scores = score_organic_jobs(batch.cycle)
    for hotkey, score in organic_job_scores.items():
        batch_scores[hotkey] += score

    for hotkey in batch_scores:
        batch_scores[hotkey] *= manifest_multipliers[hotkey]

    return dict(batch_scores)


def score_batches(batches: Sequence[SyntheticJobBatch]) -> dict[str, float]:
    hotkeys_scores: defaultdict[str, float] = defaultdict(float)
    for batch in batches:
        batch_scores = score_batch(batch)
        for hotkey, score in batch_scores.items():
            hotkeys_scores[hotkey] += score
    return dict(hotkeys_scores)


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
