import logging
from collections import defaultdict
from collections.abc import Callable, Sequence
from functools import partial

import numpy as np
from compute_horde_core.executor_class import ExecutorClass
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


def score_batches(batches: Sequence[SyntheticJobBatch]) -> dict[str, float]:
    hotkeys_scores: defaultdict[str, float] = defaultdict(float)
    for batch in batches:
        batch_scores = score_batch(batch)
        for hotkey, score in batch_scores.items():
            hotkeys_scores[hotkey] += score
    return dict(hotkeys_scores)


def get_executor_counts(batch: SyntheticJobBatch | None) -> dict[str, dict[ExecutorClass, int]]:
    """In a given batch, get the number of online executors per miner per executor class"""
    if not batch:
        return {}

    result: dict[str, dict[ExecutorClass, int]] = defaultdict(lambda: defaultdict(int))

    for manifest in MinerManifest.objects.select_related("miner").filter(batch_id=batch.id):
        executor_class = ExecutorClass(manifest.executor_class)
        result[manifest.miner.hotkey][executor_class] += manifest.online_executor_count

    return result



