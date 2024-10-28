import logging
from collections import defaultdict
from collections.abc import Callable, Sequence
from functools import partial

import numpy as np
from compute_horde.executor_class import ExecutorClass
from django.conf import settings

from .dynamic_config import aget_config, aget_weights_version, get_executor_class_weights
from .models import SyntheticJob, SyntheticJobBatch

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

    By default scores are proportional to horde "strength" - having 10 executors would have the same
    score as separate 10 single executor miners. Subnet owner can control significance of defined features:

    alpha - controls significance of average score, so smaller horde can have higher score if executors are stronger;
            best values are from range [0, 1], with 0 meaning no effect
    beta - controls sigmoid function steepness; sigmoid function is over `-(1 / horde_size)`, so larger hordes can be
           more significant than smaller ones, even if summary strength of a horde is the same;
           best values are from range [0,5] (or more, but higher values does not change sigmoid steepnes much),
           with 0 meaning no effect
    delta - controls where sigmoid function has 0.5 value allowing for better control over effect of beta param;
            best values are from range [0, 1]
    """
    sum_agent = sum(benchmarks)
    inverted_n = 1 / len(benchmarks)
    avg_benchmark = sum_agent * inverted_n
    scaled_inverted_n = reversed_sigmoid(inverted_n, beta=10**beta, delta=delta)
    scaled_avg_benchmark = float(avg_benchmark**alpha)
    return scaled_avg_benchmark * sum_agent * scaled_inverted_n


def score_jobs(
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


def score_batch(batch: SyntheticJobBatch) -> dict[str, float]:
    _prev_batch = SyntheticJobBatch.objects.order_by("-id").filter(id__lt=batch.id).first()

    executor_class_weights = get_executor_class_weights()
    executor_class_jobs = defaultdict(list)
    for job in batch.synthetic_jobs.select_related("miner"):
        if job.executor_class in executor_class_weights:
            executor_class = ExecutorClass(job.executor_class)
            executor_class_jobs[executor_class].append(job)

    parametriezed_horde_score: Callable[[list[float]], float] = partial(
        horde_score,
        # scaling factor for avg_score of a horde - best in range [0, 1] (0 means no effect on score)
        alpha=settings.HORDE_SCORE_AVG_PARAM,
        # sigmoid steepnes param - best in range [0, 5] (0 means no effect on score)
        beta=settings.HORDE_SCORE_SIZE_PARAM,
        # horde size for 0.5 value of sigmoid - sigmoid is for 1 / horde_size
        delta=1 / settings.HORDE_SCORE_CENTRAL_SIZE_PARAM,
    )

    batch_scores: defaultdict[str, float] = defaultdict(float)
    for executor_class, jobs in executor_class_jobs.items():
        executor_class_weight = executor_class_weights[executor_class]
        if executor_class == ExecutorClass.spin_up_4min__gpu_24gb:
            score_aggregation = parametriezed_horde_score
        else:
            score_aggregation = sum

        executors_class_scores = score_jobs(
            jobs,
            score_aggregation=score_aggregation,
            normalization_weight=executor_class_weight,
        )
        for hotkey, score in executors_class_scores.items():
            batch_scores[hotkey] += score
    return dict(batch_scores)


def score_batches(batches: Sequence[SyntheticJobBatch]) -> dict[str, float]:
    hotkeys_scores: defaultdict[str, float] = defaultdict(float)
    for batch in batches:
        batch_scores = score_batch(batch)
        for hotkey, score in batch_scores.items():
            hotkeys_scores[hotkey] += score
    return dict(hotkeys_scores)


def get_manifest_multiplier_v2(
    previous_online_executors: int | None,
    current_online_executors: int,
    weights_version: int,
    multiplier: float,
    ratio_threshold: float,
) -> float:
    # weights version 0, 1, 2 does not get any dancing bonus
    if weights_version < 3:
        return 1

    # first batch of a miner gets bonus
    if previous_online_executors is None:
        return multiplier

    low, high = sorted([previous_online_executors, current_online_executors])
    # low can be 0 if previous_online_executors == 0, but we make it that way to
    # make this function correct for any kind of input
    if low == 0 or high / low >= ratio_threshold:
        return multiplier

    return 1


async def get_manifest_multiplier(
    previous_online_executors: int | None, current_online_executors: int
) -> float | None:
    weights_version = await aget_weights_version()
    multiplier = None
    if weights_version >= 2:
        if previous_online_executors is None:
            multiplier = await aget_config("DYNAMIC_MANIFEST_SCORE_MULTIPLIER")
        else:
            low, high = sorted([previous_online_executors, current_online_executors])
            # low can be 0 if previous_online_executors == 0, but we make it that way to
            # make this function correct for any kind of input
            threshold = await aget_config("DYNAMIC_MANIFEST_DANCE_RATIO_THRESHOLD")
            if low == 0 or high / low >= threshold:
                multiplier = await aget_config("DYNAMIC_MANIFEST_SCORE_MULTIPLIER")
    return multiplier
