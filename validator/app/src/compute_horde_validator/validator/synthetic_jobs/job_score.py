import numpy as np
from django.conf import settings
from numpy.typing import NDArray


def _sigmoid(x, beta, delta):
    return 1 / (1 + np.exp(beta * (-x + delta)))


def _reversed_sigmoid(x, beta, delta):
    return _sigmoid(-x, beta=beta, delta=-delta)


def _horde_score(benchmarks, alpha=0, beta=0, delta=0):
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
    scaled_inverted_n = _reversed_sigmoid(inverted_n, beta=beta, delta=delta)
    scaled_avg_benchmark = avg_benchmark**alpha
    return scaled_avg_benchmark * sum_agent * scaled_inverted_n


def compute_weights(
    multi_batch_scores: list[dict[int, list]], uids: NDArray[np.int64]
) -> NDArray[np.float32] | None:
    # scaling factor for avg_score of a horde - best in range [0, 1] (0 means no effect on score)
    alpha = settings.HORDE_SCORE_AVG_PARAM
    # sigmoid steepness param - best in range [0, 5] (0 means no effect on score)
    beta = settings.HORDE_SCORE_SIZE_PARAM
    # horde size for 0.5 value of sigmoid - sigmoid is for 1 / horde_size
    central_horde_size = settings.HORDE_SCORE_CENTRAL_SIZE_PARAM
    delta = 1 / central_horde_size

    score_per_uid = {}
    for batch_scores in multi_batch_scores:
        for uid, uid_batch_scores in batch_scores.items():
            uid_horde_score = _horde_score(uid_batch_scores, alpha=alpha, beta=beta, delta=delta)
            score_per_uid[uid] = score_per_uid.get(uid, 0) + uid_horde_score
    if not score_per_uid:
        return None

    weights = np.zeros(len(uids), dtype=np.float32)
    for i, uid in enumerate(uids):
        weights[i] = score_per_uid.get(uid, 0)

    return weights
