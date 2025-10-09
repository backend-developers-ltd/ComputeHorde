import pytest

from compute_horde_validator.validator.routing.utils import weighted_shuffle


@pytest.mark.parametrize(
    "scores",
    [
        # doesn't explode with empty input
        {},
        # doesn't explode with single item
        {"miner1": 0},
        # handles all items having the same score - all zero
        {"miner1": 0, "miner2": 0, "miner3": 0},
        # handles all items having the same score - all negative
        {"miner1": -1, "miner2": -1, "miner3": -1},
        # handles all items having the same score - all positive
        {"miner1": 1, "miner2": 1, "miner3": 1},
        # doesn't lose items when there are duplicate scores
        {
            "positive": 10,
            "another positive": 10,
            "zero": 0,
            "another zero": 0,
            "negative": -2,
            "another negative": -2,
            "huge positive score": 50000000,
            "huge positive score again": 50000000,
            "huge negative score": -50000000,
            "huge negative score again": -50000000,
            "tiny positive score": 0.00000001,
            "tiny positive score again": 0.00000001,
            "tiny negative score": -0.00000001,
            "tiny negative score again": -0.00000001,
        },
    ],
)
@pytest.mark.parametrize("center", [-100000, -0.001, 0.001, 100000])
@pytest.mark.parametrize("steepness", [-100000, -0.001, 0, 0.001, 100000])
def test_weighted_shuffle_edge_cases(scores, center, steepness):
    """
    Simple check for some edge cases - only make sure the function works, doesn't explode with numerical or precision
    errors and returns all the expected items. The order is not checked for - these ranges of values are too big, and
    weights far from the center are essentially random.
    """
    items = list(scores.keys())
    weights = [scores[item] for item in items]
    result = weighted_shuffle(items, weights, center, steepness)
    assert set(result) == set(scores.keys())


def mean_shuffled_indices(
    iterations: int,
    weights: list[float],
    center: float,
    steepness: float,
) -> list[float]:
    position_sums = [0.0] * len(weights)
    items = list(range(len(weights)))

    for _ in range(iterations):
        shuffled = weighted_shuffle(
            items=items, weights=weights, center=center, steepness=steepness
        )
        for index_after_shuffling, item in enumerate(shuffled):
            position_sums[item] += index_after_shuffling

    return [total / iterations for total in position_sums]


def test_weighted_shuffle_averages_out_predictably():
    """
    This test is mostly a check that the shuffling function works as intended - biases by weights.

    Assuming:
    - Order of weights at the input is randomized
    - Shuffling prefers higher weights first
    - We perform a lot of shuffles and average out the indices at which each weight appears
    Then we can sort the weights by their average indices -> they should end up in descending order.
    This requires that the chosen values are nicely behaved - weights are far apart from each other and not too far
    from the center of the sigmoid. Also, the sigmoid must not be too steep. This way each weight is assigned a
    probability of being selected distinct enough from all others that after many iterations we can tell them apart.

    If this breaks because an item is out of order, congratulations. You won the lottery.
    """
    # Random order of weights on the input to make sure we're not just getting back the same order.
    # Some repetitions to make sure we're handling multiple instances of the same weight.
    weights = [10, -10, 7, 3, -7, 0, 5, -3, -5, 0, 7, -3]
    mean_indices = mean_shuffled_indices(10000, weights, center=-5, steepness=1)
    weights_descending = list(sorted(weights, reverse=True))
    sorted_by_mean_position = [
        weights[idx] for idx in sorted(range(len(weights)), key=lambda i: mean_indices[i])
    ]

    assert weights_descending == sorted_by_mean_position
