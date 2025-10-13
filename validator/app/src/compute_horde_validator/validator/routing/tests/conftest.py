from unittest.mock import patch

import pytest


@pytest.fixture
def disable_miner_shuffling():
    """
    Instead of shuffling miners, make the tests deterministic by returning the most probable outcome.
    """

    def sorted_by_weight(items, weights, center, steepness):
        # Must be sorted ascending by weight
        return (
            [item for _, item in sorted(zip(weights, items), key=lambda x: x[0], reverse=True)],
            list(reversed(range(len(items)))),
        )

    with patch(
        "compute_horde_validator.validator.routing.default.weighted_shuffle",
        side_effect=sorted_by_weight,
    ):
        yield
