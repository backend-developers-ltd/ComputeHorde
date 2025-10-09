from typing import Any, TypeVar

import numpy as np
from numpy.typing import NDArray

T = TypeVar("T")


def sigmoid(x: NDArray[np.floating[Any]], k: float, c: float) -> NDArray[np.floating[Any]]:
    """
    Sigmoid for mapping raw values to probabilities: (-inf,inf) -> (0,1)

    Args:
        x: Input array of values to transform
        k: Sigmoid steepness parameter
        c: Center point of the sigmoid
    """
    # Clipping `z` here is necessary, because np.exp() behaves only within ~[-500, 500] range. This doesn't matter in
    # practice, because values at -500 and 500 are essentially 0 and 1 anyway.
    # (Note - this is NOT equivalent to clipping the input weights to [-500,500])
    z = -k * (x - c)
    z = np.clip(z, -500, 500)
    return 1 / (1 + np.exp(z))  # type: ignore[no-any-return]


def weighted_shuffle(
    items: list[T], weights: list[float], center: float, steepness: float
) -> list[T]:
    """
    Shuffles items by weighted random selection based on provided weights.
    Returns items in a random order, but higher weighted items are biased **towards the beginning**.
    Monkey brain visualization of the sigmoid shape, params and the resulting probs:
    https://www.desmos.com/calculator/kg3oquajgr

    Args:
        items: List of items to shuffle
        weights: List of weights corresponding to each item
        center: Center point of the sigmoid. See the visualization above.
        steepness: Sigmoid steepness parameter. See the visualization above.

    """
    if len(items) != len(weights):
        raise ValueError("Items and weights must have the same length")

    if len(items) == 0:
        return []

    weights_arr = np.array(weights, dtype=float)

    # Sigmoid normalization.
    # k=s/c makes the sigmoid curve steeper or flatter depending on the steepness parameter, but keeps the relative
    # shape of the sigmoid curve constant regardless of c.
    probs = sigmoid(weights_arr, k=steepness / -center, c=center)

    # Clip to [epsilon, 1-epsilon] to avoid exact 0/1 weights, which would trip up np.random.choice
    epsilon = 1e-6
    probs = np.clip(probs, epsilon, 1 - epsilon)

    # Renormalize so that sum=1, or np.random.choice will complain
    probs /= probs.sum()

    # Weighted shuffle!
    indices = np.arange(len(items))
    shuffled_indices = np.random.choice(indices, size=len(items), replace=False, p=probs)
    return [items[i] for i in shuffled_indices]
