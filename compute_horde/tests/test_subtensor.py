import pytest

from compute_horde.subtensor import (
    get_cycle_containing_block,
    get_epoch_containing_block,
)


@pytest.mark.parametrize(
    ("netuid", "block", "expected_epoch"),
    [
        # netuid == 0
        (0, 25, range(-2, 359)),
        (0, 359, range(-2, 359)),
        (0, 360, range(359, 720)),
        (0, 720, range(359, 720)),
        (0, 721, range(720, 1081)),
        # netuid == 12
        (12, 25, range(-14, 347)),
        (12, 347, range(-14, 347)),
        (12, 348, range(347, 708)),
        (12, 708, range(347, 708)),
        (12, 709, range(708, 1069)),
        (12, 1100, range(1069, 1430)),
    ],
)
def test__get_epoch_containing_block(netuid, block, expected_epoch):
    assert (
        get_epoch_containing_block(block=block, netuid=netuid) == expected_epoch
    ), f"block: {block}, netuid: {netuid}, expected: {expected_epoch}"


@pytest.mark.parametrize(
    ("netuid", "block", "expected_cycle"),
    [
        # netuid == 0
        (0, 25, range(-2, 359 + 361)),
        (0, 359, range(-2, 359 + 361)),
        (0, 360, range(359 - 361, 720)),
        (0, 720, range(359 - 361, 720)),
        (0, 721, range(720, 1081 + 361)),
        # netuid == 12
        (12, 25, range(-14, 347 + 361)),
        (12, 347, range(-14, 347 + 361)),
        (12, 348, range(347 - 361, 708)),
        (12, 708, range(347 - 361, 708)),
        (12, 709, range(708, 1069 + 361)),
        (12, 1100, range(1069 - 361, 1430)),
    ],
)
def test__get_cycle_containing_block(netuid, block, expected_cycle):
    assert (
        get_cycle_containing_block(block=block, netuid=netuid) == expected_cycle
    ), f"block: {block}, netuid: {netuid}, expected: {expected_cycle}"
