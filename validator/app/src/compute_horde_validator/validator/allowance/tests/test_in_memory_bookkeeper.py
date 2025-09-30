import datetime

import pytest
from compute_horde_core.executor_class import ExecutorClass

from compute_horde_validator.validator.allowance.utils.spending import (
    AllowanceInfo,
    InMemorySpendingBookkeeper,
    Triplet,
)
from compute_horde_validator.validator.models import Block

"""
Tests the in-memory implementation of the bookkeeper:
- all query methods return the expected results
- _register_transaction updates internal state correctly
- doesn't confuse similar triplets
"""

pytestmark = [
    pytest.mark.django_db,
]


def _datetime_between(dt1: datetime.datetime, dt2: datetime.datetime) -> datetime.datetime:
    """Returns a datetime smack between two passed in dates."""
    delta = dt2 - dt1
    return dt1 + delta / 2


class _Blocks:
    """
    Convenience thing for building blocks:
    - blocks[2]: Block(2, datetime.datetime(2024, 1, 2, 0, 0))
    - blocks[2,]: [Block(2, datetime.datetime(2024, 1, 2, 0, 0))]
    - blocks[1:3]: [Block(1, ..., Block(2, ...), Block(3, ...)]
    - blocks[1,3]: [Block(1, ...), Block(3, ...)]
    """

    def __getitem__(self, key):
        base_datetime = datetime.datetime(2024, 1, 1, 0, 0, 0)

        def _block(block_number: int) -> Block:
            timestamp = base_datetime + datetime.timedelta(hours=block_number)
            return Block(block_number, timestamp)

        if isinstance(key, slice):
            start = key.start or 1
            stop = key.stop + 1  # Inclusive
            step = key.step or 1
            return [_block(i) for i in range(start, stop, step)]
        elif isinstance(key, tuple | list):
            return [_block(num) for num in key]
        else:
            return _block(key)


blocks = _Blocks()


@pytest.fixture(
    params=[
        [
            # Completely separate triplets
            Triplet("validator1", "miner1", ExecutorClass.always_on__gpu_24gb),
            Triplet("validator2", "miner2", ExecutorClass.spin_up_4min__gpu_24gb),
        ],
        [
            # Same validator
            Triplet("validator1", "miner1", ExecutorClass.always_on__gpu_24gb),
            Triplet("validator1", "miner2", ExecutorClass.spin_up_4min__gpu_24gb),
        ],
        [
            # Same miner
            Triplet("validator1", "miner1", ExecutorClass.always_on__gpu_24gb),
            Triplet("validator2", "miner1", ExecutorClass.spin_up_4min__gpu_24gb),
        ],
        [
            # Same executor class
            Triplet("validator1", "miner1", ExecutorClass.always_on__gpu_24gb),
            Triplet("validator2", "miner2", ExecutorClass.always_on__gpu_24gb),
        ],
        [
            # Same vali and miner (different executor class)
            Triplet("validator1", "miner1", ExecutorClass.always_on__gpu_24gb),
            Triplet("validator1", "miner1", ExecutorClass.spin_up_4min__gpu_24gb),
        ],
        [
            # Same vali and executor class (different miner)
            Triplet("validator1", "miner1", ExecutorClass.always_on__gpu_24gb),
            Triplet("validator1", "miner2", ExecutorClass.always_on__gpu_24gb),
        ],
        [
            # Same miner and executor class (different vali)
            Triplet("validator1", "miner1", ExecutorClass.always_on__gpu_24gb),
            Triplet("validator2", "miner1", ExecutorClass.always_on__gpu_24gb),
        ],
    ]
)  # type: ignore
def triplets(request) -> list[Triplet]:
    # This parametrizes all tests here so that we test against different combinations of similar triplets.
    return request.param  # type: ignore


def test_empty_allowances(triplets):
    """Empty bookkeeper returns no data for any queries."""
    bookkeeper = InMemorySpendingBookkeeper({}, [])

    for triplet in triplets:
        assert bookkeeper._get_blocks_allowances(triplet, {1, 2}) == {}
        assert bookkeeper._filter_for_spent_blocks(triplet, {1, 2}) == set()
        assert bookkeeper._filter_for_invalidated_blocks(triplet, {1, 2}, 1) == set()
        assert bookkeeper._filter_for_invalidated_blocks(triplet, {1, 2}, 2) == set()
        assert bookkeeper._filter_for_invalidated_blocks(triplet, {1, 2}, 5) == set()

    assert bookkeeper._get_block_at_time(datetime.datetime.now()) is None


@pytest.mark.parametrize(
    "triplet_idx,expected",
    [
        (0, {1: 10.0, 2: 5.0}),
        (1, {1: 3.0, 3: 7.0}),
    ],
)
def test_get_blocks_allowances(triplets, triplet_idx, expected):
    """Returns allowance amounts only for blocks that exist for the triplet."""
    allowances = {
        triplets[0]: {
            1: AllowanceInfo(10.0, None),
            2: AllowanceInfo(5.0, None),
        },
        triplets[1]: {
            1: AllowanceInfo(3.0, None),
            3: AllowanceInfo(7.0, None),
        },
    }
    bookkeeper = InMemorySpendingBookkeeper(allowances, [])
    assert bookkeeper._get_blocks_allowances(triplets[triplet_idx], {1, 2, 3}) == expected


@pytest.mark.parametrize(
    "triplet_idx,expected",
    [
        (0, {1, 2}),
        (1, {2}),
    ],
)
def test_register_transaction_and_filter_for_spent_blocks(triplets, triplet_idx, expected):
    """Tracks spent blocks per triplet after transactions are registered."""
    bookkeeper = InMemorySpendingBookkeeper({}, [])
    bookkeeper._register_transaction(triplets[0], [1, 2])
    bookkeeper._register_transaction(triplets[1], [2, 4])
    assert bookkeeper._filter_for_spent_blocks(triplets[triplet_idx], {1, 2, 3}) == expected


@pytest.mark.parametrize(
    "at_block,triplet_idx,expected",
    [
        (3, 0, set()),
        (3, 1, set()),
        (4, 0, {2}),
        (4, 1, set()),
        (5, 0, {2}),
        (5, 1, {2}),
        (6, 0, {2, 3}),
        (6, 1, {2}),
    ],
)
def test_filter_for_invalidated_blocks(triplets, at_block, triplet_idx, expected):
    """Blocks are invalidated when >= invalidated_at_block."""
    bookkeeper = InMemorySpendingBookkeeper(
        known_allowances={
            triplets[0]: {
                1: AllowanceInfo(10.0),
                2: AllowanceInfo(5.0, invalidated_at_block=4),
                3: AllowanceInfo(3.0, invalidated_at_block=6),
            },
            triplets[1]: {
                1: AllowanceInfo(3.0),
                2: AllowanceInfo(7.0, invalidated_at_block=5),
                3: AllowanceInfo(2.0),
            },
        },
        blocks=[],
    )

    result = bookkeeper._filter_for_invalidated_blocks(triplets[triplet_idx], {1, 2, 3}, at_block)
    assert result == expected


@pytest.mark.parametrize(
    "test_time,expected_block",
    [
        (blocks[1].creation_timestamp, 1),  # Right at the beginning of the first known block
        (_datetime_between(blocks[1].creation_timestamp, blocks[2].creation_timestamp), 1),
        # Middle of the first block
        (blocks[2].creation_timestamp, 2),  # Right at the beginning of the second known block
        (_datetime_between(blocks[2].creation_timestamp, blocks[3].creation_timestamp), 2),
        (_datetime_between(blocks[3].creation_timestamp, blocks[4].creation_timestamp), 3),
    ],
)
def test_block_time_lookup(test_time, expected_block):
    """Finds correct block for timestamp. Last block is boundary (excluded)."""
    bookkeeper = InMemorySpendingBookkeeper({}, blocks[1:5])
    assert bookkeeper._get_block_at_time(test_time) == expected_block


@pytest.mark.parametrize(
    "blocks,check_time",
    [
        ([], blocks[0].creation_timestamp),  # No blocks, no way to answer
        (blocks[1,], blocks[0].creation_timestamp),  # One block is not enough
        (blocks[1,], blocks[1].creation_timestamp),
        (blocks[1,], blocks[2].creation_timestamp),
        (blocks[1:3], blocks[0].creation_timestamp),  # Before range
        (blocks[1:3], blocks[3].creation_timestamp),  # Start of last block is already outside range
    ],
)
def test_block_time_lookup_fails(blocks, check_time):
    """Returns None when insufficient blocks or time is outside bounds."""
    bookkeeper = InMemorySpendingBookkeeper({}, blocks)
    assert bookkeeper._get_block_at_time(check_time) is None
