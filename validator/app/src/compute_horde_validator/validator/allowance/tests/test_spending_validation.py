import datetime
from unittest import mock

import pytest
from compute_horde_core.executor_class import ExecutorClass

from compute_horde_validator.validator.allowance.types import (
    CannotSpend,
    ErrorWhileSpending,
    SpendingDetails,
)
from compute_horde_validator.validator.allowance.utils.spending import (
    SpendingBookkeeperBase,
    Triplet,
)


class MockSpendingBookkeeper(SpendingBookkeeperBase):
    """
    In this file the ".spend()" method is mostly being tested:
    - validation of spending
    - calling _register_transaction
    """

    def __init__(self):
        super().__init__()
        self._valid_allowances = {}
        self._spent_blocks = set()
        self._invalidated_blocks = set()
        self._block_at_spending_time = None

    def _get_blocks_allowances(self, triplet, blocks):
        return self._valid_allowances

    def _filter_for_spent_blocks(self, triplet, blocks):
        return self._spent_blocks

    def _filter_for_invalidated_blocks(self, triplet, blocks, at_block):
        return self._invalidated_blocks

    def _get_block_at_time(self, time):
        return self._block_at_spending_time

    def _register_transaction(self, triplet, blocks):
        # Mock this to check what transactions got registered in the bookkeeper
        pass


@pytest.fixture
def bookkeeper():
    return MockSpendingBookkeeper()


@pytest.fixture
def triplet():
    return Triplet("validator", "miner", ExecutorClass.always_on__gpu_24gb)


@pytest.fixture
def spend_time():
    return datetime.datetime(2024, 1, 1, 12, 0)


@pytest.fixture(autouse=True)
def mock_block_expiration():
    with mock.patch(
        "compute_horde_validator.validator.allowance.utils.spending.settings.BLOCK_EXPIRY", 3
    ):
        yield


@pytest.mark.django_db
def test_spend__successful(bookkeeper, triplet, spend_time):
    # Two valid blocks (1: 10.0, 2: 5.0) with 15.0 total allowance for 12.0 spend
    bookkeeper._valid_allowances = {1: 10.0, 2: 5.0}
    bookkeeper._block_at_spending_time = 4

    with mock.patch.object(bookkeeper, "_register_transaction") as mock_register_transaction:
        result = bookkeeper.spend(triplet, spend_time, [1, 2], 12.0)

    assert result == SpendingDetails(
        requested_amount=12.0,
        offered_blocks=[1, 2],
        spendable_amount=15.0,
        spent_blocks=[1, 2],
        outside_range_blocks=[],
        double_spent_blocks=[],
        invalidated_blocks=[],
    )
    mock_register_transaction.assert_has_calls([mock.call(triplet, [1, 2])])


@pytest.mark.django_db
def test_spend__blocks_outside_range(bookkeeper, triplet, spend_time):
    # Block 1 is valid (5.0 allowance), block 50 is outside allowed range (1-4)
    # Spending is still good, the one block is enough
    # (Assuming block_expiry=3)
    bookkeeper._valid_allowances = {1: 5.0}
    bookkeeper._block_at_spending_time = 4

    with mock.patch.object(bookkeeper, "_register_transaction") as mock_register_transaction:
        result = bookkeeper.spend(triplet, spend_time, [1, 50], 3.0)

    assert result == SpendingDetails(
        requested_amount=3.0,
        offered_blocks=[1, 50],
        spendable_amount=5.0,
        spent_blocks=[1],
        outside_range_blocks=[50],
        double_spent_blocks=[],
        invalidated_blocks=[],
    )
    mock_register_transaction.assert_has_calls([mock.call(triplet, [1])])


@pytest.mark.django_db
def test_spend__double_spent_blocks(bookkeeper, triplet, spend_time):
    # Block 1 is already spent, block 2 has 5.0 allowance (insufficient for 12.0 spend)
    bookkeeper._valid_allowances = {2: 5.0}
    bookkeeper._spent_blocks = {1}
    bookkeeper._block_at_spending_time = 4

    with (
        pytest.raises(CannotSpend) as cannot_spend,
        mock.patch.object(bookkeeper, "_register_transaction") as mock_register_transaction,
    ):
        bookkeeper.spend(triplet, spend_time, [1, 2], 12.0)

    assert cannot_spend.value.details == SpendingDetails(
        requested_amount=12.0,
        offered_blocks=[1, 2],
        spendable_amount=5.0,
        spent_blocks=[],
        outside_range_blocks=[],
        double_spent_blocks=[1],
        invalidated_blocks=[],
    )

    mock_register_transaction.assert_has_calls([])


@pytest.mark.django_db
def test_spend__invalidated_blocks(bookkeeper, triplet, spend_time):
    # Block 2 is invalidated, block 1 has 10.0 allowance (insufficient for 12.0 spend)
    bookkeeper._valid_allowances = {1: 10.0}
    bookkeeper._invalidated_blocks = {2}
    bookkeeper._block_at_spending_time = 4

    with (
        pytest.raises(CannotSpend) as cannot_spend,
        mock.patch.object(bookkeeper, "_register_transaction") as mock_register_transaction,
    ):
        bookkeeper.spend(triplet, spend_time, [1, 2], 12.0)

    assert cannot_spend.value.details == SpendingDetails(
        requested_amount=12.0,
        offered_blocks=[1, 2],
        spendable_amount=10.0,
        spent_blocks=[],
        outside_range_blocks=[],
        double_spent_blocks=[],
        invalidated_blocks=[2],
    )

    mock_register_transaction.assert_has_calls([])


@pytest.mark.django_db
def test_spend__insufficient_allowance(bookkeeper, triplet, spend_time):
    # Two valid blocks with 8.0 total allowance (5.0 + 3.0), insufficient for 10.0 spend
    bookkeeper._valid_allowances = {1: 5.0, 2: 3.0}
    bookkeeper._block_at_spending_time = 4

    with (
        pytest.raises(CannotSpend) as cannot_spend,
        mock.patch.object(bookkeeper, "_register_transaction") as mock_register_transaction,
    ):
        bookkeeper.spend(triplet, spend_time, [1, 2], 10.0)

    assert cannot_spend.value.details == SpendingDetails(
        requested_amount=10.0,
        offered_blocks=[1, 2],
        spendable_amount=8.0,
        spent_blocks=[],
        outside_range_blocks=[],
        double_spent_blocks=[],
        invalidated_blocks=[],
    )

    mock_register_transaction.assert_has_calls([])


@pytest.mark.django_db
def test_spend__no_block_at_time(bookkeeper, triplet, spend_time):
    # Block 1 has sufficient allowance, but no block exists at submission time
    bookkeeper._valid_allowances = {1: 10.0}

    with (
        pytest.raises(ErrorWhileSpending) as cannot_spend,
        mock.patch.object(bookkeeper, "_register_transaction") as mock_register_transaction,
    ):
        bookkeeper.spend(triplet, spend_time, [1], 5.0)

    assert "Cannot find block at job submission time" in cannot_spend.value.args[0]
    mock_register_transaction.assert_has_calls([])


@pytest.mark.django_db
def test_spend__partial_success(bookkeeper, triplet, spend_time):
    # Blocks 1,2 are valid (15.0 total allowance), block 50 is outside range (1-4)
    bookkeeper._valid_allowances = {1: 10.0, 2: 5.0}
    bookkeeper._block_at_spending_time = 4

    with mock.patch.object(bookkeeper, "_register_transaction") as mock_register_transaction:
        result = bookkeeper.spend(triplet, spend_time, [1, 2, 50], 12.0)

    assert result == SpendingDetails(
        requested_amount=12.0,
        offered_blocks=[1, 2, 50],
        spendable_amount=15.0,
        spent_blocks=[1, 2],
        outside_range_blocks=[50],
        double_spent_blocks=[],
        invalidated_blocks=[],
    )
    mock_register_transaction.assert_has_calls([mock.call(triplet, [1, 2])])


@pytest.mark.django_db
def test_spend__multiple_issues__fails_when_not_enough(bookkeeper, triplet, spend_time):
    # Setup blocks with different issues:
    # - Block 1: already spent (5.0 allowance, but unavailable)
    # - Block 2: invalidated (3.0 allowance, but unavailable)
    # - Block 3: valid with 6.0 allowance (insufficient for 10.0 spend)
    # - Block 50: outside allowed range (1-4)
    bookkeeper._spent_blocks = {1}
    bookkeeper._invalidated_blocks = {2}
    bookkeeper._block_at_spending_time = 4
    bookkeeper._valid_allowances = {3: 6.0}

    with (
        pytest.raises(CannotSpend) as cannot_spend,
        mock.patch.object(bookkeeper, "_register_transaction") as mock_register_transaction,
    ):
        bookkeeper.spend(triplet, spend_time, [1, 2, 3, 50], 10.0)

    assert cannot_spend.value.details == SpendingDetails(
        requested_amount=10.0,
        offered_blocks=[1, 2, 3, 50],
        spendable_amount=6.0,
        spent_blocks=[],
        outside_range_blocks=[50],
        double_spent_blocks=[1],
        invalidated_blocks=[2],
    )
    mock_register_transaction.assert_has_calls([])


@pytest.mark.django_db
def test_spend__multiple_issues__succeeds_anyway(bookkeeper, triplet, spend_time):
    # Same as above, except the single valid block has enough allowance for the spend
    bookkeeper._spent_blocks = {1}
    bookkeeper._invalidated_blocks = {2}
    bookkeeper._block_at_spending_time = 4
    bookkeeper._valid_allowances = {3: 60.0}

    with mock.patch.object(bookkeeper, "_register_transaction") as mock_register_transaction:
        result = bookkeeper.spend(triplet, spend_time, [1, 2, 3, 50], 10.0)

    assert result == SpendingDetails(
        requested_amount=10.0,
        offered_blocks=[1, 2, 3, 50],
        spendable_amount=60.0,
        spent_blocks=[3],
        outside_range_blocks=[50],
        double_spent_blocks=[1],
        invalidated_blocks=[2],
    )
    mock_register_transaction.assert_has_calls([])


@pytest.mark.django_db
def test_spend__empty_payment_blocks(bookkeeper, triplet, spend_time):
    # No blocks provided, 0.0 allowance for 10.0 spend requirement
    bookkeeper._block_at_spending_time = 4

    with (
        pytest.raises(CannotSpend) as cannot_spend,
        mock.patch.object(bookkeeper, "_register_transaction") as mock_register_transaction,
    ):
        bookkeeper.spend(triplet, spend_time, [], 10.0)

    assert cannot_spend.value.details == SpendingDetails(
        requested_amount=10.0,
        offered_blocks=[],
        spendable_amount=0.0,
        spent_blocks=[],
        outside_range_blocks=[],
        double_spent_blocks=[],
        invalidated_blocks=[],
    )
    mock_register_transaction.assert_has_calls([])


@pytest.mark.django_db
def test_spend__duplicate_blocks_deduplicated(bookkeeper, triplet, spend_time):
    # Duplicate blocks [1,1,2,2] should be deduplicated to [1,2] with 15.0 total allowance
    bookkeeper._valid_allowances = {1: 10.0, 2: 5.0}
    bookkeeper._block_at_spending_time = 4

    with mock.patch.object(bookkeeper, "_register_transaction") as mock_register_transaction:
        result = bookkeeper.spend(triplet, spend_time, [1, 1, 2, 2], 12.0)

    assert result == SpendingDetails(
        requested_amount=12.0,
        offered_blocks=[1, 1, 2, 2],
        spendable_amount=15.0,
        spent_blocks=[1, 2],
        outside_range_blocks=[],
        double_spent_blocks=[],
        invalidated_blocks=[],
    )
    mock_register_transaction.assert_called_once()
    registered_blocks = mock_register_transaction.call_args[0][1]
    assert len(registered_blocks) == 2  # Should register each block only once
