import concurrent.futures
import threading
from typing import cast

import pytest
from compute_horde_core.executor_class import ExecutorClass

from ...models.allowance.internal import AllowanceBooking, Block, BlockAllowance
from ..default import allowance
from ..tasks import evict_old_data
from ..types import (
    CannotReserveAllowanceException,
    ReservationAlreadySpent,
    ReservationNotFound,
)
from ..utils import booking as booking_module
from ..utils.supertensor import supertensor
from .mockchain import set_block_number


@pytest.mark.django_db(transaction=True)
@pytest.mark.filterwarnings("ignore::pytest.PytestUnraisableExceptionWarning")
def test_concurrent_reservations_serialized():
    # Prepare a single miner with two small allowance blocks so that only one reservation of size 2 can succeed
    miner = "concurrent_miner"

    exec_class = ExecutorClass.always_on__llm__a6000

    with set_block_number(2000):
        validator_ss58 = supertensor().wallet().get_hotkey().ss58_address

        # Create two blocks worth 1s each
        b1 = Block.objects.create(block_number=1999, creation_timestamp="2024-01-01T00:00:00Z")
        b2 = Block.objects.create(block_number=2000, creation_timestamp="2024-01-01T00:00:01Z")
        # total allowance 2 seconds
        BlockAllowance.objects.create(
            block=b1,
            allowance=1.0,
            miner_ss58=miner,
            validator_ss58=validator_ss58,
            executor_class=exec_class.value,
        )
        BlockAllowance.objects.create(
            block=b2,
            allowance=1.0,
            miner_ss58=miner,
            validator_ss58=validator_ss58,
            executor_class=exec_class.value,
        )

    # We will attempt two competing reservations each asking for 2 seconds (entire pool)
    # Two possible result tuple shapes:
    # ("ok", reservation_id:int, blocks_used:list[int]) or ("fail", available_allowance:float)
    results: list[tuple[str, int, list[int]] | tuple[str, float]] = []
    lock = threading.Lock()

    def do_reserve():
        with set_block_number(2000):
            try:
                rid, blocks_used = allowance().reserve_allowance(miner, exec_class, 2.0, 2000)
                with lock:
                    results.append(("ok", rid, blocks_used))
            except CannotReserveAllowanceException as e:
                with lock:
                    results.append(("fail", e.available_allowance_seconds))

    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as ex:
        futures = [ex.submit(do_reserve) for _ in range(2)]
        for f in futures:
            f.result()

    # Exactly one success and one failure expected
    oks = [r for r in results if r[0] == "ok"]
    fails = [r for r in results if r[0] == "fail"]
    assert len(oks) == 1, f"expected 1 successful reservation, got {results}"
    assert len(fails) == 1, f"expected 1 failed reservation, got {results}"

    # Ensure that exactly two BlockAllowances point to a single booking (the winner)
    booking_ids = list(AllowanceBooking.objects.values_list("id", flat=True))
    assert len(booking_ids) == 1, "Only one booking should exist after concurrent race"
    winner_booking_id = booking_ids[0]
    assert BlockAllowance.objects.filter(allowance_booking_id=winner_booking_id).count() == 2, (
        "Both allowances should be tied to winning booking"
    )


@pytest.mark.django_db(transaction=True)
@pytest.mark.filterwarnings("ignore::pytest.PytestUnraisableExceptionWarning")
def test_concurrent_undo_vs_spend():
    """Ensure race between undo and spend produces a consistent final state.

    Acceptable outcomes:
      A) spend first: booking remains, is_spent=True; undo raises ReservationAlreadySpent
      B) undo first: booking deleted; spend raises ReservationNotFound
    """
    miner = "race_miner"
    exec_class = ExecutorClass.always_on__llm__a6000

    with set_block_number(3000):
        validator_ss58 = supertensor().wallet().get_hotkey().ss58_address

        b = Block.objects.create(block_number=2999, creation_timestamp="2024-01-01T00:00:00Z")
        BlockAllowance.objects.create(
            block=b,
            allowance=1.0,
            miner_ss58=miner,
            validator_ss58=validator_ss58,
            executor_class=exec_class.value,
        )

    with set_block_number(3000):
        reservation_id, _ = allowance().reserve_allowance(miner, exec_class, 1.0, 3000)

    results: list[str] = []
    barrier = threading.Barrier(2)

    def do_spend():
        barrier.wait()
        try:
            allowance().spend_allowance(reservation_id)
            results.append("spend_ok")
        except ReservationAlreadySpent:
            results.append("spend_already_spent")
        except ReservationNotFound:
            results.append("spend_not_found")

    def do_undo():
        barrier.wait()
        try:
            allowance().undo_allowance_reservation(reservation_id)
            results.append("undo_ok")
        except ReservationAlreadySpent:
            results.append("undo_already_spent")
        except ReservationNotFound:
            results.append("undo_not_found")

    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as ex:
        f1 = ex.submit(do_spend)
        f2 = ex.submit(do_undo)
    f1.result()
    f2.result()

    # Inspect final state
    booking_exists = AllowanceBooking.objects.filter(id=reservation_id).exists()

    if booking_exists:
        # Must be spent state; undo should have failed with already spent
        booking = AllowanceBooking.objects.get(id=reservation_id)
        assert booking.is_spent is True
        assert any(r in ("undo_already_spent", "undo_not_found") for r in results), results
        assert "spend_ok" in results, results
    else:
        # Booking deleted by undo; spend should report not found
        assert any(r == "undo_ok" for r in results), results
        assert any(r == "spend_not_found" for r in results), results

    # In either case, consistency: allowances either all still linked (spent) or all freed (undo)
    linked = BlockAllowance.objects.filter(allowance_booking_id=reservation_id).count()
    if booking_exists:
        assert linked > 0, "Spent booking should retain linked allowances"
    else:
        assert linked == 0, "Undo should release all allowances"


@pytest.mark.django_db(transaction=True)
def test_concurrent_evict_vs_reserve():
    """Simulate race between eviction task and reservation creation.

    Goal: ensure that while a reservation booking is being created and its BlockAllowances
    are being associated, the eviction logic does not delete the freshly created booking.

    Without proper handling in reserve_allowance the following could occur:
      1. reserve_allowance creates AllowanceBooking (unlinked yet) and yields before linking blocks.
      2. eviction runs and deletes 'orphan' AllowanceBooking (no BlockAllowance references yet).
      3. reserve_allowance resumes and links BlockAllowances to a now-deleted booking id, or raises.

    This test orchestrates a timing window using an event to pause after booking creation but
    before bulk_update is applied. We monkeypatch AllowanceBooking.objects.create to insert a hook.
    """

    miner = "evict_race_miner"
    exec_class = ExecutorClass.always_on__llm__a6000

    # Prepare allowance blocks that will be within eviction threshold (far above eviction cutoff)
    with set_block_number(10_000):
        validator_ss58 = supertensor().wallet().get_hotkey().ss58_address

        b1 = Block.objects.create(block_number=9998, creation_timestamp="2024-01-01T00:00:00Z")
        b2 = Block.objects.create(block_number=9999, creation_timestamp="2024-01-01T00:00:01Z")
        BlockAllowance.objects.create(
            block=b1,
            allowance=1.0,
            miner_ss58=miner,
            validator_ss58=validator_ss58,
            executor_class=exec_class.value,
        )
        BlockAllowance.objects.create(
            block=b2,
            allowance=1.0,
            miner_ss58=miner,
            validator_ss58=validator_ss58,
            executor_class=exec_class.value,
        )

    hook_event = threading.Event()
    proceed_event = threading.Event()

    real_create = AllowanceBooking.objects.create

    def hooked_create(*args, **kwargs):
        obj = real_create(*args, **kwargs)
        # Signal booking row created but not yet linked to BlockAllowances
        hook_event.set()
        # Wait until eviction attempt has run
        proceed_event.wait(10)
        return obj

    # Monkeypatch create
    booking_module.AllowanceBooking.objects.create = hooked_create  # type: ignore

    reserve_result: dict[str, object] = {}

    def do_reserve():
        with set_block_number(10_000):
            rid, used_blocks = allowance().reserve_allowance(miner, exec_class, 2.0, 10_000)
            reserve_result["rid"] = rid
            reserve_result["used_blocks"] = used_blocks

    # Start reservation in thread
    t_reserve = threading.Thread(target=do_reserve, name="reserve-thread")
    t_reserve.start()

    # Wait until booking row exists (pre linking)
    hook_event.wait(10)

    # Run eviction concurrently (should NOT delete the in-progress booking because the entire
    # reserve logic is within a single transaction, so booking row is not yet committed / visible
    # outside the transaction OR bulk_update + create are committed atomically depending on DB).
    with set_block_number(10_000):
        evict_old_data()

    # Allow reservation to continue linking allowances
    proceed_event.set()
    t_reserve.join(15)

    # Restore original create to avoid side-effects for subsequent tests
    booking_module.AllowanceBooking.objects.create = real_create  # type: ignore

    assert "rid" in reserve_result, f"Reservation did not complete: {reserve_result}"
    rid = cast(int, reserve_result["rid"])  # booking id
    used_blocks = cast(list[int], reserve_result["used_blocks"])  # list of block numbers

    # Booking must still exist
    assert AllowanceBooking.objects.filter(id=rid).exists(), "Booking was incorrectly evicted"
    # All allowances should point to it
    assert BlockAllowance.objects.filter(allowance_booking_id=rid).count() == 2, (
        "All allowances should be linked to booking"
    )
    assert len(used_blocks) == 2

    # Cleanup
    allowance().undo_allowance_reservation(rid)
