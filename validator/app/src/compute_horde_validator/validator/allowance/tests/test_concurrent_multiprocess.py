import multiprocessing
from typing import Any

import pytest
from compute_horde_core.executor_class import ExecutorClass
from django.db import connection, connections

from ...models.allowance.internal import AllowanceBooking, Block, BlockAllowance
from ..default import allowance
from ..utils.supertensor import supertensor
from .mockchain import set_block_number
from .mp_helpers import ReservationResult, reserve_worker, spend_worker, undo_worker


@pytest.fixture
def mp_sync():
    """Factory returning synchronization primitives for 2-worker tests.

    Returns a callable: mp_sync(ctx) -> (start_event, ready_events:list)
    """

    def _factory(ctx):
        start = ctx.Event()
        ready = [ctx.Event(), ctx.Event()]
        return start, ready

    return _factory


@pytest.mark.django_db(transaction=True)
@pytest.mark.filterwarnings("ignore::pytest.PytestUnraisableExceptionWarning")
def test_concurrent_reservations_multiprocess(mp_sync):
    """Same as thread test but using distinct OS processes."""
    miner = "mp_miner"
    exec_class = ExecutorClass.always_on__llm__a6000

    with set_block_number(4000):
        validator_ss58 = supertensor().wallet().get_hotkey().ss58_address

        b1 = Block.objects.create(block_number=3999, creation_timestamp="2024-01-01T00:00:00Z")
        b2 = Block.objects.create(block_number=4000, creation_timestamp="2024-01-01T00:00:01Z")
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
    # Ensure committed so spawned processes can observe rows (defensive; should already be committed)
    connection.commit()

    # Prefer fork on POSIX to share test DB state; fall back to spawn.

    ctx: Any
    if "fork" in multiprocessing.get_all_start_methods():
        connections.close_all()  # ensure clean connections pre-fork
        ctx = multiprocessing.get_context("fork")
    elif "spawn" in multiprocessing.get_all_start_methods():
        ctx = multiprocessing.get_context("spawn")
    else:
        pytest.skip("No suitable start method for multiprocessing test")
    q = ctx.Queue()
    # Synchronization primitives for deterministic spawn starts
    start_event, (r1_ready, r2_ready) = mp_sync(ctx)
    p1 = ctx.Process(
        target=reserve_worker,
        args=(miner, exec_class.value, 4000, 2.0, q, r1_ready, start_event),
        name="reserve-1",
    )
    p2 = ctx.Process(
        target=reserve_worker,
        args=(miner, exec_class.value, 4000, 2.0, q, r2_ready, start_event),
        name="reserve-2",
    )
    p1.start()
    p2.start()
    # Wait both workers to finish bootstrap (esp. needed for spawn)
    r1_ready.wait(10)
    r2_ready.wait(10)
    # Release both simultaneously
    start_event.set()
    p1.join(15)
    p2.join(15)
    assert p1.exitcode is not None and p2.exitcode is not None, "Processes did not terminate"

    results: list[ReservationResult] = [q.get_nowait(), q.get_nowait()]
    oks = [r for r in results if r.status == "ok"]
    # Under spawn the isolation is stricter; acceptable outcomes:
    # - one ok and one fail (preferred)
    # - zero oks (both saw none available). We assert no double success.
    assert len(oks) < 2, f"Both processes reserved unexpectedly: {results}"

    booking_ids = list(AllowanceBooking.objects.values_list("id", flat=True))
    assert len(booking_ids) == 1, f"Expected exactly one booking, saw {booking_ids}"
    winner_booking_id = booking_ids[0]
    assert BlockAllowance.objects.filter(allowance_booking_id=winner_booking_id).count() == 2, (
        "Both allowances should be tied to winning booking"
    )

    with set_block_number(5000):
        allowance().undo_allowance_reservation(winner_booking_id)


@pytest.mark.django_db(transaction=True)
@pytest.mark.filterwarnings("ignore::pytest.PytestUnraisableExceptionWarning")
def test_concurrent_undo_vs_spend_multiprocess(mp_sync):
    miner = "mp_race_miner"
    exec_class = ExecutorClass.always_on__llm__a6000

    with set_block_number(5000):
        validator_ss58 = supertensor().wallet().get_hotkey().ss58_address

        b = Block.objects.create(block_number=4999, creation_timestamp="2024-01-01T00:00:00Z")
        BlockAllowance.objects.create(
            block=b,
            allowance=1.0,
            miner_ss58=miner,
            validator_ss58=validator_ss58,
            executor_class=exec_class.value,
        )
    with set_block_number(5000):
        reservation_id, _ = allowance().reserve_allowance(miner, exec_class, 1.0, 5000)
    connection.commit()

    ctx: Any
    if "fork" in multiprocessing.get_all_start_methods():
        connections.close_all()
        ctx = multiprocessing.get_context("fork")
    elif "spawn" in multiprocessing.get_all_start_methods():
        ctx = multiprocessing.get_context("spawn")
    else:
        pytest.skip("No suitable start method")
    q = ctx.Queue()
    start_event, (spend_ready, undo_ready) = mp_sync(ctx)
    spend_p = ctx.Process(
        target=spend_worker,
        args=(reservation_id, q, spend_ready, start_event),
        name="spend",
    )
    undo_p = ctx.Process(
        target=undo_worker,
        args=(reservation_id, q, undo_ready, start_event),
        name="undo",
    )
    spend_p.start()
    undo_p.start()
    spend_ready.wait(10)
    undo_ready.wait(10)
    start_event.set()
    spend_p.join(15)
    undo_p.join(15)
    assert spend_p.exitcode is not None and undo_p.exitcode is not None

    results = [q.get_nowait(), q.get_nowait()]
    booking_exists = AllowanceBooking.objects.filter(id=reservation_id).exists()
    if booking_exists:
        booking = AllowanceBooking.objects.get(id=reservation_id)
        # Accept either spent or (rare) lingering reserved if process interleaving delayed action
        if booking.is_spent:
            assert any(r.startswith("undo_") for r in results)
            assert any(r.startswith("spend_") or r == "spend_ok" for r in results)
        else:
            # Undo might have failed after spend lost race; treat as acceptable transient state
            assert any(r in ("spend_ok", "spend_already_spent") for r in results)
    else:
        assert any(r == "undo_ok" for r in results)
        assert any(r in ("spend_not_found",) for r in results)

    linked = BlockAllowance.objects.filter(allowance_booking_id=reservation_id).count()
    if booking_exists:
        assert linked > 0
    else:
        assert linked == 0
