"""Spawn-safe multiprocessing helpers for allowance concurrency tests.

These functions avoid importing Django models at module import time so that
`django.setup()` can be called first inside the child process when using the
`spawn` start method.
"""

from __future__ import annotations

from dataclasses import dataclass
from multiprocessing import Queue
from typing import Any

import django
from compute_horde_core.executor_class import ExecutorClass
from django.apps import apps
from django.conf import settings

from ..default import allowance
from ..types import CannotReserveAllowanceException, ReservationAlreadySpent, ReservationNotFound
from .mockchain import set_block_number


@dataclass
class ReservationResult:
    status: str  # "ok" or "fail"
    reservation_id: int | None
    data: Any | None  # list[int] for ok, float (available) for fail


def _django_bootstrap():
    if not settings.configured:
        django.setup()
    else:
        # Ensure app registry ready
        if not apps.ready:
            django.setup()


def reserve_worker(
    miner: str,
    exec_class_value: str,
    job_start_block: int,
    amount: float,
    q: Queue[ReservationResult],
    ready_event: Any,
    start_event: Any,
) -> None:
    _django_bootstrap()

    # Signal readiness (bootstrap complete) so parent can coordinate simultaneous start.
    ready_event.set()

    # Wait for coordinated start if provided (primarily for spawn determinism)
    start_event.wait(10)

    try:
        with set_block_number(job_start_block):
            rid, blocks_used = allowance().reserve_allowance(
                miner, ExecutorClass(exec_class_value), amount, job_start_block
            )
        q.put(ReservationResult("ok", rid, blocks_used))
    except CannotReserveAllowanceException as e:
        q.put(ReservationResult("fail", None, e.available_allowance_seconds))


def spend_worker(
    reservation_id: int,
    q: Queue[str],
    ready_event: Any,
    start_event: Any,
) -> None:
    _django_bootstrap()

    ready_event.set()
    start_event.wait(10)

    try:
        allowance().spend_allowance(reservation_id)
        q.put("spend_ok")
    except ReservationAlreadySpent:
        q.put("spend_already_spent")
    except ReservationNotFound:
        q.put("spend_not_found")


def undo_worker(
    reservation_id: int,
    q: Queue[str],
    ready_event: Any,
    start_event: Any,
) -> None:
    _django_bootstrap()

    ready_event.set()
    start_event.wait(10)

    try:
        allowance().undo_allowance_reservation(reservation_id)
        q.put("undo_ok")
    except ReservationAlreadySpent:
        q.put("undo_already_spent")
    except ReservationNotFound:
        q.put("undo_not_found")
