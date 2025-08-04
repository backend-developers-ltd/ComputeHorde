import threading
import time

import pytest
from django.db import transaction

from compute_horde_validator.validator.locks import Lock, LockTimeout, LockType


@pytest.mark.django_db(databases=["default"], transaction=True)
def test_lock_context_manager_success():
    """Test successful lock acquisition and release using context manager."""
    with transaction.atomic():
        with Lock(LockType.WEIGHT_SETTING, timeout_seconds=1.0):
            pass


@pytest.mark.django_db(databases=["default"], transaction=True)
def test_lock_timeout():
    """Test lock timeout functionality."""

    def hold_lock():
        with transaction.atomic():
            with Lock(LockType.WEIGHT_SETTING, timeout_seconds=2.0):
                time.sleep(1.0)  # Hold lock for 1 second

    # Start thread that holds the lock
    thread = threading.Thread(target=hold_lock)
    thread.start()

    # Give first thread time to acquire lock
    time.sleep(0.1)

    # Try to acquire same lock with short timeout
    with pytest.raises(LockTimeout):
        with transaction.atomic():
            with Lock(LockType.WEIGHT_SETTING, timeout_seconds=0.2):
                pass

    thread.join(timeout=3.0)


@pytest.mark.django_db(databases=["default"], transaction=True)
def test_lock_different_types_concurrent():
    """Test that different lock types can be acquired concurrently."""
    results = []

    def acquire_lock(lock_type, result_id):
        try:
            with transaction.atomic():
                with Lock(lock_type, timeout_seconds=0.1):
                    time.sleep(0.2)
                    results.append(f"success_{result_id}")
        except Exception as e:
            results.append(f"error_{result_id}: {e}")

    threads = []
    for i, lock_type in enumerate(
        [LockType.WEIGHT_SETTING, LockType.VALIDATION_SCHEDULING, LockType.TRUSTED_MINER_LOCK]
    ):
        thread = threading.Thread(target=acquire_lock, args=(lock_type, i))
        threads.append(thread)
        thread.start()

    for thread in threads:
        thread.join(timeout=2.0)

    assert set(results) == {"success_0", "success_1", "success_2"}


@pytest.mark.django_db(databases=["default"], transaction=True)
def test_lock_concurrent_same_type():
    """Test concurrent access to the same lock type."""
    events = []

    def acquire_lock(thread_id, pre_lock_sleep, lock_timeout, in_lock_sleep):
        time.sleep(pre_lock_sleep)
        try:
            with transaction.atomic():
                with Lock(LockType.WEIGHT_SETTING, timeout_seconds=lock_timeout):
                    events.append(f"in_lock_pre_sleep_{thread_id}")
                    time.sleep(in_lock_sleep)
                    events.append(f"in_lock_post_sleep_{thread_id}")
        except LockTimeout:
            events.append(f"lock_timeout_{thread_id}")
        except Exception as e:
            events.append(f"error_{thread_id}: {e}")

    threads = []

    threads.append(
        threading.Thread(
            target=acquire_lock,
            kwargs={
                "thread_id": 0,
                "pre_lock_sleep": 1.0,
                "lock_timeout": 1.0,
                "in_lock_sleep": 0.3,
            },
        )
    )
    threads[-1].start()

    threads.append(
        threading.Thread(
            target=acquire_lock,
            kwargs={
                "thread_id": 1,
                "pre_lock_sleep": 0.0,
                "lock_timeout": 1.0,
                "in_lock_sleep": 0.3,
            },
        )
    )
    threads[-1].start()

    threads.append(
        threading.Thread(
            target=acquire_lock,
            kwargs={
                "thread_id": 2,
                "pre_lock_sleep": 0.1,
                "lock_timeout": 0.1,
                "in_lock_sleep": 0.3,
            },
        )
    )
    threads[-1].start()

    # Wait for all threads to complete
    for thread in threads:
        thread.join(timeout=5.0)

    assert events == [
        "in_lock_pre_sleep_1",
        "lock_timeout_2",
        "in_lock_post_sleep_1",
        "in_lock_pre_sleep_0",
        "in_lock_post_sleep_0",
    ]
