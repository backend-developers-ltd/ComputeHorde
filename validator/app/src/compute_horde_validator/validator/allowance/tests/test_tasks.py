import datetime
import threading
import time
from unittest import mock
from unittest.mock import MagicMock

import pytest
from freezegun import freeze_time

from compute_horde_validator.validator.allowance import tasks as allowance_tasks
from compute_horde_validator.validator.allowance.tests.mockchain import set_block_number
from compute_horde_validator.validator.allowance.utils import blocks, manifests
from compute_horde_validator.validator.allowance.utils.supertensor import supertensor


@pytest.mark.django_db(transaction=True, databases=["default_alias", "default"])
def test_blocks_scan_blocks_calls_process_for_missing_blocks_and_times_out(monkeypatch):
    missing_blocks = [1000, 1001, 1002]
    back_st = mock.MagicMock()

    def process_block_allowance_with_reporting_side_effect(block_number, st, *a, **kw):
        if st == back_st:
            supertensor().block_number += 1  # type: ignore
        elif st == supertensor():
            freezer.tick(datetime.timedelta(seconds=10000))

    with (
        mock.patch("turbobt.Bittensor", side_effect=AssertionError("Should not init Bittensor")),
        mock.patch.object(blocks, "find_missing_blocks", new=lambda *_, **__: missing_blocks),
        freeze_time(datetime.datetime(2025, 1, 1, 12, 0, 0)) as freezer,
        mock.patch.object(blocks.time, "sleep"),  # type: ignore
        set_block_number(1003),
        mock.patch.object(
            blocks,
            "process_block_allowance_with_reporting",
            side_effect=process_block_allowance_with_reporting_side_effect,
        ) as proc_mock,
    ):
        with pytest.raises(blocks.TimesUpError):
            blocks.scan_blocks_and_calculate_allowance(
                report_callback=None,
                backfilling_supertensor=back_st,
            )

        assert proc_mock.call_args_list == [
            mock.call(1000, back_st),
            mock.call(1001, back_st),
            mock.call(1002, back_st),
            mock.call(1004, supertensor(), live=True),
        ]


@pytest.mark.django_db(transaction=True, databases=["default_alias", "default"])
def test_tasks_scan_blocks_and_calculate_allowance_uses_real_lock_allows_single_parallel_execution_without_patching_lock():
    with (
        set_block_number(1000),
        mock.patch.object(allowance_tasks, "LOCK_WAIT_TIMEOUT", new=0.5),
        mock.patch.object(
            blocks,
            "scan_blocks_and_calculate_allowance",
            side_effect=lambda *_, **__: time.sleep(1.0),
        ) as scan_mock,
    ):
        manifests.sync_manifests()
        threads = [
            threading.Thread(
                target=allowance_tasks.scan_blocks_and_calculate_allowance, args=(MagicMock(),)
            )
            for _ in range(3)
        ]
        for t in threads:
            t.start()

        for t in threads:
            t.join(timeout=8)

        assert scan_mock.call_count == 1


@pytest.mark.django_db(transaction=True, databases=["default_alias", "default"])
@pytest.mark.filterwarnings("ignore::pytest.PytestUnraisableExceptionWarning")
def test_find_missing_blocks():
    with set_block_number(1000):
        blocks.process_block_allowance_with_reporting(1000, supertensor_=supertensor())

    with set_block_number(1002):
        blocks.process_block_allowance_with_reporting(1002, supertensor_=supertensor())

    with set_block_number(1003):
        blocks.process_block_allowance_with_reporting(1003, supertensor_=supertensor())

    for block_number in range(1005, 1010):
        with set_block_number(block_number):
            manifests.sync_manifests()
            blocks.process_block_allowance_with_reporting(block_number, supertensor_=supertensor())

    with set_block_number(1020):
        assert blocks.find_missing_blocks(1020) == list(range(-424, 1000)) + [1001, 1004] + list(
            range(1010, 1021)
        )
