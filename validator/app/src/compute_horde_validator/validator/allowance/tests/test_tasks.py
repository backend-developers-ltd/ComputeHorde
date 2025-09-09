import datetime
import threading
import time
from unittest import mock
from unittest.mock import MagicMock

import pytest
from freezegun import freeze_time

from compute_horde_validator.validator.allowance import tasks as allowance_tasks
from compute_horde_validator.validator.allowance.tests.mockchain import (
    mock_manifest_endpoints_for_block_number,
    set_block_number,
)
from compute_horde_validator.validator.allowance.utils import blocks, manifests
from compute_horde_validator.validator.allowance.utils.supertensor import supertensor


@pytest.fixture()
def scan_blocks_prep(configure_logs):
    orig_process_block_allowance_with_reporting = blocks.process_block_allowance_with_reporting

    def process_block_allowance_with_reporting_side_effect(*a, **kw):
        ret = orig_process_block_allowance_with_reporting(*a, **kw)
        freezer.tick(datetime.timedelta(seconds=1))
        return ret

    with set_block_number(1000, oldest_reachable_block=1005):
        manifests.sync_manifests()

    with set_block_number(1013, oldest_reachable_block=1005):
        backfilling_supertensor = supertensor()

    with (
        mock.patch("turbobt.Bittensor", side_effect=AssertionError("Should not init Bittensor")),
        freeze_time(datetime.datetime(2025, 1, 1, 12, 0, 0)) as freezer,
        mock.patch.object(
            blocks.time,  # type: ignore
            "sleep",
            side_effect=lambda *a, **kw: supertensor().inc_block_number(),  # type: ignore
        ),
        set_block_number(1013, oldest_reachable_block=1005),
        mock.patch.object(
            blocks,
            "process_block_allowance_with_reporting",
            side_effect=process_block_allowance_with_reporting_side_effect,
        ) as proc_mock,
    ):
        livefilling_supertensor = supertensor()
        assert (
            livefilling_supertensor is not backfilling_supertensor
        )  # this is useful in asserting which calls get which supertensor
        yield proc_mock, livefilling_supertensor, backfilling_supertensor


@pytest.mark.django_db(transaction=True, databases=["default_alias", "default"])
def test_blocks_scan_blocks_timeout_on_backfilling(scan_blocks_prep):
    proc_mock, livefilling_supertensor, backfilling_supertensor = scan_blocks_prep
    with (
        mock.patch.object(allowance_tasks, "MAX_RUN_TIME", new=7),
    ):
        allowance_tasks.scan_blocks_and_calculate_allowance(
            backfilling_supertensor=backfilling_supertensor,
            keep_running=False,
        )

    assert proc_mock.call_args_list == [
        mock.call(1005, backfilling_supertensor, blocks_behind=8),
        mock.call(1006, backfilling_supertensor, blocks_behind=7),
        mock.call(1007, backfilling_supertensor, blocks_behind=6),
        mock.call(1008, backfilling_supertensor, blocks_behind=5),
        mock.call(1009, backfilling_supertensor, blocks_behind=4),
        mock.call(1010, backfilling_supertensor, blocks_behind=3),
        mock.call(1011, backfilling_supertensor, blocks_behind=2),
        mock.call(1012, backfilling_supertensor, blocks_behind=1),
    ]


@pytest.mark.django_db(transaction=True, databases=["default_alias", "default"])
def test_blocks_scan_blocks_timeout_on_livefilling(scan_blocks_prep):
    proc_mock, livefilling_supertensor, backfilling_supertensor = scan_blocks_prep
    with (
        mock.patch.object(allowance_tasks, "MAX_RUN_TIME", new=9),
    ):
        allowance_tasks.scan_blocks_and_calculate_allowance(
            backfilling_supertensor=backfilling_supertensor,
            keep_running=False,
        )

    assert proc_mock.call_args_list == [
        mock.call(1005, backfilling_supertensor, blocks_behind=8),
        mock.call(1006, backfilling_supertensor, blocks_behind=7),
        mock.call(1007, backfilling_supertensor, blocks_behind=6),
        mock.call(1008, backfilling_supertensor, blocks_behind=5),
        mock.call(1009, backfilling_supertensor, blocks_behind=4),
        mock.call(1010, backfilling_supertensor, blocks_behind=3),
        mock.call(1011, backfilling_supertensor, blocks_behind=2),
        mock.call(1012, backfilling_supertensor, blocks_behind=1),
        mock.call(1013, backfilling_supertensor, blocks_behind=0),
        mock.call(1014, livefilling_supertensor, live=True),
    ]


@pytest.mark.django_db(transaction=True, databases=["default_alias", "default"])
def test_blocks_scan_blocks_timeout_on_livefilling_several_blocks_in(scan_blocks_prep):
    proc_mock, livefilling_supertensor, backfilling_supertensor = scan_blocks_prep
    with (
        mock.patch.object(allowance_tasks, "MAX_RUN_TIME", new=12),
    ):
        allowance_tasks.scan_blocks_and_calculate_allowance(
            backfilling_supertensor=backfilling_supertensor,
            keep_running=False,
        )

    assert proc_mock.call_args_list == [
        mock.call(1005, backfilling_supertensor, blocks_behind=8),
        mock.call(1006, backfilling_supertensor, blocks_behind=7),
        mock.call(1007, backfilling_supertensor, blocks_behind=6),
        mock.call(1008, backfilling_supertensor, blocks_behind=5),
        mock.call(1009, backfilling_supertensor, blocks_behind=4),
        mock.call(1010, backfilling_supertensor, blocks_behind=3),
        mock.call(1011, backfilling_supertensor, blocks_behind=2),
        mock.call(1012, backfilling_supertensor, blocks_behind=1),
        mock.call(1013, backfilling_supertensor, blocks_behind=0),
        mock.call(1014, livefilling_supertensor, live=True),
        mock.call(1015, livefilling_supertensor, live=True),
        mock.call(1016, livefilling_supertensor, live=True),
        mock.call(1017, livefilling_supertensor, live=True),
    ]


@pytest.mark.django_db(transaction=True, databases=["default_alias", "default"])
def test_blocks_scan_blocks_timeout_on_livefilling_several_blocks_in_rerun(scan_blocks_prep):
    proc_mock, livefilling_supertensor, backfilling_supertensor = scan_blocks_prep
    with (
        mock.patch.object(allowance_tasks, "MAX_RUN_TIME", new=12),
    ):
        allowance_tasks.scan_blocks_and_calculate_allowance(
            backfilling_supertensor=backfilling_supertensor,
            keep_running=False,
        )

    assert proc_mock.call_args_list == [
        mock.call(1005, backfilling_supertensor, blocks_behind=8),
        mock.call(1006, backfilling_supertensor, blocks_behind=7),
        mock.call(1007, backfilling_supertensor, blocks_behind=6),
        mock.call(1008, backfilling_supertensor, blocks_behind=5),
        mock.call(1009, backfilling_supertensor, blocks_behind=4),
        mock.call(1010, backfilling_supertensor, blocks_behind=3),
        mock.call(1011, backfilling_supertensor, blocks_behind=2),
        mock.call(1012, backfilling_supertensor, blocks_behind=1),
        mock.call(1013, backfilling_supertensor, blocks_behind=0),
        mock.call(1014, livefilling_supertensor, live=True),
        mock.call(1015, livefilling_supertensor, live=True),
        mock.call(1016, livefilling_supertensor, live=True),
        mock.call(1017, livefilling_supertensor, live=True),
    ]

    proc_mock.call_args_list = []

    with (
        mock.patch.object(allowance_tasks, "MAX_RUN_TIME", new=12),
    ):
        allowance_tasks.scan_blocks_and_calculate_allowance(
            backfilling_supertensor=backfilling_supertensor,
            keep_running=False,
        )

    assert proc_mock.call_args_list == [
        mock.call(1018, livefilling_supertensor, live=True),
        mock.call(1019, livefilling_supertensor, live=True),
        mock.call(1020, livefilling_supertensor, live=True),
        mock.call(1021, livefilling_supertensor, live=True),
        mock.call(1022, livefilling_supertensor, live=True),
        mock.call(1023, livefilling_supertensor, live=True),
        mock.call(1024, livefilling_supertensor, live=True),
        mock.call(1025, livefilling_supertensor, live=True),
        mock.call(1026, livefilling_supertensor, live=True),
        mock.call(1027, livefilling_supertensor, live=True),
        mock.call(1028, livefilling_supertensor, live=True),
        mock.call(1029, livefilling_supertensor, live=True),
        mock.call(1030, livefilling_supertensor, live=True),
    ]


@pytest.mark.django_db(transaction=True, databases=["default_alias", "default"])
def test_blocks_scan_blocks_timeout_on_livefilling_several_blocks_in_rerun_with_blocks_in_between(
    scan_blocks_prep,
):
    proc_mock, livefilling_supertensor, backfilling_supertensor = scan_blocks_prep
    with (
        mock.patch.object(allowance_tasks, "MAX_RUN_TIME", new=12),
    ):
        allowance_tasks.scan_blocks_and_calculate_allowance(
            backfilling_supertensor=backfilling_supertensor,
            keep_running=False,
        )

    assert proc_mock.call_args_list == [
        mock.call(1005, backfilling_supertensor, blocks_behind=8),
        mock.call(1006, backfilling_supertensor, blocks_behind=7),
        mock.call(1007, backfilling_supertensor, blocks_behind=6),
        mock.call(1008, backfilling_supertensor, blocks_behind=5),
        mock.call(1009, backfilling_supertensor, blocks_behind=4),
        mock.call(1010, backfilling_supertensor, blocks_behind=3),
        mock.call(1011, backfilling_supertensor, blocks_behind=2),
        mock.call(1012, backfilling_supertensor, blocks_behind=1),
        mock.call(1013, backfilling_supertensor, blocks_behind=0),
        mock.call(1014, livefilling_supertensor, live=True),
        mock.call(1015, livefilling_supertensor, live=True),
        mock.call(1016, livefilling_supertensor, live=True),
        mock.call(1017, livefilling_supertensor, live=True),
    ]

    proc_mock.call_args_list = []
    livefilling_supertensor.inc_block_number()
    livefilling_supertensor.inc_block_number()
    livefilling_supertensor.inc_block_number()

    with (
        mock.patch.object(allowance_tasks, "MAX_RUN_TIME", new=12),
    ):
        allowance_tasks.scan_blocks_and_calculate_allowance(
            backfilling_supertensor=backfilling_supertensor,
            keep_running=False,
        )

    assert proc_mock.call_args_list == [
        mock.call(1018, backfilling_supertensor, blocks_behind=2),
        mock.call(1019, backfilling_supertensor, blocks_behind=1),
        mock.call(1020, backfilling_supertensor, blocks_behind=0),
        mock.call(1021, livefilling_supertensor, live=True),
        mock.call(1022, livefilling_supertensor, live=True),
        mock.call(1023, livefilling_supertensor, live=True),
        mock.call(1024, livefilling_supertensor, live=True),
        mock.call(1025, livefilling_supertensor, live=True),
        mock.call(1026, livefilling_supertensor, live=True),
        mock.call(1027, livefilling_supertensor, live=True),
        mock.call(1028, livefilling_supertensor, live=True),
        mock.call(1029, livefilling_supertensor, live=True),
        mock.call(1030, livefilling_supertensor, live=True),
    ]


@pytest.mark.django_db(transaction=True, databases=["default_alias", "default"])
def test_tasks_scan_blocks_and_calculate_allowance_uses_real_lock_allows_single_parallel_execution_without_patching_lock():
    with (
        set_block_number(1000),
        mock_manifest_endpoints_for_block_number(1000),
        mock.patch.object(allowance_tasks, "LOCK_WAIT_TIMEOUT", new=0.5),
        mock.patch.object(allowance_tasks, "MAX_RUN_TIME", new=0.01),
        mock.patch.object(
            blocks,
            "backfill_blocks_if_necessary",
            side_effect=lambda *_, **__: time.sleep(1.0),
        ) as scan_mock,
    ):
        manifests.sync_manifests()
        threads = [
            threading.Thread(
                target=allowance_tasks.scan_blocks_and_calculate_allowance,
                args=(MagicMock(), False),
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
        with set_block_number(block_number), mock_manifest_endpoints_for_block_number(block_number):
            manifests.sync_manifests()
            blocks.process_block_allowance_with_reporting(block_number, supertensor_=supertensor())

    with set_block_number(1020):
        assert blocks.find_missing_blocks(1020) == list(range(-424, 1000)) + [1001, 1004] + list(
            range(1010, 1021)
        )
