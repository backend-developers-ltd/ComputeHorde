import datetime
import threading
import time
from unittest import mock
from unittest.mock import MagicMock

import pytest
from freezegun import freeze_time

from compute_horde_validator.validator.allowance import settings as allowance_settings
from compute_horde_validator.validator.allowance import tasks as allowance_tasks
from compute_horde_validator.validator.allowance.tests.mockchain import set_block_number
from compute_horde_validator.validator.allowance.utils import blocks, manifests
from compute_horde_validator.validator.allowance.utils.supertensor import (
    SuperTensorError,
    supertensor,
)
from compute_horde_validator.validator.models import SystemEvent


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
        with set_block_number(block_number):
            manifests.sync_manifests()
            blocks.process_block_allowance_with_reporting(block_number, supertensor_=supertensor())

    with set_block_number(1020):
        assert blocks.find_missing_blocks(1020) == list(range(-424, 1000)) + [1001, 1004] + list(
            range(1010, 1021)
        )


@pytest.mark.django_db(transaction=True, databases=["default_alias", "default"])
def test_archive_scan_stops_on_supertensor_error_to_prevent_gaps(configure_logs):
    current_block = 2500
    blocks_to_seed = [1000, 1100, 1200, 1300, 2300, 2350]

    with set_block_number(1000, oldest_reachable_block=1000):
        manifests.sync_manifests()

    for block_num in blocks_to_seed:
        with set_block_number(block_num, oldest_reachable_block=1000):
            blocks.process_block_allowance_with_reporting(block_num, supertensor_=supertensor())

    with set_block_number(current_block, oldest_reachable_block=1000):
        archive_supertensor = supertensor()

    call_count = 0

    def mock_process_with_error(block_number, *args, **kwargs):
        nonlocal call_count
        call_count += 1
        if block_number == 2399:
            raise SuperTensorError("Simulated blockchain API error")
        return blocks.process_block_allowance_with_reporting(block_number, *args, **kwargs)

    with (
        mock.patch("turbobt.Bittensor", side_effect=AssertionError("Should not init Bittensor")),
        set_block_number(current_block, oldest_reachable_block=1000),
        mock.patch.object(
            blocks,
            "process_block_allowance_with_reporting",
            side_effect=mock_process_with_error,
        ) as proc_mock,
    ):
        allowance_tasks.scan_archive_blocks_and_calculate_allowance(
            backfilling_supertensor=archive_supertensor,
            keep_running=False,
        )

    assert call_count == 1
    assert proc_mock.call_args_list[0].args[0] == 2399

    system_events = SystemEvent.objects.filter(
        type=SystemEvent.EventType.COMPUTE_TIME_ALLOWANCE,
        subtype=SystemEvent.EventSubType.FAILURE,
        data__context="archive_scan",
    )
    assert system_events.count() == 1
    assert system_events[0].data["block_number"] == 2399
    assert "blockchain API error" in system_events[0].data["error"]


@pytest.mark.django_db(transaction=True, databases=["default_alias", "default"])
def test_archive_scan_respects_range_boundaries_and_oldest_reachable(configure_logs):
    current_block = 1650
    blocks_to_seed = [1000, 1100, 1200, 1450, 1500, 1520]
    oldest_reachable = 1100

    with set_block_number(1000, oldest_reachable_block=oldest_reachable):
        manifests.sync_manifests()

    for block_num in blocks_to_seed:
        with set_block_number(block_num, oldest_reachable_block=oldest_reachable):
            blocks.process_block_allowance_with_reporting(block_num, supertensor_=supertensor())

    with set_block_number(current_block, oldest_reachable_block=oldest_reachable):
        archive_supertensor = supertensor()

    def mock_process_noop(block_number, *args, **kwargs):
        pass

    with (
        mock.patch("turbobt.Bittensor", side_effect=AssertionError("Should not init Bittensor")),
        set_block_number(current_block, oldest_reachable_block=oldest_reachable),
        mock.patch.object(
            blocks,
            "process_block_allowance_with_reporting",
            side_effect=mock_process_noop,
        ) as proc_mock,
        mock.patch.object(allowance_settings, "ARCHIVE_SCAN_MAX_RUN_TIME", new=999999),
        mock.patch.object(allowance_settings, "ARCHIVE_MAX_LOOKBACK", new=500),
    ):
        allowance_tasks.scan_archive_blocks_and_calculate_allowance(
            backfilling_supertensor=archive_supertensor,
            keep_running=False,
        )

    called_blocks = [call.args[0] for call in proc_mock.call_args_list]

    archive_start = current_block - allowance_settings.ARCHIVE_START_OFFSET
    archive_end = current_block - 500
    effective_start = max(archive_end, oldest_reachable)

    assert 1000 not in called_blocks
    assert all(block <= archive_start for block in called_blocks)
    assert all(block >= effective_start for block in called_blocks)

    expected_missing = [
        block_num
        for block_num in range(archive_start, effective_start - 1, -1)
        if block_num not in blocks_to_seed
    ]
    assert called_blocks == expected_missing


@pytest.mark.django_db(transaction=True, databases=["default_alias", "default"])
def test_archive_scan_completes_successfully_with_checkpoints(configure_logs):
    current_block = 1400
    blocks_to_seed = list(range(1000, 1020)) + list(range(1050, 1060)) + list(range(1280, 1285))

    with set_block_number(1000, oldest_reachable_block=1000):
        manifests.sync_manifests()

    for block_num in blocks_to_seed:
        with set_block_number(block_num, oldest_reachable_block=1000):
            blocks.process_block_allowance_with_reporting(block_num, supertensor_=supertensor())

    with set_block_number(current_block, oldest_reachable_block=1000):
        archive_supertensor = supertensor()

    checkpoint_calls = []

    def mock_checkpoint(block_lt, block_gte):
        checkpoint_calls.append((block_lt, block_gte))

    def mock_process_noop(block_number, *args, **kwargs):
        pass

    with (
        mock.patch("turbobt.Bittensor", side_effect=AssertionError("Should not init Bittensor")),
        set_block_number(current_block, oldest_reachable_block=1000),
        mock.patch.object(
            blocks,
            "process_block_allowance_with_reporting",
            side_effect=mock_process_noop,
        ) as proc_mock,
        mock.patch.object(allowance_settings, "ARCHIVE_SCAN_MAX_RUN_TIME", new=999999),
        mock.patch.object(allowance_settings, "ARCHIVE_MAX_LOOKBACK", new=300),
        mock.patch.object(allowance_settings, "ARCHIVE_SCAN_BATCH_SIZE", new=50),
        mock.patch.object(
            allowance_tasks,
            "report_allowance_checkpoint",
            MagicMock(delay=mock_checkpoint),
        ),
    ):
        allowance_tasks.scan_archive_blocks_and_calculate_allowance(
            backfilling_supertensor=archive_supertensor,
            keep_running=False,
        )

    called_blocks = [call.args[0] for call in proc_mock.call_args_list]

    archive_start = current_block - allowance_settings.ARCHIVE_START_OFFSET
    archive_end = current_block - 300

    expected_missing = [
        block_num
        for block_num in range(archive_start, archive_end - 1, -1)
        if block_num not in blocks_to_seed
    ]

    assert called_blocks == expected_missing
    assert len(called_blocks) > 0

    expected_checkpoint_count = len(called_blocks) // 50
    assert len(checkpoint_calls) == expected_checkpoint_count

    if checkpoint_calls:
        for i, (block_lt, block_gte) in enumerate(checkpoint_calls):
            expected_idx = (i + 1) * 50 - 1
            if expected_idx < len(called_blocks):
                expected_block = called_blocks[expected_idx]
                assert block_gte == expected_block
                assert block_lt == expected_block + 50
