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


@pytest.mark.django_db(transaction=True, databases=['default_alias', 'default'])
def test_blocks_scan_blocks_calls_process_for_missing_blocks_and_times_out(monkeypatch):
    missing_blocks = [1000, 1001, 1002]
    with (
        mock.patch("turbobt.Bittensor", side_effect=AssertionError("Should not init Bittensor")),
        mock.patch.object(blocks, "process_block_allowance_with_reporting") as proc_mock,
        mock.patch.object(blocks, "find_missing_blocks", new=lambda *_, **__: missing_blocks),
        freeze_time(datetime.datetime(2025, 1, 1, 12, 0, 0)) as freezer,
        mock.patch.object(blocks.time, "sleep", lambda s: freezer.tick(datetime.timedelta(seconds=s))),
        set_block_number(1002),
    ):
        live_st = mock.MagicMock()
        live_st.get_current_block.return_value = 1002

        with pytest.raises(blocks.TimesUpError):
            blocks.scan_blocks_and_calculate_allowance(
                report_callback=None,
                backfilling_supertensor=mock.MagicMock(),
                livefilling_supertensor=live_st,
            )

        assert [args[0][0] for args in proc_mock.call_args_list] == missing_blocks


@pytest.mark.django_db(transaction=True, databases=['default_alias', 'default'])
def test_tasks_scan_blocks_and_calculate_allowance_uses_real_lock_allows_single_parallel_execution_without_patching_lock():
    # Ensure the preliminary check passes without needing to insert DB records
    with (
        set_block_number(1000),
        mock.patch.object(allowance_tasks, "LOCK_WAIT_TIMEOUT", new=0.5),
        mock.patch.object(blocks, "scan_blocks_and_calculate_allowance", side_effect=lambda *_, **__: time.sleep(1.0)) as scan_mock,
    ):
        manifests.sync_manifests()
        threads = [
            threading.Thread(target=allowance_tasks.scan_blocks_and_calculate_allowance, args=(MagicMock(),))
            for _ in range(3)
        ]
        for t in threads:
            t.start()

        for t in threads:
            t.join(timeout=8)

        assert scan_mock.call_count == 1


"""
TODO: 
2. PrecachingSupertensor - with mocked turbobt interface - to make sure the threads etc. work, making sure the blocks get collected when needed. with django and inmemorycache (maybe as a pytest parametrize)
4. scan_blocks_and_calculate_allowance with patching of process_block_allowance_with_reporting but using the real report_callback
5. find_missing_blocks
"""