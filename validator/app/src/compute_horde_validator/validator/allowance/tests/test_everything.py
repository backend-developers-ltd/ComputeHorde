import datetime
import json
import logging
import pathlib
import random
from unittest import mock

import pytest
from compute_horde.test_wallet import get_test_validator_wallet
from compute_horde_core.executor_class import ExecutorClass
from freezegun import freeze_time

from ...models import AllowanceBooking, AllowanceMinerManifest, Block, BlockAllowance
from .. import tasks
from ..default import allowance
from ..metrics import (
    VALIDATOR_ALLOWANCE_CHECKPOINT,
    VALIDATOR_RESERVE_ALLOWANCE_DURATION,
    VALIDATOR_UNDO_ALLOWANCE_RESERVATION_DURATION,
)
from ..tasks import evict_old_data
from ..types import (
    AllowanceException,
    CannotReserveAllowanceException,
    NotEnoughAllowanceException,
    ReservationAlreadySpent,
    ReservationNotFound,
)
from ..utils import blocks, manifests
from ..utils.manifests import sync_manifests
from ..utils.supertensor import supertensor
from .mockchain import (
    EXECUTOR_CAP,
    MINER_HOTKEYS,
    VALIDATOR_HOTKEYS,
    manifest_responses,
    mock_manifest_endpoints_for_block_number,
    set_block_number,
)
from .utils_for_tests import (
    LF,
    Matcher,
    allowance_dict,
    assert_metric_observed,
    assert_system_events,
    get_metric_values_by_remaining_labels,
    inject_blocks_with_allowances,
)


@pytest.fixture
def configure_logs(caplog):
    with (
        caplog.at_level(logging.CRITICAL, logger="transport"),
        caplog.at_level(logging.CRITICAL, logger="compute_horde.miner_client.base"),
        caplog.at_level(logging.CRITICAL, logger="compute_horde.miner_client.organic"),
    ):
        yield


def test_current_block():
    with set_block_number(1000):
        assert allowance().get_current_block() == 1000

    with set_block_number(1005):
        assert allowance().get_current_block() == 1005


@pytest.mark.filterwarnings("ignore::pytest.PytestUnraisableExceptionWarning")
def test_job_too_long():
    with set_block_number(1000):
        with pytest.raises(AllowanceException) as e1:
            allowance().find_miners_with_allowance(
                3601.0, ExecutorClass.always_on__llm__a6000, 1000
            )
        assert "Required allowance cannot be greater than 3600.0 seconds" in e1.value.args[0]

        with pytest.raises(AllowanceException) as e2:
            allowance().reserve_allowance(
                "doesn't matter", ExecutorClass.always_on__llm__a6000, 3601.0, 1000
            )
        assert "Required allowance cannot be greater than 3600.0 seconds" in e2.value.args[0]


@pytest.mark.django_db(transaction=True)
@pytest.mark.filterwarnings("ignore::pytest.PytestUnraisableExceptionWarning")
def test_empty():
    with set_block_number(1000):
        with pytest.raises(NotEnoughAllowanceException) as e1:
            allowance().find_miners_with_allowance(1.0, ExecutorClass.always_on__llm__a6000, 1000)
        assert e1.value.to_dict() == {
            "highest_available_allowance": 0,
            "highest_available_allowance_ss58": "",
            "highest_unspent_allowance": 0,
            "highest_unspent_allowance_ss58": "",
        }

        # Test reserve_allowance with metrics
        with assert_metric_observed(VALIDATOR_RESERVE_ALLOWANCE_DURATION, "reserve_allowance"):
            with pytest.raises(CannotReserveAllowanceException) as e2:
                allowance().reserve_allowance(
                    MINER_HOTKEYS[0],
                    ExecutorClass.always_on__llm__a6000,
                    1.0,
                    1000,
                )
        assert e2.value.to_dict() == {
            "miner": "stable_miner_000",
            "required_allowance_seconds": LF(1.0),
            "available_allowance_seconds": LF(0.0),
        }

        # Test undo_allowance_reservation with metrics (using non-existent reservation)
        with assert_metric_observed(
            VALIDATOR_UNDO_ALLOWANCE_RESERVATION_DURATION, "undo_allowance_reservation"
        ):
            with pytest.raises(ReservationNotFound) as e3:
                allowance().undo_allowance_reservation(999999)  # Non-existent reservation ID
        assert "Reservation with ID 999999 not found" in str(e3.value)


@pytest.mark.django_db(transaction=True)
@pytest.mark.filterwarnings("ignore::pytest.PytestUnraisableExceptionWarning")
def test_block_without_manifests():
    with set_block_number(1000):
        blocks.process_block_allowance_with_reporting(1000, supertensor_=supertensor())
        with pytest.raises(NotEnoughAllowanceException) as e1:
            allowance().find_miners_with_allowance(1.0, ExecutorClass.always_on__llm__a6000, 1000)
        assert e1.value.to_dict() == {
            "highest_available_allowance": 0,
            "highest_available_allowance_ss58": "",
            "highest_unspent_allowance": 0,
            "highest_unspent_allowance_ss58": "",
        }
        with pytest.raises(CannotReserveAllowanceException) as e2:
            allowance().reserve_allowance(
                MINER_HOTKEYS[0],
                ExecutorClass.always_on__llm__a6000,
                1.0,
                1000,
            )
        assert e2.value.to_dict() == {
            "miner": "stable_miner_000",
            "required_allowance_seconds": LF(1.0),
            "available_allowance_seconds": LF(0.0),
        }

    with set_block_number(1001):
        blocks.process_block_allowance_with_reporting(1001, supertensor_=supertensor())

        with pytest.raises(NotEnoughAllowanceException) as e3:
            allowance().find_miners_with_allowance(1.0, ExecutorClass.always_on__llm__a6000, 1001)
        assert e3.value.to_dict() == {
            "highest_available_allowance": 0,
            "highest_available_allowance_ss58": "",
            "highest_unspent_allowance": 0,
            "highest_unspent_allowance_ss58": "",
        }
        with pytest.raises(CannotReserveAllowanceException) as e4:
            allowance().reserve_allowance(
                MINER_HOTKEYS[0],
                ExecutorClass.always_on__llm__a6000,
                1.0,
                1000,
            )
        assert e4.value.to_dict() == {
            "miner": "stable_miner_000",
            "required_allowance_seconds": LF(1.0),
            "available_allowance_seconds": LF(0.0),
        }


def assert_error_messages(block_number: int, highest_available: float):
    with pytest.raises(NotEnoughAllowanceException) as e1:
        allowance().find_miners_with_allowance(
            highest_available + 1e-6, ExecutorClass.always_on__llm__a6000, block_number
        )
    assert e1.value.to_dict() == {
        "highest_available_allowance": LF(highest_available),
        "highest_available_allowance_ss58": Matcher(r".*"),
        "highest_unspent_allowance": LF(highest_available),
        "highest_unspent_allowance_ss58": Matcher(r".*"),
    }, "find_miners_with_allowance returned wrong error message"

    highest_miner = e1.value.highest_available_allowance_ss58

    with pytest.raises(CannotReserveAllowanceException) as e2:
        allowance().reserve_allowance(
            highest_miner,
            ExecutorClass.always_on__llm__a6000,
            highest_available + 1e-6,
            block_number,
        )

    assert e2.value.to_dict() == {
        "miner": highest_miner,
        "required_allowance_seconds": LF(highest_available + 1e-6),
        "available_allowance_seconds": LF(highest_available),
    }, "reserve_allowance returned wrong error message"


@pytest.mark.django_db(transaction=True, databases=["default_alias", "default"])
@pytest.mark.filterwarnings("ignore::pytest.PytestUnraisableExceptionWarning")
def test_complete(caplog, configure_logs):
    with set_block_number(1000), mock_manifest_endpoints_for_block_number(1000):
        manifests.sync_manifests()
        blocks.process_block_allowance_with_reporting(1000, supertensor_=supertensor(), live=True)
    with set_block_number(1001), mock_manifest_endpoints_for_block_number(1001):
        blocks.process_block_allowance_with_reporting(1001, supertensor_=supertensor(), live=True)
        resp = allowance().find_miners_with_allowance(
            1.0, ExecutorClass.always_on__llm__a6000, 1001
        )
    highest_allowance = resp[0][1]
    number_of_executors = manifest_responses(1000)[0][1][  # type: ignore
        ExecutorClass.always_on__llm__a6000
    ]
    assert highest_allowance == number_of_executors * 11.99 * 9009.0 / (
        1001 + 4004 + 9009 + 16016 + 25025 + 36036
    )
    for block_number in range(1002, 1006):
        with set_block_number(block_number):
            blocks.process_block_allowance_with_reporting(
                block_number, supertensor_=supertensor(), live=True
            )

    with set_block_number(1004):
        assert "deregging_miner_247" in [n.hotkey_ss58 for n in allowance().neurons(block=1004)]
        assert "deregging_miner_247" in [
            el[0]
            for el in allowance().find_miners_with_allowance(
                1.0, ExecutorClass.always_on__llm__a6000, 1004
            )
        ]

    resp = allowance().find_miners_with_allowance(1.0, ExecutorClass.always_on__llm__a6000, 1001)
    highest_allowance = resp[0][1]
    assert LF(highest_allowance) == number_of_executors * (11.99 + 3 * 12.00 + 12.01) * 9009.0 / (
        1001 + 4004 + 9009 + 16016 + 25025 + 36036
    )
    for block_number in range(1006, 1011):
        with set_block_number(block_number):
            blocks.process_block_allowance_with_reporting(
                block_number, supertensor_=supertensor(), live=True
            )

    with set_block_number(1010):
        assert "deregging_miner_247" not in [n.hotkey_ss58 for n in allowance().neurons(block=1010)]
        assert "deregging_miner_247" not in [
            el[0]
            for el in allowance().find_miners_with_allowance(
                1.0, ExecutorClass.always_on__llm__a6000, 1010
            )
        ]

    resp = allowance().find_miners_with_allowance(1.0, ExecutorClass.always_on__llm__a6000, 1001)
    highest_allowance = resp[0][1]
    assert LF(highest_allowance) == (
        number_of_executors
        * (11.99 + 3 * 12.00 + 12.01)
        * 9009.0
        / (1001 + 4004 + 9009 + 16016 + 25025 + 36036)
        + number_of_executors
        * 11.99
        * 9009.0
        / (1001 + 4004 + 9009 + 16016 + 36036)  # stake-losing validator fell out
        + number_of_executors
        * 12.00
        * 9021.6126
        / (
            1002.0009999999999
            + 4008.8048000000003
            + 9021.6126
            + 16041.625600000001
            + 25070.045000000002
            + 36108.072
        )
        + number_of_executors
        * 12.00
        * 9034.225199999999
        / (1003.002 + 4013.6096 + 9034.225199999999 + 16067.2512 + 25115.09 + 36180.144)
        + number_of_executors
        * 12.00
        * 9046.8378
        / (
            1004.0029999999999 + 4018.4144 + 9046.8378 + 16092.876799999998 + 25160.135000000002
        )  # deregging validator fell out
        + number_of_executors
        * 12.01
        * 9059.4504
        / (
            1005.004 + 4023.2191999999995 + 9059.4504 + 16118.5024 + 25205.180000000004 + 36324.288
        )  # stake-losing validator fell out
    )
    with pytest.raises(NotEnoughAllowanceException) as e1:
        allowance().find_miners_with_allowance(3600.0, ExecutorClass.always_on__llm__a6000, 1001)
    assert e1.value.to_dict() == {
        "highest_available_allowance": LF(highest_allowance),
        "highest_available_allowance_ss58": Matcher(r"stable_miner_\d{3}"),
        "highest_unspent_allowance": LF(highest_allowance),
        "highest_unspent_allowance_ss58": Matcher(r"stable_miner_\d{3}"),
    }
    with set_block_number(1011), mock_manifest_endpoints_for_block_number(1011):
        with assert_system_events(
            [
                {
                    "type": "COMPUTE_TIME_ALLOWANCE",
                    "subtype": "MANIFEST_ERROR",
                    "data": {"hotkey": "malforming_miner_249"},
                },
                {
                    "type": "COMPUTE_TIME_ALLOWANCE",
                    "subtype": "MANIFEST_TIMEOUT",
                    "data": {"hotkey": "timing_out_miner_248"},
                },
            ]
        ):
            sync_manifests()
    new_resp = allowance().find_miners_with_allowance(
        1.0, ExecutorClass.always_on__llm__a6000, 1001
    )
    assert len(resp) - len(new_resp) == 82  # some manifests dropped
    for hotkey, allowance_ in new_resp:
        assert LF(dict(resp)[hotkey]) == allowance_, hotkey  # but nothing else should have changed

    for block_number in range(1011, 1102):
        if not block_number % 25:
            with (
                set_block_number(block_number),
                mock_manifest_endpoints_for_block_number(block_number),
            ):
                sync_manifests()
                with (
                    freeze_time(datetime.datetime(2025, 1, 1, 12, 0, 0)) as freezer,
                    mock.patch.object(blocks.time, "sleep", lambda s: freezer.tick(s)),  # type: ignore
                    caplog.at_level(
                        logging.CRITICAL,
                        logger="compute_horde_validator.validator.allowance.utils.blocks",
                    ),
                ):
                    tasks.scan_blocks_and_calculate_allowance(supertensor(), keep_running=False)
    assert get_metric_values_by_remaining_labels(
        VALIDATOR_ALLOWANCE_CHECKPOINT,
        {
            "validator_hotkey": get_test_validator_wallet().get_hotkey().ss58_address,
            "executor_class": ExecutorClass.always_on__llm__a6000.value,
        },
    ) == allowance_dict(
        json.loads((pathlib.Path(__file__).parent / "allowance_after_100_blocks.json").read_text())
    )

    assert len(get_metric_values_by_remaining_labels(VALIDATOR_ALLOWANCE_CHECKPOINT, {})) == 2898
    allowance_after_100_blocks = allowance().find_miners_with_allowance(
        1.0, ExecutorClass.always_on__llm__a6000, 1101
    )

    assert allowance_dict(allowance_after_100_blocks) == allowance_dict(
        json.loads((pathlib.Path(__file__).parent / "allowance_after_100_blocks.json").read_text())
    )
    # Let's check that various errors are consistent in terms of calculated available amount
    assert_error_messages(1101, 394.6701132404817)

    # Up until now nothing was relying on internals - only interfaces of other parts of the system were mocked - like
    # miner and subtensor responses. it does, however take too long to generate even more blocks so now we're diving
    # into internals and messing with the DB to test a case when there's a lot of data

    inject_blocks_with_allowances(900)

    allowance_after_1000_blocks = allowance().find_miners_with_allowance(
        1.0, ExecutorClass.always_on__llm__a6000, 1101
    )

    assert allowance_dict(allowance_after_1000_blocks) == allowance_dict(
        json.loads((pathlib.Path(__file__).parent / "allowance_after_1000_blocks.json").read_text())
    )

    assert_error_messages(1101, 1755.5362369976503)

    inject_blocks_with_allowances(100)
    allowance_after_1100_blocks = allowance().find_miners_with_allowance(
        1.0, ExecutorClass.always_on__llm__a6000, 1101
    )

    # no difference as these blocks are too old:
    assert allowance_dict(allowance_after_1100_blocks) == allowance_dict(
        json.loads((pathlib.Path(__file__).parent / "allowance_after_1000_blocks.json").read_text())
    )
    assert_error_messages(1101, 1755.5362369976503)

    # a quick test to check reserving works for huge numbers of blocks
    with assert_metric_observed(VALIDATOR_RESERVE_ALLOWANCE_DURATION, "reserve_allowance"):
        reservation_id, blocks_ = allowance().reserve_allowance(
            "stable_miner_081",
            ExecutorClass.always_on__llm__a6000,
            1755,
            1101,
        )

    assert blocks_ == list(range(379, 1100))

    # nothing left
    with pytest.raises(CannotReserveAllowanceException) as e2:
        allowance().reserve_allowance(
            "stable_miner_081",
            ExecutorClass.always_on__llm__a6000,
            1.0,
            1101,
        )
    assert e2.value.to_dict() == {
        "miner": "stable_miner_081",
        "required_allowance_seconds": LF(1.0),
        "available_allowance_seconds": LF(0.0),
    }

    with assert_metric_observed(
        VALIDATOR_UNDO_ALLOWANCE_RESERVATION_DURATION, "undo_allowance_reservation"
    ):
        allowance().undo_allowance_reservation(reservation_id)

    # now it's working again
    with assert_metric_observed(VALIDATOR_RESERVE_ALLOWANCE_DURATION, "reserve_allowance"):
        _, blocks_ = allowance().reserve_allowance(
            "stable_miner_081",
            ExecutorClass.always_on__llm__a6000,
            1755,
            1101,
        )

    assert blocks_ == list(range(379, 1100))

    assert BlockAllowance.objects.count() == 5041428
    assert AllowanceMinerManifest.objects.count() == 4593
    assert Block.objects.count() == 1101
    assert AllowanceBooking.objects.count() == 1

    with set_block_number(2906):
        evict_old_data()

    assert BlockAllowance.objects.count() == 1626900
    assert AllowanceMinerManifest.objects.count() == 4593
    assert Block.objects.count() == 360
    assert AllowanceBooking.objects.count() == 1

    with set_block_number(4000):
        evict_old_data()

    assert BlockAllowance.objects.count() == 0
    assert AllowanceMinerManifest.objects.count() == 0
    assert Block.objects.count() == 0
    assert AllowanceBooking.objects.count() == 0


@pytest.mark.django_db(transaction=True)
@pytest.mark.filterwarnings("ignore::pytest.PytestUnraisableExceptionWarning")
def test_blocks_out_of_order(configure_logs):
    for block_number in [1000, 1011, 1025, 1050, 1075, 1100]:
        with set_block_number(block_number), mock_manifest_endpoints_for_block_number(block_number):
            sync_manifests()
    block_numbers = list(range(1000, 1101))
    random.Random(42).shuffle(block_numbers)
    for block_number in block_numbers:
        with set_block_number(block_number):
            blocks.process_block_allowance_with_reporting(block_number, supertensor_=supertensor())

    with set_block_number(1101):
        allowance_after_100_blocks = allowance().find_miners_with_allowance(
            1.0, ExecutorClass.always_on__llm__a6000, 1101
        )

    assert allowance_dict(allowance_after_100_blocks) == allowance_dict(
        json.loads((pathlib.Path(__file__).parent / "allowance_after_100_blocks.json").read_text())
    )


@pytest.mark.django_db(transaction=True)
@pytest.mark.filterwarnings("ignore::pytest.PytestUnraisableExceptionWarning")
def test_allowance_reservation_corner_cases(configure_logs):
    """Test all corner cases of reserve_allowance, undo_allowance_reservation, and spend_allowance methods."""

    # Set up initial state with some blocks and allowances
    with set_block_number(1000), mock_manifest_endpoints_for_block_number(1000):
        manifests.sync_manifests()
        blocks.process_block_allowance_with_reporting(1000, supertensor_=supertensor())

    for block_number in range(1001, 1006):
        with set_block_number(block_number):
            blocks.process_block_allowance_with_reporting(block_number, supertensor_=supertensor())

    with set_block_number(1005):
        # Get miners with available allowance
        miners_with_allowance = allowance().find_miners_with_allowance(
            1.0, ExecutorClass.always_on__llm__a6000, 1005
        )
        test_miner = miners_with_allowance[0][0]  # Get the first miner's hotkey
        available_allowance = miners_with_allowance[0][1]  # Get available allowance

        first_reservation_amount = available_allowance * 0.3

        # Test Case 1: Successful reservation - normal case
        reservation_id_1, block_ids_1 = allowance().reserve_allowance(
            test_miner, ExecutorClass.always_on__llm__a6000, first_reservation_amount, 1005
        )

        assert block_ids_1 == [1000, 1001]

        # Use second miner for this test
        second_miner = miners_with_allowance[1][0]
        second_miner_allowance = miners_with_allowance[1][1]
        reservation_amount = second_miner_allowance * 0.3

        reservation_id_2, block_ids_2 = allowance().reserve_allowance(
            second_miner, ExecutorClass.always_on__llm__a6000, reservation_amount, 1005
        )

        # Verify second reservation is different
        assert reservation_id_2 != reservation_id_1
        assert block_ids_2 == [1000, 1001]

        gpu_miners = allowance().find_miners_with_allowance(
            1.0, ExecutorClass.always_on__gpu_24gb, 1005
        )
        gpu_miner = gpu_miners[0][0]
        gpu_allowance = gpu_miners[0][1]
        gpu_reservation_amount = min(gpu_allowance * 0.3, 3.0)

        reservation_id_3, block_ids_3 = allowance().reserve_allowance(
            gpu_miner, ExecutorClass.always_on__gpu_24gb, gpu_reservation_amount, 1005
        )

        assert reservation_id_3 not in [reservation_id_1, reservation_id_2]

        # Test Case 4: CannotReserveAllowanceException when not enough allowance
        # Find current available allowance for the test miner after first reservation
        current_miners = allowance().find_miners_with_allowance(
            1.0, ExecutorClass.always_on__llm__a6000, 1005
        )

        current_available = dict(current_miners)[test_miner]
        # Request more than currently available
        excessive_amount = current_available + 10.0

        with pytest.raises(CannotReserveAllowanceException):
            allowance().reserve_allowance(
                test_miner, ExecutorClass.always_on__llm__a6000, excessive_amount, 1005
            )

        # Test Case 5: Successful undo of valid reservation
        allowance().undo_allowance_reservation(reservation_id_1)

        # Test Case 6: After undo, same reservation can be made again
        reservation_id_1_new, block_ids_1_new = allowance().reserve_allowance(
            test_miner,
            ExecutorClass.always_on__llm__a6000,
            first_reservation_amount,  # Same amount as before
            1005,
        )

        # New reservation should have different ID but similar block allocation
        assert reservation_id_1_new != reservation_id_1
        assert block_ids_1_new == block_ids_1

        # Test Case 7: ReservationNotFound when undoing non-existent reservation
        with pytest.raises(ReservationNotFound):
            allowance().undo_allowance_reservation(999999)  # Non-existent ID

        # Test Case 8: ReservationNotFound when undoing already undone reservation
        with pytest.raises(ReservationNotFound):
            allowance().undo_allowance_reservation(reservation_id_1)  # Already undone

        # Test Case 9: Successful spend of valid reservation
        allowance().spend_allowance(reservation_id_2)

        # Test Case 10: ReservationNotFound when spending non-existent reservation
        with pytest.raises(ReservationNotFound):
            allowance().spend_allowance(888888)  # Non-existent ID

        # Test Case 11: Spending already spent reservation raises
        with pytest.raises(ReservationAlreadySpent):
            allowance().spend_allowance(reservation_id_2)

        # Test Case 12: ReservationNotFound when spending undone reservation
        allowance().undo_allowance_reservation(reservation_id_1_new)  # Undo first

        with pytest.raises(ReservationNotFound):
            allowance().spend_allowance(reservation_id_1_new)  # Try to spend undone reservation

        # Test Case 13: Appropriate values in NotEnoughAllowanceException
        reservations = []
        for ind, miner_allowance_ in enumerate(
            allowance().find_miners_with_allowance(1.0, ExecutorClass.always_on__llm__a6000, 1005)
        ):
            # half the miners we'll book fully, the other half we'll reserve at >50%
            miner, allowance_ = miner_allowance_
            res_id, _ = allowance().reserve_allowance(
                miner,
                ExecutorClass.always_on__llm__a6000,
                (allowance_ / (ind % 2 + 1)) - 1e-6,
                1005,
            )
            reservations.append(res_id)

        with pytest.raises(NotEnoughAllowanceException) as e1:
            allowance().find_miners_with_allowance(100.0, ExecutorClass.always_on__llm__a6000, 1005)
        assert e1.value.to_dict() == {
            "highest_available_allowance": LF(4.749230769230769),
            "highest_available_allowance_ss58": Matcher(r".*"),
            "highest_unspent_allowance": LF(11.86813186813187),
            "highest_unspent_allowance_ss58": Matcher(r"stable_miner_\d{3}"),
        }

        for res_id in reservations:
            allowance().spend_allowance(res_id)

        with pytest.raises(NotEnoughAllowanceException) as e2:
            allowance().find_miners_with_allowance(100.0, ExecutorClass.always_on__llm__a6000, 1005)
        assert e2.value.to_dict() == {
            "highest_available_allowance": LF(4.749230769230769),
            "highest_available_allowance_ss58": Matcher(r".*"),
            "highest_unspent_allowance": LF(4.749230769230769),
            "highest_unspent_allowance_ss58": Matcher(r".*"),
        }


@pytest.mark.django_db(transaction=True)
def test_manifests(configure_logs):
    with set_block_number(1000), mock_manifest_endpoints_for_block_number(1000):
        sync_manifests()

        miner_manifests = allowance().get_manifests()

        for validator_hotkey in VALIDATOR_HOTKEYS.values():
            executor_dict = miner_manifests.pop(validator_hotkey)
            total_executor_count = sum(executor_count for executor_count in executor_dict.values())
            # validators shouldn't have executors
            assert total_executor_count == 0

        for miner_hotkey in MINER_HOTKEYS.values():
            executor_dict = miner_manifests.pop(miner_hotkey)
            total_executor_count = sum(executor_count for executor_count in executor_dict.values())
            # miners must have executors
            assert total_executor_count != 0

        # all manifests should have been accounted for
        assert not miner_manifests

    # test with a block number after START_CHANGING_MANIFESTS_BLOCK
    with set_block_number(1020), mock_manifest_endpoints_for_block_number(1020):
        sync_manifests()

        miner_manifests = allowance().get_manifests()

        for validator_hotkey in VALIDATOR_HOTKEYS.values():
            executor_dict = miner_manifests.pop(validator_hotkey)
            total_executor_count = sum(executor_count for executor_count in executor_dict.values())
            # validators shouldn't have executors
            assert total_executor_count == 0

        # Build expected manifest values for 1000 and 1020
        responses_1000 = {hk: req for hk, req, _ in manifest_responses(1000)}
        responses_1020 = {hk: req for hk, req, _ in manifest_responses(1020)}
        target_ec = ExecutorClass.always_on__llm__a6000
        # Choose a miner whose manifest changes after 1010
        whacky_miner = next(h for h in MINER_HOTKEYS.values() if h.startswith("whacky_miner_"))

        # Ensure current manifests reflect the latest sync (1020) and not earlier (1000)
        assert whacky_miner in miner_manifests
        whacky_execs = miner_manifests[whacky_miner]
        assert (
            whacky_execs[target_ec] == responses_1020[whacky_miner][target_ec]  # type: ignore
        )
        assert (
            whacky_execs[target_ec] != responses_1000[whacky_miner][target_ec]  # type: ignore
        )

        # Do the same check for an always-increasing miner
        always_inc_miner = next(
            h for h in MINER_HOTKEYS.values() if h.startswith("always_increasing_miner_")
        )
        assert always_inc_miner in miner_manifests
        always_execs = miner_manifests[always_inc_miner]
        expected_inc_1020 = responses_1020[always_inc_miner][target_ec]  # type: ignore
        # sync_manifests clamps by max_executors_per_class; in tests this aligns with EXECUTOR_CAP
        expected_inc_1020_clamped = min(expected_inc_1020, EXECUTOR_CAP[target_ec])
        assert always_execs[target_ec] == expected_inc_1020_clamped
        assert (
            always_execs[target_ec] != responses_1000[always_inc_miner][target_ec]  # type: ignore
        )
