import json
import pathlib
import random

import pytest

from compute_horde_core.executor_class import ExecutorClass
from .mockchain import set_block_number, manifest_responses, MINER_HOTKEYS
from .utils_for_tests import LF, allowance_dict, inject_blocks_with_allowances, assert_system_events, Matcher
from ..types import NotEnoughAllowanceException, CannotReserveAllowanceException
from ..utils import blocks, manifests
from ..utils.manifests import sync_manifests
from ...tests.helpers import patch_constance
from ..default import allowance


@pytest.mark.django_db(transaction=True)
def test_empty():
    with set_block_number(1000):
        with pytest.raises(NotEnoughAllowanceException) as e:
            allowance().find_miners_with_allowance(1.0, ExecutorClass.always_on__llm__a6000, 1000)
        assert e.value.to_dict() == {
            'highest_available_allowance': 0,
            'highest_available_allowance_ss58': '',
            'highest_unspent_allowance': 0,
            'highest_unspent_allowance_ss58': '',
        }
        with pytest.raises(CannotReserveAllowanceException) as e:
            allowance().reserve_allowance(
                MINER_HOTKEYS[0],
                ExecutorClass.always_on__llm__a6000,
                1.0,
                1000,
            )
        assert e.value.args == ('Not enough allowance from miner stable_miner_000. Required: 1.0, Available: 0.0',)



@pytest.mark.django_db(transaction=True)
def test_block_without_manifests():
    with set_block_number(1000):
        blocks.process_block_allowance_with_reporting(1000)
        with pytest.raises(NotEnoughAllowanceException) as e:
            allowance().find_miners_with_allowance(1.0, ExecutorClass.always_on__llm__a6000, 1000)
        assert e.value.to_dict() == {
            'highest_available_allowance': 0,
            'highest_available_allowance_ss58': '',
            'highest_unspent_allowance': 0,
            'highest_unspent_allowance_ss58': '',
        }
        with pytest.raises(CannotReserveAllowanceException) as e:
            allowance().reserve_allowance(
                MINER_HOTKEYS[0],
                ExecutorClass.always_on__llm__a6000,
                1.0,
                1000,
            )
        assert e.value.args == ('Not enough allowance from miner stable_miner_000. Required: 1.0, Available: 0.0',)

    with set_block_number(1001):
        blocks.process_block_allowance_with_reporting(1001)

        with pytest.raises(NotEnoughAllowanceException) as e:
            allowance().find_miners_with_allowance(1.0, ExecutorClass.always_on__llm__a6000, 1001)
        assert e.value.to_dict() == {
            'highest_available_allowance': 0,
            'highest_available_allowance_ss58': '',
            'highest_unspent_allowance': 0,
            'highest_unspent_allowance_ss58': '',
        }
        with pytest.raises(CannotReserveAllowanceException) as e:
            allowance().reserve_allowance(
                MINER_HOTKEYS[0],
                ExecutorClass.always_on__llm__a6000,
                1.0,
                1000,
            )
        assert e.value.args == ('Not enough allowance from miner stable_miner_000. Required: 1.0, Available: 0.0',)


@pytest.mark.django_db(transaction=True)
@pytest.mark.filterwarnings("ignore::pytest.PytestUnraisableExceptionWarning")
@patch_constance({
    "DYNAMIC_MINER_MAX_EXECUTORS_PER_CLASS": "always_on.llm.a6000=3,always_on.gpu-24gb=5,spin_up-4min.gpu-24gb=10"
})
def test_complete():
    with set_block_number(1000):
        manifests.sync_manifests()
        blocks.process_block_allowance_with_reporting(1000)
    with set_block_number(1001):
        blocks.process_block_allowance_with_reporting(1001)
        resp = allowance().find_miners_with_allowance(1.0, ExecutorClass.always_on__llm__a6000, 1001)
    highest_allowance = resp[0][1]
    number_of_executors = manifest_responses(1000)[0][1].manifest[ExecutorClass.always_on__llm__a6000]
    assert highest_allowance == number_of_executors * 11.99 * 9009.0 / (1001 + 4004 + 9009 + 16016 + 25025 + 36036)
    for block_number in range(1002, 1006):
        with set_block_number(block_number):
            blocks.process_block_allowance_with_reporting(block_number)

    with set_block_number(1004):
        assert "deregging_miner_247" in [n.hotkey_ss58 for n in allowance().neurons(block=1004)]
        assert "deregging_miner_247" in [el[0] for el in allowance().find_miners_with_allowance(1.0, ExecutorClass.always_on__llm__a6000, 1004)]

    resp = allowance().find_miners_with_allowance(1.0, ExecutorClass.always_on__llm__a6000, 1001)
    highest_allowance = resp[0][1]
    assert (
            LF(highest_allowance)
            ==
            number_of_executors * (11.99 + 3*12.00 + 12.01) * 9009.0 / (1001 + 4004 + 9009 + 16016 + 25025 + 36036)
    )
    for block_number in range(1006, 1011):
        with set_block_number(block_number):
            blocks.process_block_allowance_with_reporting(block_number)

    with set_block_number(1010):
        assert "deregging_miner_247" not in [n.hotkey_ss58 for n in allowance().neurons(block=1010)]
        assert "deregging_miner_247" not in [el[0] for el in allowance().find_miners_with_allowance(1.0, ExecutorClass.always_on__llm__a6000, 1010)]

    resp = allowance().find_miners_with_allowance(1.0, ExecutorClass.always_on__llm__a6000, 1001)
    highest_allowance = resp[0][1]
    assert (
            LF(highest_allowance)
            ==
            (
               number_of_executors * (11.99 + 3 * 12.00 + 12.01) * 9009.0 / (1001 + 4004 + 9009 + 16016 + 25025 + 36036)
               +
               number_of_executors * 11.99 * 9009.0 / (1001 + 4004 + 9009 + 16016 + 36036)  # stake loosing validator fell out
               +
               number_of_executors * 12.00 * 9021.6126 / (
                           1002.0009999999999 + 4008.8048000000003 + 9021.6126 + 16041.625600000001 + 25070.045000000002 + 36108.072)
               +
               number_of_executors * 12.00 * 9034.225199999999 / (
                       1003.002 + 4013.6096 + 9034.225199999999 + 16067.2512 + 25115.09 + 36180.144)
               +
               number_of_executors * 12.00 * 9046.8378 / (
                       1004.0029999999999 + 4018.4144 + 9046.8378 + 16092.876799999998 + 25160.135000000002)   # deregging validator fell out
               +
               number_of_executors * 12.01 * 9059.4504 / (
                       1005.004 + 4023.2191999999995 + 9059.4504 + 16118.5024 + 25205.180000000004 + 36324.288)  # stake loosing validator fell out
           )
   )
    with pytest.raises(NotEnoughAllowanceException) as e:
        allowance().find_miners_with_allowance(1000000000.0, ExecutorClass.always_on__llm__a6000, 1001)
    assert e.value.to_dict() == {
        'highest_available_allowance': LF(highest_allowance),
        'highest_available_allowance_ss58': Matcher(r"stable_miner_\d{3}"),
        'highest_unspent_allowance': LF(highest_allowance),
        'highest_unspent_allowance_ss58': Matcher(r"stable_miner_\d{3}"),
    }
    with set_block_number(1011):
        with assert_system_events([
            {'type': 'COMPUTE_TIME_ALLOWANCE', 'subtype': "MANIFEST_TIMEOUT",
             'data': {"hotkey": "malforming_miner_249"}},
            {'type': 'COMPUTE_TIME_ALLOWANCE', 'subtype': "MANIFEST_TIMEOUT",
             'data': {"hotkey": "timing_out_miner_248"}},
        ]):
            sync_manifests()
    new_resp = allowance().find_miners_with_allowance(1.0, ExecutorClass.always_on__llm__a6000, 1001)
    assert len(resp) - len(new_resp) == 82  # some manifests dropped
    for hotkey, allowance_ in new_resp:
        assert LF(dict(resp)[hotkey]) == allowance_, hotkey  # but nothing else should have changed
    for block_number in range(1011, 1101):
        with set_block_number(block_number):
            if not block_number % 25:
                sync_manifests()
            blocks.process_block_allowance_with_reporting(block_number)
    allowance_after_100_blocks = allowance().find_miners_with_allowance(
        1.0, ExecutorClass.always_on__llm__a6000, 1101)

    assert (
            allowance_dict(allowance_after_100_blocks)
            ==
            allowance_dict(json.loads((pathlib.Path(__file__).parent / 'allowance_after_100_blocks.json').read_text()))
    )

    # Up unitl now nothing was relying on internals - only interfaces of other parts of the system were mocked - like
    # miner and subtensor responses. it does, however take too long to generate even more blocks so now we're diving
    # into internals and messing with the DB to test a case when there's a lot of data

    inject_blocks_with_allowances(900)

    allowance_after_1000_blocks = allowance().find_miners_with_allowance(
        1.0, ExecutorClass.always_on__llm__a6000, 1101)

    assert (
            allowance_dict(allowance_after_1000_blocks)
            ==
            allowance_dict(json.loads((pathlib.Path(__file__).parent / 'allowance_after_1000_blocks.json').read_text()))
    )

    inject_blocks_with_allowances(100)
    allowance_after_1100_blocks = allowance().find_miners_with_allowance(
        1.0, ExecutorClass.always_on__llm__a6000, 1101)

    # no difference as these blocks are too old:
    assert (
            allowance_dict(allowance_after_1100_blocks)
            ==
            allowance_dict(json.loads((pathlib.Path(__file__).parent / 'allowance_after_1000_blocks.json').read_text()))
    )

    # a quick test to check reserving works for huge numbers of blocks
    reservation_id, blocks_ = allowance().reserve_allowance(
        "stable_miner_081",
        ExecutorClass.always_on__llm__a6000,
        1755,
        1101,
    )
    assert blocks_ == list(range(379, 1100))

    # nothing left
    with pytest.raises(CannotReserveAllowanceException) as e:
        allowance().reserve_allowance(
            "stable_miner_081",
            ExecutorClass.always_on__llm__a6000,
            1.0,
            1101,
        )
    assert e.value.args == ('Not enough allowance from miner stable_miner_081. Required: 1.0, Available: 0.0',)

    allowance().undo_allowance_reservation(reservation_id)

    # now it's working again
    _, blocks_ = allowance().reserve_allowance(
        "stable_miner_081",
        ExecutorClass.always_on__llm__a6000,
        1755,
        1101,
    )
    assert blocks_ == list(range(379, 1100))


@pytest.mark.django_db(transaction=True)
@pytest.mark.filterwarnings("ignore::pytest.PytestUnraisableExceptionWarning")
@patch_constance({
    "DYNAMIC_MINER_MAX_EXECUTORS_PER_CLASS": "always_on.llm.a6000=3,always_on.gpu-24gb=5,spin_up-4min.gpu-24gb=10"
})
def test_blocks_out_of_order():
    for block_number in [1000, 1011, 1025, 1050, 1075, 1100]:
        with set_block_number(block_number):
            sync_manifests()
    block_numbers = list(range(1000, 1101))
    random.Random(42).shuffle(block_numbers)
    for block_number in block_numbers:
        with set_block_number(block_number):
            blocks.process_block_allowance_with_reporting(block_number)

    with set_block_number(1101):
        allowance_after_100_blocks = allowance().find_miners_with_allowance(
            1.0, ExecutorClass.always_on__llm__a6000, 1101)

    assert (
            allowance_dict(allowance_after_100_blocks)
            ==
            allowance_dict(json.loads((pathlib.Path(__file__).parent / 'allowance_after_100_blocks.json').read_text()))
    )

# TODO: assert system events on errors such as subtensor connectivity