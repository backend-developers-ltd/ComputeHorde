import time
from collections import defaultdict

import turbobt
from celery.utils.log import get_task_logger
from django.db import transaction

from compute_horde_core.executor_class import ExecutorClass

from compute_horde_validator.validator.locks import Lock, LockType

from .manifests import get_manifests, get_manifest_drops
from .supertensor import supertensor
from .types import ss58_address
from ..models.internal import Block, BlockAllowance, Neuron as NeuronModel
from ..settings import BLOCK_LOOKBACK, BLOCK_EXPIRY
from ..base import NotEnoughAllowanceException
from ...models import SystemEvent

logger = get_task_logger(__name__)


def find_missing_blocks(current_block: int) -> list[int]:
    """
    Find missing block numbers from the Block table based on BLOCK_LOOKBACK and current_block.

    Args:
        current_block: The current block number

    Returns:
        List of missing block numbers within the lookback range
    """
    # Calculate the lookback block number
    lookback_block = current_block - BLOCK_LOOKBACK

    # Get all block numbers that should exist within the lookback range
    expected_block_numbers = set(range(lookback_block, current_block))

    # Get existing block numbers from the database
    existing_blocks = Block.objects.filter(
        block_number__gte=lookback_block,
        block_number__lt=current_block
    ).values_list('block_number', flat=True)
    existing_block_numbers = set(existing_blocks)

    # Find missing block numbers
    missing_block_numbers = expected_block_numbers - existing_block_numbers

    # Return sorted list of missing block numbers
    return sorted(list(missing_block_numbers))


MAX_RUN_TIME = 300
MIN_BLOCK_WAIT_TIME = 15


class TimesUpError(Exception):
    pass


class Timer:
    def __init__(self):
        self.start_time = time.time()

    def check_time(self):
        if self.time_left() < 0:
            raise TimesUpError

    def time_left(self):
        return MAX_RUN_TIME - (time.time() - self.start_time)


def wait_for_block(target_block: int, timeout_seconds: float):
    timeout_seconds = max(timeout_seconds, MIN_BLOCK_WAIT_TIME)
    start = time.time()
    while target_block > supertensor().get_current_block():
        if time.time() - start > timeout_seconds:
            logger.warning(f"Timeout waiting for block {target_block} after {timeout_seconds} seconds")
            raise TimesUpError(f"Timeout waiting for block {target_block}")
        time.sleep(0.1)


def get_stake_share(validator_list: list[turbobt.Neuron], validator: turbobt.Neuron):
    try:
        return validator.stake / sum(validator.stake for validator in validator_list)
    except ZeroDivisionError:
        return 0.0


def save_neurons(neurons: list[turbobt.Neuron], block: int):
    NeuronModel.objects.bulk_create([
        NeuronModel(
            hotkey_ss58address=neuron.hotkey,
            coldkey_ss58address=neuron.coldkey,
            block=block,
        )
        for neuron in neurons
    ])


def process_block_allowance(block_number: int):
    with transaction.atomic():
        block_obj, created = Block.objects.create(
            block_number=block_number,
            creation_timestamp=supertensor().get_block_timestamp(block_number),
        )

        finalized_blocks = []

        # Check if next block exists (would finalize current block)
        try:
            next_block = Block.objects.filter(block_number=block_number + 1).get()
        except Block.DoesNotExist:
            pass
        else:
            block_obj.end_timestamp = next_block.creation_timestamp
            block_obj.save()
            finalized_blocks.append(block_obj)

        # Check if current block would finalize previous block
        try:
            prev_block = Block.objects.get(block_number=block_number - 1)
        except Block.DoesNotExist:
            pass
        else:
            if prev_block.end_timestamp is None:
                prev_block.end_timestamp = block_obj.creation_timestamp
                prev_block.save()
                finalized_blocks.append(prev_block)

        for finalized_block in finalized_blocks:

            neurons = supertensor().list_neurons(finalized_block.block_number)
            save_neurons(neurons, finalized_block.block_number)

            validators = supertensor().list_validators(finalized_block.block_number)

            hotkeys_from_metagraph = [neuron.hotkey for neuron in neurons]

            with Lock(LockType.ALLOWANCE_BLOCK_INJECTION, 10.0):
                # This will throw an error if the lock cannot be obtained in 10.0s and that's correct
                manifests = get_manifests(finalized_block.block_number, hotkeys_from_metagraph)
                drops = get_manifest_drops(finalized_block.block_number, hotkeys_from_metagraph)
                new_block_allowances = []

                for neuron in neurons:
                    for validator in validators:
                        for executor_class in ExecutorClass:
                            new_block_allowances.append(BlockAllowance(
                                block=finalized_block,
                                allowance=(
                                        manifests.get((neuron.hotkey, executor_class), 0.0)
                                        * get_stake_share(validators, validator)
                                        * (
                                                    finalized_block.end_timestamp - finalized_block.creation_timestamp).total_seconds()
                                ),
                                miner_ss58=neuron.hotkey,
                                validator_ss58=validator.hotkey,
                                executor_class=executor_class,
                                invalidated_at_block=drops.get((neuron.hotkey, executor_class), None),
                            ))
                if new_block_allowances:
                    BlockAllowance.objects.bulk_create(new_block_allowances)
                    logger.info(f"Created {len(new_block_allowances)} block allowances for block "
                                f"{finalized_block.block_number}")


def process_block_allowance_with_reporting(block_number: int):
    try:
        start = time.time()
        process_block_allowance(block_number)
        end = time.time()
    except Exception as e:
        logger.error(f"Error processing block allowance for block {block_number}: {e!r}", exc_info=True)
        SystemEvent.objects.create(
            event_type=SystemEvent.EventType.COMPUTE_TIME_ALLOWANCE,
            event_subtype=SystemEvent.EventSubType.FAILURE,
            data={
                "block_number": block_number,
                "error": str(e),
            },
        )
    else:
        logger.info(f"Block allowance processing for block {block_number} took {end - start} seconds")
        SystemEvent.objects.create(
            event_type=SystemEvent.EventType.COMPUTE_TIME_ALLOWANCE,
            event_subtype=SystemEvent.EventSubType.SUCCESS,
            data={
                "block_number": block_number,
                "duration_seconds": end - start,
            },
        )
    if not block_number % 50:
        report_checkpoint(block_number)


def report_checkpoint(block_number):
    allowances = BlockAllowance.objects.filter(
        block__block_number__le=block_number,
        block__block_number__gt=block_number - 50,
    ).order_by("block__block_number", "validator_ss58", "miner_ss58", "executor_class")

    by_block = defaultdict(list)
    for allowance in allowances:
        by_block[str(allowance.block.block_number)].append(allowance)

    SystemEvent.objects.create(
        event_type=SystemEvent.EventType.COMPUTE_TIME_ALLOWANCE,
        event_subtype=SystemEvent.EventSubType.CHECKPOINT,
        data={
            "block_number": block_number,
            "allowances": by_block,
        },
    )


def scan_blocks_and_calculate_allowance():
    timer = Timer()
    try:
        current_block = supertensor().get_current_block()
        missing_block_numbers = find_missing_blocks(current_block)
        for block_number in missing_block_numbers:
            process_block_allowance_with_reporting(block_number)
            timer.check_time()

        while True:
            wait_for_block(current_block + 1, timer.time_left())
            current_block += 1

            process_block_allowance_with_reporting(current_block)
            timer.check_time()

    except TimesUpError:
        logger.debug("scan_blocks_and_calculate_allowance times out gracefully, spawning a new task")
        raise


def find_miners_with_allowance(
    required_allowance: float,
    executor_class: ExecutorClass,
    job_start_block: int,
    validator_ss58: ss58_address,
)-> list(tuple(ss58_address, float)):
    """
    Find miners that have at least the required amount of allowance left.

    Returns miners sorted by:
    1. Earliest unspent/unreserved block
    2. Highest total allowance percentage left

    Args:
        required_allowance: The minimum allowance amount required (in seconds)
        executor_class: executor class
        job_start_block: used to determine which blocks can be used for the reservation, as per block expiry rules
        validator_ss58: validator's ss58address

    Returns:
        List of miners that meet the allowance requirements, sorted appropriately (see README).
        The returned list is always not empty. If there are no miners with enough allowance,
        NotEnoughAllowanceException is raised. The returned miners are present in the subnet's metagraph snapshot
        kept by this module.
    """

    