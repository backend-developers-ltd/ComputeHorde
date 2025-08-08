import time
from collections import defaultdict
from collections.abc import Callable
from typing import Any

import turbobt
from celery.utils.log import get_task_logger
from compute_horde_core.executor_class import ExecutorClass
from django.db import transaction
from django.db.models import Case, FloatField, Max, Min, Q, Sum, Value, When
from django.forms.models import model_to_dict

from compute_horde_validator.validator.locks import Lock, LockType

from ...models import SystemEvent
from ...models.allowance.internal import Block, BlockAllowance
from ...models.allowance.internal import Neuron as NeuronModel
from .. import settings
from ..metrics import VALIDATOR_BLOCK_ALLOWANCE_PROCESSING_DURATION, VALIDATOR_ALLOWANCE_CHECKPOINT
from ..types import AllowanceException, NotEnoughAllowanceException, ss58_address
from .manifests import get_manifest_drops, get_manifests
from .supertensor import supertensor, SuperTensor

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
    lookback_block = current_block - settings.BLOCK_LOOKBACK

    # Get all block numbers that should exist within the lookback range
    expected_block_numbers = set(range(lookback_block, current_block))

    # Get existing block numbers from the database
    existing_blocks = Block.objects.filter(
        block_number__gte=lookback_block, block_number__lt=current_block
    ).values_list("block_number", flat=True)
    existing_block_numbers = set(existing_blocks)

    # Find missing block numbers
    missing_block_numbers = expected_block_numbers - existing_block_numbers

    # Return sorted list of missing block numbers
    return sorted(list(missing_block_numbers))


MAX_RUN_TIME = 90
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
            logger.warning(
                f"Timeout waiting for block {target_block} after {timeout_seconds} seconds"
            )
            raise TimesUpError(f"Timeout waiting for block {target_block}")
        time.sleep(0.1)


def get_stake_share(validator_list: list[turbobt.Neuron], validator: turbobt.Neuron):
    try:
        return validator.stake / sum(validator.stake for validator in validator_list)
    except ZeroDivisionError:
        return 0.0


def save_neurons(neurons: list[turbobt.Neuron], block: int):
    NeuronModel.objects.bulk_create(
        [
            NeuronModel(
                hotkey_ss58address=neuron.hotkey,
                coldkey_ss58address=neuron.coldkey,
                block=block,
            )
            for neuron in neurons
        ]
    )


def process_block_allowance(block_number: int, supertensor_: SuperTensor):
    """
    Only call this once the block is already minted
    """
    with transaction.atomic():
        block_obj = Block.objects.create(
            block_number=block_number,
            creation_timestamp=supertensor_.get_block_timestamp(block_number),
        )

        neurons = supertensor_.list_neurons(block_number)
        save_neurons(neurons, block_number)

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
            assert finalized_block.end_timestamp is not None

            neurons = supertensor_.list_neurons(finalized_block.block_number)

            validators = supertensor_.list_validators(finalized_block.block_number)

            hotkeys_from_metagraph = [neuron.hotkey for neuron in neurons]

            with Lock(LockType.ALLOWANCE_BLOCK_INJECTION, 10.0):
                # This will throw an error if the lock cannot be obtained in 10.0s and that's correct
                manifests = get_manifests(finalized_block.block_number, hotkeys_from_metagraph)
                drops = get_manifest_drops(finalized_block.block_number, hotkeys_from_metagraph)
                new_block_allowances = []

                for neuron in neurons:
                    for validator in validators:
                        for executor_class in ExecutorClass:
                            new_block_allowances.append(
                                BlockAllowance(
                                    block=finalized_block,
                                    allowance=(
                                        manifests.get((neuron.hotkey, executor_class), 0.0)
                                        * get_stake_share(validators, validator)
                                        * (
                                            finalized_block.end_timestamp
                                            - finalized_block.creation_timestamp
                                        ).total_seconds()
                                    ),
                                    miner_ss58=neuron.hotkey,
                                    validator_ss58=validator.hotkey,
                                    executor_class=executor_class,
                                    invalidated_at_block=drops.get(
                                        (neuron.hotkey, executor_class), None
                                    ),
                                )
                            )
                if new_block_allowances:
                    BlockAllowance.objects.bulk_create(new_block_allowances)
                    logger.info(
                        f"Created {len(new_block_allowances)} block allowances for block "
                        f"{finalized_block.block_number}"
                    )


def process_block_allowance_with_reporting(block_number: int, supertensor_: SuperTensor):
    """
    Only call this once the block is already minted
    """
    try:
        start = time.time()
        process_block_allowance(block_number, supertensor_)
        end = time.time()
    except Exception as e:
        logger.error(
            f"Error processing block allowance for block {block_number}: {e!r}", exc_info=True
        )
        SystemEvent.objects.create(
            type=SystemEvent.EventType.COMPUTE_TIME_ALLOWANCE,
            subtype=SystemEvent.EventSubType.FAILURE,
            data={
                "block_number": block_number,
                "error": str(e),
            },
        )
    else:
        duration = end - start
        logger.info(f"Block allowance processing for block {block_number} took {duration} seconds")
        # Record processing duration in Prometheus metric instead of SystemEvent
        VALIDATOR_BLOCK_ALLOWANCE_PROCESSING_DURATION.observe(duration)


def report_checkpoint(block_number_lt: int, block_number_gte: int):
    allowances = BlockAllowance.objects.filter(
        block__block_number__lt=block_number_lt,
        block__block_number__gte=block_number_gte,
    ).values(
        "miner_ss58",
        "validator_ss58",
        "executor_class",
    ).annotate(
        total_allowance=Sum("allowance")
    ).filter(
        total_allowance__gt=0
    )

    for allowance in allowances:
        VALIDATOR_ALLOWANCE_CHECKPOINT.labels(
            allowance['miner_ss58'],
            allowance['validator_ss58'],
            allowance['executor_class'],
            block_number_lt,
        ).set(allowance['total_allowance'])
        print(
            f"Miner: {allowance['miner_ss58']}, "
            f"Validator: {allowance['validator_ss58']}, "
            f"Executor: {allowance['executor_class']}, "
            f"Total Allowance: {allowance['total_allowance']}"
        )


def scan_blocks_and_calculate_allowance(
    report_callback: Callable[[int, int], Any] | None = None,
    backfilling_supertensor: SuperTensor | None = None,
    livefilling_supertensor: SuperTensor | None = None,
):
    if backfilling_supertensor is None:
        backfilling_supertensor = supertensor()
    if livefilling_supertensor is None:
        livefilling_supertensor = supertensor()
    timer = Timer()
    try:
        current_block = livefilling_supertensor.get_current_block()
        missing_block_numbers = find_missing_blocks(current_block)
        for block_number in missing_block_numbers:
            # TODO process_block_allowance_with_reporting never throws, but logs errors appropriately. maybe it should
            # be retried? otherwise random failures will leave holes until they are backfilled
            process_block_allowance_with_reporting(block_number, backfilling_supertensor)
            if not block_number % 100 and report_callback:
                report_callback(block_number, block_number-10)
            timer.check_time()

        while True:
            wait_for_block(current_block + 1, timer.time_left())
            current_block += 1

            process_block_allowance_with_reporting(current_block, livefilling_supertensor)
            # no precaching needed in live sampling
            if not current_block % 100 and report_callback:
                report_callback(current_block, current_block-100)
            timer.check_time()

    except TimesUpError:
        logger.debug(
            "scan_blocks_and_calculate_allowance times out gracefully, spawning a new task"
        )
        raise


def find_miners_with_allowance(
    allowance_seconds: float,
    executor_class: ExecutorClass,
    job_start_block: int,
    validator_ss58: ss58_address,
) -> list[tuple[ss58_address, float]]:
    """
    Find miners that have at least the required amount of allowance left.

    Returns miners sorted by:
    1. Earliest unspent/unreserved block
    2. Highest total allowance percentage left
    3. Highest total allowance left

    Args:
        allowance_seconds: The minimum allowance amount required (in seconds)
        executor_class: executor class
        job_start_block: used to determine which blocks can be used for the reservation, as per block expiry rules
        validator_ss58: validator's ss58address

    Returns:
        List of miners that meet the allowance requirements, sorted appropriately (see README).
        The returned list is always not empty. If there are no miners with enough allowance,
        NotEnoughAllowanceException is raised. The returned miners are present in the subnet's metagraph snapshot
        kept by this module.
    """
    if allowance_seconds > settings.MAX_JOB_RUN_TIME:
        raise AllowanceException(
            f"Required allowance cannot be greater than {settings.MAX_JOB_RUN_TIME} seconds"
        )

    earliest_usable_block = job_start_block - settings.BLOCK_EXPIRY

    miner_aggregates = (
        BlockAllowance.objects.filter(
            validator_ss58=validator_ss58,
            executor_class=executor_class,
            block__block_number__gte=earliest_usable_block,
            invalidated_at_block__isnull=True,  # Only non-invalidated allowances
            miner_ss58__in=NeuronModel.objects.filter(
                block=NeuronModel.objects.aggregate(Max("block"))["block__max"]
            ).values_list("hotkey_ss58address", flat=True),
        )
        .values("miner_ss58")
        .annotate(
            # Total allowance for non-invalidated allowances
            total_allowance=Sum("allowance"),
            # Available allowance: sum where booking is null OR (not spent AND not reserved)
            available_allowance=Sum(
                Case(
                    When(
                        Q(allowance_booking__isnull=True)
                        | Q(
                            allowance_booking__is_spent=False, allowance_booking__is_reserved=False
                        ),
                        then="allowance",
                    ),
                    default=Value(0.0),
                    output_field=FloatField(),
                )
            ),
            # Unspent allowance: sum where booking is null OR booking has is_spent=False
            unspent_allowance=Sum(
                Case(
                    When(
                        Q(allowance_booking__isnull=True) | Q(allowance_booking__is_spent=False),
                        then="allowance",
                    ),
                    default=Value(0.0),
                    output_field=FloatField(),
                )
            ),
            # Earliest unspent block: minimum block number where allowance is available
            earliest_unspent_block=Min(
                Case(
                    When(Q(allowance_booking__isnull=True), then="block__block_number"),
                    default=None,
                )
            ),
        )
    )

    # Convert to dictionary for easier access
    miner_data = {
        agg["miner_ss58"]: {
            "total_allowance": agg["total_allowance"] or 0.0,
            "available_allowance": agg["available_allowance"] or 0.0,
            "unspent_allowance": agg["unspent_allowance"] or 0.0,
            "earliest_unspent_block": agg["earliest_unspent_block"],
        }
        for agg in miner_aggregates
    }

    # Filter miners with sufficient allowance
    eligible_miners = []
    highest_available_allowance = 0.0
    highest_available_allowance_ss58 = None
    highest_unspent_allowance = 0.0
    highest_unspent_allowance_ss58 = None

    for miner_ss58, data in miner_data.items():
        available = data["available_allowance"]
        total = data["total_allowance"]
        unspent = data["unspent_allowance"]

        # Track highest allowances for exception handling
        if available > highest_available_allowance:
            highest_available_allowance = available
            highest_available_allowance_ss58 = miner_ss58

        if unspent > highest_unspent_allowance:
            highest_unspent_allowance = unspent
            highest_unspent_allowance_ss58 = miner_ss58

        # Check if miner has sufficient allowance
        if available >= allowance_seconds:
            # Calculate allowance percentage left (available / total)
            allowance_percentage = available / total if total > 0 else 0.0
            earliest_block = data["earliest_unspent_block"] or float("inf")

            eligible_miners.append((miner_ss58, available, earliest_block, allowance_percentage))

    # If no miners have enough allowance, raise exception
    if not eligible_miners:
        raise NotEnoughAllowanceException(
            highest_available_allowance=highest_available_allowance,
            highest_available_allowance_ss58=highest_available_allowance_ss58 or "",
            highest_unspent_allowance=highest_unspent_allowance,
            highest_unspent_allowance_ss58=highest_unspent_allowance_ss58 or "",
        )

    # Sort miners by:
    # 1. Earliest unspent/unreserved block (ascending)
    # 2. Highest total allowance percentage left (descending)
    # 3. Highest total allowance left
    eligible_miners.sort(key=lambda x: (x[2], -x[3], -x[1]))

    # Return list of tuples (miner_ss58, available_allowance)
    return [(miner[0], miner[1]) for miner in eligible_miners]
