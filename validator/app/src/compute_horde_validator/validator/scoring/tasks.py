import contextlib
import logging
import random
import time
import traceback
from functools import cached_property
from typing import Union

import bittensor
import numpy as np
import turbobt.substrate
from asgiref.sync import async_to_sync
from bittensor import u16_normalized_float
from bittensor.utils.weight_utils import process_weights
from constance import config
from django.conf import settings
from django.db import transaction
from numpy._typing import NDArray
from pydantic import JsonValue

from compute_horde_validator.celery import app
from compute_horde_validator.validator.clean_me_up import bittensor_client
from compute_horde_validator.validator.locks import Locked, LockType, get_advisory_lock
from compute_horde_validator.validator.models import SystemEvent
from compute_horde_validator.validator.models.scoring.internal import (
    WeightSettingFinishedEvent,
)
from compute_horde_validator.validator.scoring import create_scoring_engine
from compute_horde_validator.validator.scoring.pylon_client import PylonClientError, pylon_client

if False:
    import torch

logger = logging.getLogger(__name__)

SCORING_ALGO_VERSION = 2


@contextlib.contextmanager
def save_event_on_error(subtype, exception_class=Exception):
    try:
        yield
    except exception_class:
        save_weight_setting_failure(subtype, traceback.format_exc(), {})
        raise


class MaximumNumberOfAttemptsExceeded(Exception):
    pass


@app.task
@save_event_on_error(
    SystemEvent.EventSubType.SUBTENSOR_CONNECTIVITY_ERROR,
    turbobt.substrate.exceptions.SubstrateException,
)
@bittensor_client
def set_scores(bittensor: turbobt.Bittensor):
    return
    if not config.SERVING:
        logger.warning("Not setting scores, SERVING is disabled in constance config")
        return

    commit_reveal_weights_enabled = config.DYNAMIC_COMMIT_REVEAL_WEIGHTS_ENABLED

    current_block = async_to_sync(bittensor.blocks.head)()

    if commit_reveal_weights_enabled:
        interval = CommitRevealInterval(current_block.number)

        if current_block.number not in interval.commit_window:
            logger.debug(
                "Outside of commit window, skipping, current block: %s, window: %s",
                current_block.number,
                interval.commit_window,
            )
            return

    with save_event_on_error(SystemEvent.EventSubType.GENERIC_ERROR):
        try:
            with transaction.atomic():
                try:
                    get_advisory_lock(LockType.WEIGHT_SETTING)
                except Locked:
                    logger.debug("Another thread already setting weights")
                    return

                subnet = bittensor.subnet(settings.BITTENSOR_NETUID)
                hyperparameters = async_to_sync(subnet.get_hyperparameters)(current_block.hash)
                neurons = async_to_sync(subnet.list_neurons)(current_block.hash)
                # TODO: refactor to use neurons from `allowance`
                wsfe, created = WeightSettingFinishedEvent.from_block(
                    current_block.number, settings.BITTENSOR_NETUID
                )
                if not created:
                    logger.debug(
                        f"Weights already set for cycle {wsfe.block_from}-{wsfe.block_to} (current_block={current_block.number})"
                    )
                    return
                save_weight_setting_event(
                    SystemEvent.EventType.WEIGHT_SETTING_INFO,
                    SystemEvent.EventSubType.SUCCESS,
                    "",
                    {
                        "DYNAMIC_COMMIT_REVEAL_WEIGHTS_INTERVAL": config.DYNAMIC_COMMIT_REVEAL_WEIGHTS_INTERVAL,
                        "DYNAMIC_COMMIT_REVEAL_COMMIT_START_OFFSET": config.DYNAMIC_COMMIT_REVEAL_COMMIT_START_OFFSET,
                        "DYNAMIC_COMMIT_REVEAL_COMMIT_END_BUFFER": config.DYNAMIC_COMMIT_REVEAL_COMMIT_END_BUFFER,
                        "DYNAMIC_COMMIT_REVEAL_REVEAL_END_BUFFER": config.DYNAMIC_COMMIT_REVEAL_REVEAL_END_BUFFER,
                        "DYNAMIC_COMMIT_REVEAL_WEIGHTS_ENABLED": config.DYNAMIC_COMMIT_REVEAL_WEIGHTS_ENABLED,
                        "current_block_number": current_block.number,
                    },
                )

                hotkey_scores = _score_cycles(current_block.number)

                if not hotkey_scores:
                    logger.warning("No scores calculated")

                uids, weights = normalize_batch_scores(
                    hotkey_scores,
                    neurons,
                    min_allowed_weights=hyperparameters["min_allowed_weights"],
                    max_weight_limit=hyperparameters["max_weights_limit"],
                )

                uids, weights = apply_dancing_burners(
                    uids,
                    weights,
                    neurons,
                    wsfe.block_from,
                    min_allowed_weights=hyperparameters["min_allowed_weights"],
                    max_weight_limit=hyperparameters["max_weights_limit"],
                )
                hk_by_uid = {n.uid: n.hotkey for n in neurons}
                hk_weight_mapping = {hk_by_uid[uid]: weight for uid, weight in zip(uids, weights)}

                for try_number in range(WEIGHT_SETTING_ATTEMPTS):
                    logger.debug(
                        f"Setting weights (attempt #{try_number}):\nuids={uids}\nscores={weights}"
                    )
                    try:
                        pylon_client().set_weights(hk_weight_mapping)
                        logger.info("Successfully set weights!!!")
                        break
                    except PylonClientError:
                        logger.warning(
                            "Encountered when setting weights (attempt #{try_number}): ",
                            exc_info=True,
                        )
                        save_weight_setting_failure(
                            subtype=SystemEvent.EventSubType.WRITING_TO_CHAIN_GENERIC_ERROR,
                            long_description=traceback.format_exc(),
                            data={"try_number": try_number, "operation": "setting/committing"},
                        )
                    time.sleep(WEIGHT_SETTING_FAILURE_BACKOFF)
                else:
                    raise MaximumNumberOfAttemptsExceeded()
                save_weight_setting_event(
                    type_=SystemEvent.EventType.WEIGHT_SETTING_SUCCESS,
                    subtype=SystemEvent.EventSubType.SET_WEIGHTS_SUCCESS,
                    long_description="",
                    data={},
                )
        except MaximumNumberOfAttemptsExceeded:
            msg = f"Failed to set weights after {WEIGHT_SETTING_ATTEMPTS} attempts"
            logger.warning(msg)
            save_weight_setting_failure(
                subtype=SystemEvent.EventSubType.GIVING_UP,
                long_description=msg,
                data={"try_number": WEIGHT_SETTING_ATTEMPTS, "operation": "setting/committing"},
            )


WEIGHT_SETTING_TTL = 60
WEIGHT_SETTING_HARD_TTL = 65
WEIGHT_SETTING_ATTEMPTS = 100
WEIGHT_SETTING_FAILURE_BACKOFF = 5


class CommitRevealInterval:
    """
    Commit-reveal interval for a given block.

    722                                    1444                                   2166
    |______________________________________|______________________________________|
    ^-1                                    ^-2                                    ^-3

    ^                     ^                ^                                      ^
    interval start        actual commit    interval end / reveal starts           ends

    Subtensor uses the actual subnet hyperparam to determine the interval:
    https://github.com/opentensor/subtensor/blob/af585b9b8a17d27508431257052da502055477b7/pallets/subtensor/src/subnets/weights.rs#L482

    Each interval is divided into reveal and commit windows based on dynamic parameters:

    722                                                                          1443
    |______________________________________|_______________________________________|
    ^                                      ^                              ^
    |-----------------------------|--------|------------------------------|-------_|
    |       REVEAL WINDOW         | BUFFER |        COMMIT WINDOW         | BUFFER |
    |                                      |
    |            COMMIT OFFSET             |

    """

    def __init__(
        self,
        current_block: int,
        *,
        length: int | None = None,
        commit_start_offset: int | None = None,
        commit_end_buffer: int | None = None,
        reveal_end_buffer: int | None = None,
    ):
        self.current_block = current_block
        self.length = length or config.DYNAMIC_COMMIT_REVEAL_WEIGHTS_INTERVAL
        self.commit_start_offset = (
            commit_start_offset or config.DYNAMIC_COMMIT_REVEAL_COMMIT_START_OFFSET
        )
        self.commit_end_buffer = commit_end_buffer or config.DYNAMIC_COMMIT_REVEAL_COMMIT_END_BUFFER
        self.reveal_end_buffer = reveal_end_buffer or config.DYNAMIC_COMMIT_REVEAL_REVEAL_END_BUFFER

    @cached_property
    def start(self):
        """
        https://github.com/opentensor/subtensor/blob/af585b9b8a17d27508431257052da502055477b7/pallets/subtensor/src/subnets/weights.rs#L488
        """
        return self.current_block - self.current_block % self.length

    @cached_property
    def stop(self):
        return self.start + self.length

    @property
    def reveal_start(self):
        return self.start

    @cached_property
    def reveal_stop(self):
        return self.start + self.commit_start_offset - self.reveal_end_buffer

    @property
    def reveal_window(self):
        return range(self.reveal_start, self.reveal_stop)

    @cached_property
    def commit_start(self):
        return self.start + self.commit_start_offset

    @cached_property
    def commit_stop(self):
        return self.stop - self.commit_end_buffer

    @property
    def commit_window(self):
        return range(self.commit_start, self.commit_stop)


def save_weight_setting_event(type_: str, subtype: str, long_description: str, data: JsonValue):
    SystemEvent.objects.using(settings.DEFAULT_DB_ALIAS).create(
        type=type_,
        subtype=subtype,
        long_description=long_description,
        data=data,
    )


def save_weight_setting_failure(subtype: str, long_description: str, data: JsonValue):
    save_weight_setting_event(
        type_=SystemEvent.EventType.WEIGHT_SETTING_FAILURE,
        subtype=subtype,
        long_description=long_description,
        data=data,
    )


def _normalize_weights_for_committing(weights: list[float], max_: int):
    factor = max_ / max(weights)
    return [round(w * factor) for w in weights]


def _get_subtensor_for_setting_scores(network):
    with save_event_on_error(SystemEvent.EventSubType.SUBTENSOR_CONNECTIVITY_ERROR):
        return bittensor.subtensor(network=network)


def _get_cycles_for_scoring(current_block: int) -> tuple[int, int]:
    """
    Determine current and previous cycle start blocks for scoring.

    Args:
        current_block: Current block number

    Returns:
        Tuple of (current_cycle_start, previous_cycle_start)
    """
    cycle_number = (current_block - 723) // 722
    current_cycle_start = 722 * cycle_number

    previous_cycle_start = 722 * (cycle_number - 1) if cycle_number > 0 else 0

    logger.info(
        f"Using test cycle calculation: current_block={current_block}, "
        f"cycle_number={cycle_number}, current_cycle_start={current_cycle_start}, "
        f"previous_cycle_start={previous_cycle_start}"
    )

    return current_cycle_start, previous_cycle_start


def _score_cycles(current_block: int) -> dict[str, float]:
    """
    Score cycles.

    Args:
        current_block: Current block number

    Returns:
        Dictionary mapping hotkey to score
    """
    current_cycle_start, previous_cycle_start = _get_cycles_for_scoring(current_block)

    if current_cycle_start is None or previous_cycle_start is None:
        logger.error("Could not determine cycles for scoring")
        return {}

    if previous_cycle_start < 0:
        logger.warning("Previous cycle start is negative, using 0")
        previous_cycle_start = 0

    engine = create_scoring_engine()

    logger.info(
        f"Calculating scores for cycles: current={current_cycle_start}, previous={previous_cycle_start}"
    )

    scores = engine.calculate_scores_for_cycles(
        current_cycle_start=current_cycle_start,
        previous_cycle_start=previous_cycle_start,
    )

    return scores


def normalize_batch_scores(
    hotkey_scores: dict[str, float],
    neurons: list[turbobt.Neuron],
    min_allowed_weights: int,
    max_weight_limit: int,
) -> tuple["torch.Tensor", "torch.FloatTensor"] | tuple[NDArray[np.int64], NDArray[np.float32]]:
    hotkey_to_uid = {n.hotkey: n.uid for n in neurons}
    score_per_uid = {}

    for hotkey, score in hotkey_scores.items():
        uid = hotkey_to_uid.get(hotkey)
        if uid is None:
            continue
        score_per_uid[uid] = score

    uids = np.zeros(len(neurons), dtype=np.int64)
    weights = np.zeros(len(neurons), dtype=np.float32)

    for ind, n in enumerate(neurons):
        uids[ind] = n.uid
        weights[ind] = score_per_uid.get(n.uid, 0)

    if not score_per_uid:
        logger.warning("Batch produced no scores")
        return uids, weights

    uids, weights = process_weights(
        uids,
        weights,
        len(neurons),
        min_allowed_weights,
        u16_normalized_float(max_weight_limit),
    )

    return uids, weights


def apply_dancing_burners(
    uids: Union["torch.Tensor", NDArray[np.int64]],
    weights: Union["torch.FloatTensor", NDArray[np.float32]],
    neurons: list[turbobt.Neuron],
    cycle_block_start: int,
    min_allowed_weights: int,
    max_weight_limit: int,
) -> tuple["torch.Tensor", "torch.FloatTensor"] | tuple[NDArray[np.int64], NDArray[np.float32]]:
    burner_hotkeys = config.DYNAMIC_BURN_TARGET_SS58ADDRESSES.split(",")
    burn_rate = config.DYNAMIC_BURN_RATE
    burn_partition = config.DYNAMIC_BURN_PARTITION

    hotkey_to_uid = {neuron.hotkey: neuron.uid for neuron in neurons}
    registered_burner_hotkeys = sorted(
        [hotkey for hotkey in burner_hotkeys if hotkey in hotkey_to_uid]
    )
    se_data = {
        "registered_burner_hotkeys": registered_burner_hotkeys,
        "burner_hotkeys": burner_hotkeys,
        "burn_rate": burn_rate,
        "burn_partition": burn_partition,
        "cycle_block_start": cycle_block_start,
    }

    if not registered_burner_hotkeys or not burn_rate:
        logger.info(
            "None of the burner hotkeys registered or burn_rate=0, not applying burn incentive"
        )
        SystemEvent.objects.using(settings.DEFAULT_DB_ALIAS).create(
            type=SystemEvent.EventType.BURNING_INCENTIVE,
            subtype=SystemEvent.EventSubType.NO_BURNING,
            long_description="",
            data=se_data,
        )
        return uids, weights

    if len(registered_burner_hotkeys) == 1:
        logger.info("Single burner hotkey registered, applying all burn incentive to it")
        weight_adjustment = {registered_burner_hotkeys[0]: burn_rate}
        main_burner = registered_burner_hotkeys[0]

    else:
        main_burner = random.Random(cycle_block_start).choice(registered_burner_hotkeys)
        logger.info(
            "Main burner: %s, other burners: %s",
            main_burner,
            [h for h in registered_burner_hotkeys if h != main_burner],
        )
        weight_adjustment = {
            main_burner: burn_rate * burn_partition,
            **{
                h: burn_rate * (1 - burn_partition) / (len(registered_burner_hotkeys) - 1)
                for h in registered_burner_hotkeys
                if h != main_burner
            },
        }

    SystemEvent.objects.using(settings.DEFAULT_DB_ALIAS).create(
        type=SystemEvent.EventType.BURNING_INCENTIVE,
        subtype=SystemEvent.EventSubType.APPLIED_BURNING,
        long_description="",
        data={**se_data, "main_burner": main_burner, "weight_adjustment": weight_adjustment},
    )

    weights = weights * (1 - burn_rate)

    for hotkey, weight in weight_adjustment.items():
        uid = hotkey_to_uid[hotkey]
        if uid not in uids:
            uids = np.append(uids, uid)
            weights = np.append(weights, weight)
        else:
            index = np.where(uids == uid)[0]
            weights[index] = weights[index] + weight

    uids, weights = process_weights(
        uids,
        weights,
        len(neurons),
        min_allowed_weights,
        u16_normalized_float(max_weight_limit),
    )

    return uids, weights
