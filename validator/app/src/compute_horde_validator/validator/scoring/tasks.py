import random
import time
import traceback
from datetime import timedelta
from functools import cached_property
from typing import Union

import billiard.exceptions
import bittensor
import celery.exceptions
import numpy as np
import turbobt.substrate
from asgiref.sync import async_to_sync
from bittensor import u16_normalized_float
from bittensor.core.errors import SubstrateRequestException
from bittensor.utils.weight_utils import process_weights
from celery.result import allow_join_result
from constance import config
from django.conf import settings
from django.db import transaction
from django.utils.timezone import now
from numpy._typing import NDArray
from pydantic import JsonValue

from compute_horde_validator.celery import app
from compute_horde_validator.validator.clean_me_up import bittensor_client
from compute_horde_validator.validator.locks import get_advisory_lock, LockType, Locked
from compute_horde_validator.validator.models import SystemEvent, SyntheticJobBatch, Weights
from compute_horde_validator.validator.scoring import create_scoring_engine
from compute_horde_validator.validator.tasks import save_event_on_error, logger, SCORING_ALGO_VERSION, \
    _normalize_weights_for_committing


@app.task
@save_event_on_error(
    SystemEvent.EventSubType.SUBTENSOR_CONNECTIVITY_ERROR,
    turbobt.substrate.exceptions.SubstrateException,
)
@bittensor_client
def set_scores(bittensor: turbobt.Bittensor):
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
        with transaction.atomic():
            try:
                get_advisory_lock(LockType.WEIGHT_SETTING)
            except Locked:
                logger.debug("Another thread already setting weights")
                return

            subnet = bittensor.subnet(settings.BITTENSOR_NETUID)
            hyperparameters = async_to_sync(subnet.get_hyperparameters)(current_block.hash)
            neurons = async_to_sync(subnet.list_neurons)(current_block.hash)

            batches = list(
                SyntheticJobBatch.objects.select_related("cycle")
                .filter(
                    scored=False,
                    should_be_scored=True,
                    started_at__gte=now() - timedelta(days=1),
                    cycle__stop__lt=current_block.number,
                )
                .order_by("started_at")
            )
            if not batches:
                logger.info("No batches - nothing to score")
                return
            if len(batches) > 1:
                logger.error("Unexpected number batches eligible for scoring: %s", len(batches))
                for batch in batches[:-1]:
                    batch.scored = True
                    batch.save()
                batches = [batches[-1]]

            logger.info(
                "Selected batches for scoring: [%s]",
                ", ".join(str(batch.id) for batch in batches),
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
                batches[-1].cycle.start,
                min_allowed_weights=hyperparameters["min_allowed_weights"],
                max_weight_limit=hyperparameters["max_weights_limit"],
            )

            for batch in batches:
                batch.scored = True
                batch.save()

            for try_number in range(WEIGHT_SETTING_ATTEMPTS):
                logger.debug(
                    f"Setting weights (attempt #{try_number}):\nuids={uids}\nscores={weights}"
                )
                success = False

                try:
                    result = do_set_weights.apply_async(
                        kwargs=dict(
                            netuid=settings.BITTENSOR_NETUID,
                            uids=uids.tolist(),
                            weights=weights.tolist(),
                            wait_for_inclusion=True,
                            wait_for_finalization=False,
                            version_key=SCORING_ALGO_VERSION,
                        ),
                        soft_time_limit=WEIGHT_SETTING_TTL,
                        time_limit=WEIGHT_SETTING_HARD_TTL,
                    )
                    logger.info(f"Setting weights task id: {result.id}")
                    try:
                        with allow_join_result():
                            success, msg = result.get(timeout=WEIGHT_SETTING_TTL)
                    except (celery.exceptions.TimeoutError, billiard.exceptions.TimeLimitExceeded):
                        result.revoke(terminate=True)
                        logger.info(f"Setting weights timed out (attempt #{try_number})")
                        save_weight_setting_failure(
                            subtype=SystemEvent.EventSubType.WRITING_TO_CHAIN_TIMEOUT,
                            long_description=traceback.format_exc(),
                            data={"try_number": try_number, "operation": "setting/committing"},
                        )
                        continue
                except Exception:
                    logger.warning("Encountered when setting weights: ", exc_info=True)
                    save_weight_setting_failure(
                        subtype=SystemEvent.EventSubType.WRITING_TO_CHAIN_GENERIC_ERROR,
                        long_description=traceback.format_exc(),
                        data={"try_number": try_number, "operation": "setting/committing"},
                    )
                    continue
                if success:
                    break
                time.sleep(WEIGHT_SETTING_FAILURE_BACKOFF)
            else:
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


class WeightsRevealError(Exception):
    pass


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
    try:
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

        if scores:
            _mark_cycle_as_scored(current_cycle_start)

        return scores

    except Exception as e:
        logger.error(f"Failed to score cycles directly: {e}")
        return {}


def _mark_cycle_as_scored(current_cycle_start: int):
    """
    Mark the current cycle as scored by updating any related batches.

    Args:
        current_cycle_start: Current cycle start block
    """
    try:
        batches_updated = SyntheticJobBatch.objects.filter(
            cycle__start=current_cycle_start,
            scored=False,
        ).update(scored=True)

        if batches_updated > 0:
            logger.info(
                f"Marked {batches_updated} batches as scored for cycle {current_cycle_start}"
            )

    except Exception as e:
        logger.warning(f"Failed to mark cycle {current_cycle_start} as scored: {e}")


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


@app.task()
def reveal_scores() -> None:
    """
    Select latest Weights that are older than `commit_reveal_weights_interval`
    and haven't been revealed yet, and reveal them.
    """
    last_weights = Weights.objects.order_by("-created_at").first()
    if not last_weights or last_weights.revealed_at is not None:
        logger.debug("No weights to reveal")
        return

    subtensor_ = _get_subtensor_for_setting_scores(network=settings.BITTENSOR_NETWORK)
    current_block = subtensor_.get_current_block()
    interval = CommitRevealInterval(current_block)
    if current_block not in interval.reveal_window:
        logger.debug(
            "Outside of reveal window, skipping, current block: %s, window: %s",
            current_block,
            interval.reveal_window,
        )
        return

    # find the interval in which the commit occurred
    block_interval = CommitRevealInterval(last_weights.block)
    # revealing starts in the next interval
    reveal_start = block_interval.stop

    if current_block < reveal_start:
        logger.warning(
            "Too early to reveal weights weights_id: %s, reveal starts: %s, current block: %s",
            last_weights.pk,
            reveal_start,
            current_block,
        )
        return

    reveal_end = reveal_start + block_interval.length
    if current_block > reveal_end:
        logger.error(
            "Weights are too old to be revealed weights_id: %s, reveal_ended: %s, current block: %s",
            last_weights.pk,
            reveal_end,
            current_block,
        )
        return

    WEIGHT_REVEALING_TTL = config.DYNAMIC_WEIGHT_REVEALING_TTL
    WEIGHT_REVEALING_HARD_TTL = config.DYNAMIC_WEIGHT_REVEALING_HARD_TTL
    WEIGHT_REVEALING_ATTEMPTS = config.DYNAMIC_WEIGHT_REVEALING_ATTEMPTS
    WEIGHT_REVEALING_FAILURE_BACKOFF = config.DYNAMIC_WEIGHT_REVEALING_FAILURE_BACKOFF

    weights_id = last_weights.id
    with transaction.atomic():
        last_weights = (
            Weights.objects.filter(id=weights_id, revealed_at=None)
            .select_for_update(skip_locked=True)
            .first()
        )
        if not last_weights:
            logger.debug(
                "Weights have already been revealed or are being revealed at this moment: %s",
                weights_id,
            )
            return

        for try_number in range(WEIGHT_REVEALING_ATTEMPTS):
            logger.debug(f"Revealing weights (attempt #{try_number}): weights_id={weights_id}")
            success = False

            try:
                result = do_reveal_weights.apply_async(
                    kwargs=dict(
                        weights_id=last_weights.id,
                    ),
                    soft_time_limit=WEIGHT_REVEALING_TTL,
                    time_limit=WEIGHT_REVEALING_HARD_TTL,
                )
                logger.info(f"Revealing weights task id: {result.id}")
                try:
                    with allow_join_result():
                        success, msg = result.get(timeout=WEIGHT_REVEALING_TTL)
                except (celery.exceptions.TimeoutError, billiard.exceptions.TimeLimitExceeded):
                    result.revoke(terminate=True)
                    logger.info(f"Revealing weights timed out (attempt #{try_number})")
                    save_weight_setting_failure(
                        subtype=SystemEvent.EventSubType.WRITING_TO_CHAIN_TIMEOUT,
                        long_description=traceback.format_exc(),
                        data={"try_number": try_number, "operation": "revealing"},
                    )
                    continue
            except Exception:
                logger.warning("Encountered when revealing weights: ")
                save_weight_setting_failure(
                    subtype=SystemEvent.EventSubType.WRITING_TO_CHAIN_GENERIC_ERROR,
                    long_description=traceback.format_exc(),
                    data={"try_number": try_number, "operation": "revealing"},
                )
                continue
            if success:
                last_weights.revealed_at = now()
                last_weights.save()
                break
            time.sleep(WEIGHT_REVEALING_FAILURE_BACKOFF)
        else:
            msg = f"Failed to set weights after {WEIGHT_REVEALING_ATTEMPTS} attempts"
            logger.warning(msg)
            save_weight_setting_failure(
                subtype=SystemEvent.EventSubType.GIVING_UP,
                long_description=msg,
                data={"try_number": WEIGHT_REVEALING_ATTEMPTS, "operation": "revealing"},
            )


@app.task()
def do_reveal_weights(weights_id: int) -> tuple[bool, str]:
    weights = Weights.objects.filter(id=weights_id, revealed_at=None).first()
    if not weights:
        logger.debug(
            "Weights have already been revealed or are being revealed at this moment: %s",
            weights_id,
        )
        return True, "nothing_to_do"

    wallet = settings.BITTENSOR_WALLET()
    subtensor_ = _get_subtensor_for_setting_scores(network=settings.BITTENSOR_NETWORK)
    try:
        is_success, message = subtensor_.reveal_weights(
            wallet=wallet,
            netuid=settings.BITTENSOR_NETUID,
            uids=weights.uids,
            weights=weights.weights,
            salt=weights.salt,
            version_key=weights.version_key,
            wait_for_inclusion=True,
            wait_for_finalization=True,
            max_retries=2,
        )
    except SubstrateRequestException as e:
        # Consider the following exception as success:
        # The transaction has too low priority to replace another transaction already in the pool.
        if e.args[0]["code"] == 1014:
            is_success = True
            message = "transaction already in the pool"
        else:
            raise
    except Exception:
        logger.warning("Encountered when setting weights: ", exc_info=True)
        is_success = False
        message = traceback.format_exc()
    if is_success:
        save_weight_setting_event(
            type_=SystemEvent.EventType.WEIGHT_SETTING_SUCCESS,
            subtype=SystemEvent.EventSubType.REVEAL_WEIGHTS_SUCCESS,
            long_description=message,
            data={"weights_id": weights.id},
        )
    else:
        current_block = "unknown"
        try:
            current_block = subtensor_.get_current_block()
        except Exception as e:
            logger.warning("Failed to get current block: %s", e)
        finally:
            save_weight_setting_failure(
                subtype=SystemEvent.EventSubType.REVEAL_WEIGHTS_ERROR,
                long_description=message,
                data={
                    "weights_id": weights.id,
                    "current_block": current_block,
                },
            )
    return is_success, message


@app.task()
@bittensor_client
def do_set_weights(
    netuid: int,
    uids: list[int],
    weights: list[float],
    wait_for_inclusion: bool,
    wait_for_finalization: bool,
    version_key: int,
    bittensor: turbobt.Bittensor,
) -> tuple[bool, str]:
    """
    Set weights. To be used in other celery tasks in order to facilitate a timeout,
     since the multiprocessing version of this doesn't work in celery.
    """
    commit_reveal_weights_enabled = config.DYNAMIC_COMMIT_REVEAL_WEIGHTS_ENABLED
    max_weight = config.DYNAMIC_MAX_WEIGHT

    def _commit_weights() -> tuple[bool, str]:
        subtensor_ = _get_subtensor_for_setting_scores(network=settings.BITTENSOR_NETWORK)
        current_block = subtensor_.get_current_block()

        normalized_weights = _normalize_weights_for_committing(weights, max_weight)
        weights_in_db = Weights(
            uids=uids,
            weights=normalized_weights,
            block=current_block,
            version_key=version_key,
        )
        try:
            is_success, message = subtensor_.commit_weights(
                wallet=settings.BITTENSOR_WALLET(),
                netuid=netuid,
                uids=uids,
                weights=normalized_weights,
                salt=weights_in_db.salt,
                version_key=version_key,
                wait_for_inclusion=wait_for_inclusion,
                wait_for_finalization=wait_for_finalization,
                max_retries=2,
            )
        except SubstrateRequestException as e:
            # Consider the following exception as success:
            # The transaction has too low priority to replace another transaction already in the pool.
            if e.args[0]["code"] == 1014:
                is_success = True
                message = "transaction already in the pool"
            else:
                raise
        except Exception:
            is_success = False
            message = traceback.format_exc()

        if is_success:
            logger.info("Successfully committed weights!!!")
            weights_in_db.save()
            save_weight_setting_event(
                type_=SystemEvent.EventType.WEIGHT_SETTING_SUCCESS,
                subtype=SystemEvent.EventSubType.COMMIT_WEIGHTS_SUCCESS,
                long_description=f"message from chain: {message}",
                data={"weights_id": weights_in_db.id},
            )
        else:
            logger.info("Failed to commit weights due to: %s", message)
            save_weight_setting_failure(
                subtype=SystemEvent.EventSubType.COMMIT_WEIGHTS_ERROR,
                long_description=f"message from chain: {message}",
                data={
                    "weights_id": weights_in_db.id,
                    "current_block": current_block,
                },
            )
        return is_success, message

    def _set_weights() -> tuple[bool, str]:
        subnet = bittensor.subnet(netuid)

        try:
            reveal_round = async_to_sync(subnet.weights.commit)(
                dict(zip(uids, weights)),
                version_key=version_key,
            )
        except Exception:
            is_success = False
            message = traceback.format_exc()
        else:
            is_success = True
            message = f"reveal_round:{reveal_round}"

        if is_success:
            logger.info("Successfully set weights!!!")
            save_weight_setting_event(
                type_=SystemEvent.EventType.WEIGHT_SETTING_SUCCESS,
                subtype=SystemEvent.EventSubType.SET_WEIGHTS_SUCCESS,
                long_description=message,
                data={},
            )
        else:
            logger.info(f"Failed to set weights due to {message=}")
            save_weight_setting_failure(
                subtype=SystemEvent.EventSubType.SET_WEIGHTS_ERROR,
                long_description=message,
                data={},
            )
        return is_success, message

    if commit_reveal_weights_enabled:
        return _commit_weights()
    else:
        return _set_weights()


def save_weight_revealing_failure(subtype: str, long_description: str, data: JsonValue):
    save_weight_setting_event(
        type_=SystemEvent.EventType.WEIGHT_SETTING_FAILURE,
        subtype=subtype,
        long_description=long_description,
        data=data,
    )
