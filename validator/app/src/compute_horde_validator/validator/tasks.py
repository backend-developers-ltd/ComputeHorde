import contextlib
import json
import numbers
import time
import traceback
from collections import defaultdict
from datetime import timedelta

import billiard.exceptions
import bittensor
import celery.exceptions
import numpy as np
import requests
from asgiref.sync import async_to_sync
from bittensor.utils.weight_utils import process_weights_for_netuid
from celery import shared_task
from celery.result import allow_join_result
from celery.utils.log import get_task_logger
from compute_horde.dynamic_config import sync_dynamic_config
from compute_horde.receipts import (
    JobFinishedReceiptPayload,
    JobStartedReceiptPayload,
    get_miner_receipts,
)
from compute_horde.utils import get_validators
from constance import config
from django.conf import settings
from django.db import transaction
from django.utils.timezone import now

from compute_horde_validator.celery import app
from compute_horde_validator.validator.locks import Locked, get_weight_setting_lock
from compute_horde_validator.validator.metagraph_client import get_miner_axon_info
from compute_horde_validator.validator.models import (
    JobFinishedReceipt,
    JobStartedReceipt,
    OrganicJob,
    SyntheticJobBatch,
    SystemEvent,
    Weights,
)
from compute_horde_validator.validator.synthetic_jobs.utils import (
    MinerClient,
    create_and_run_sythethic_job_batch,
    save_receipt_event,
)

from .miner_driver import execute_organic_job
from .models import AdminJobRequest

logger = get_task_logger(__name__)

JOB_WINDOW = 2 * 60 * 60
SYNTHETIC_JOBS_SOFT_LIMIT = 2 * 300  # 2 x job timeout - let validator code not to be "instant"
SYNTHETIC_JOBS_HARD_LIMIT = SYNTHETIC_JOBS_SOFT_LIMIT + 10

SCORING_ALGO_VERSION = 2

WEIGHT_SETTING_TTL = 60
WEIGHT_SETTING_HARD_TTL = 65
WEIGHT_SETTING_ATTEMPTS = 100
WEIGHT_SETTING_FAILURE_BACKOFF = 5


class WeightsRevealError(Exception):
    pass


@app.task(
    soft_time_limit=SYNTHETIC_JOBS_SOFT_LIMIT,
    time_limit=SYNTHETIC_JOBS_HARD_LIMIT,
)
def _run_synthetic_jobs():
    try:
        # metagraph will be refetched and that's fine, after sleeping
        # for e.g. 30 minutes we should refetch the miner list
        create_and_run_sythethic_job_batch(settings.BITTENSOR_NETUID, settings.BITTENSOR_NETWORK)
    except billiard.exceptions.SoftTimeLimitExceeded:
        logger.info("Running synthetic jobs timed out")


@app.task()
def run_synthetic_jobs():
    if not config.SERVING:
        logger.warning("Not running synthetic jobs, SERVING is disabled in constance config")
        return

    if not settings.DEBUG_DONT_STAGGER_VALIDATORS:
        validators = get_validators(
            netuid=settings.BITTENSOR_NETUID, network=settings.BITTENSOR_NETWORK
        )
        my_key = settings.BITTENSOR_WALLET().get_hotkey().ss58_address
        validator_keys = [n.hotkey for n in validators]
        if my_key not in validator_keys:
            raise ValueError(
                "Can't determine proper synthetic job window due to not in top 23 validators"
            )
        my_index = validator_keys.index(my_key)
        window_per_validator = JOB_WINDOW / len(validator_keys)
        my_window_starts_at = window_per_validator * my_index
        logger.info(
            f"Sleeping for {my_window_starts_at:02f}s because I am {my_index} out of {len(validator_keys)}"
        )
        time.sleep(my_window_starts_at)
    _run_synthetic_jobs.apply_async()


def _normalize_weights_for_committing(weights: list[numbers.Number], max_: int):
    factor = max_ / max(weights)
    return [round(w * factor) for w in weights]


@app.task()
def do_set_weights(
    netuid: int,
    uids: list,
    weights: list,
    wait_for_inclusion: bool,
    wait_for_finalization: bool,
    version_key: int,
) -> tuple[bool, str]:
    """
    Set weights. To be used in other celery tasks in order to facilitate a timeout,
     since the multiprocessing version of this doesn't work in celery.
    """
    bittensor.turn_console_off()
    subtensor_ = get_subtensor(network=settings.BITTENSOR_NETWORK)
    metagraph = subtensor_.metagraph(netuid)
    current_block = metagraph.block.item()

    try:
        hyperparams = subtensor_.get_subnet_hyperparameters(netuid=settings.BITTENSOR_NETUID)
        commit_reveal_weights_enabled = bool(hyperparams.commit_reveal_weights_enabled)
        commit_reveal_weights_interval = hyperparams.commit_reveal_weights_interval
        max_weight = hyperparams.max_weight_limit
    except Exception:
        logger.exception('Failed to fetch "commit_reveal_weights_*" hyperparameters')
        commit_reveal_weights_enabled = None
        commit_reveal_weights_interval = 0
        max_weight = 65535

    def _commit_weights() -> tuple[bool, str]:
        last_weights = Weights.objects.order_by("-created_at").first()
        if (
            last_weights
            and last_weights.revealed_at is None
            and last_weights.block >= current_block - commit_reveal_weights_interval * 1.5
        ):
            # ___|______________________|______________________|____________________
            #    ^-commit weights       ^-time to reveal       ^-2x time to reveal
            #    |_________________________________|_______________________________
            #     impossible to commit new weights     can commit new weights
            #     unless last weights are revealed     no matter what
            save_weight_setting_failure(
                subtype=SystemEvent.EventSubType.COMMIT_WEIGHTS_UNREVEALED_ERROR,
                long_description="Cannot commit new weights before revealing old ones",
                data={
                    "uncommited_weights_id": last_weights.id,
                    "created_at": str(last_weights.created_at),
                    "block": last_weights.block,
                    "current_block": current_block,
                    "commit_reveal_weights_interval": commit_reveal_weights_interval,
                },
            )
            return False, "Cannot commit new weights before revealing old ones"
        normalized_weights = _normalize_weights_for_committing(weights, max_weight)
        weights_in_db = Weights(
            uids=uids,
            weights=normalized_weights,
            block=metagraph.block.item(),
            version_key=version_key,
        )
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
                data={"weights_id": weights_in_db.id},
            )
        return is_success, message

    def _set_weights() -> tuple[bool, str]:
        is_success, message = subtensor_.set_weights(
            wallet=settings.BITTENSOR_WALLET(),
            netuid=netuid,
            uids=np.int64(uids),
            weights=np.float32(weights),
            version_key=version_key,
            wait_for_inclusion=wait_for_inclusion,
            wait_for_finalization=wait_for_finalization,
            max_retries=2,
        )
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

    match commit_reveal_weights_enabled:
        case None:
            # we don't know current hyperparams, so we can't decide which method to use -> try both
            try:
                is_success, msg = _commit_weights()
            except:
                is_success, msg = False, "Unexpected error occurred when committing weights"
                logger.exception("Encountered when committing weights")
            if not is_success:  # 'Subtensor returned `CommitRevealDisabled (Module)` error. This means: `attempting to commit/reveal weights when disabled.`'
                is_success, msg = _set_weights()
            return is_success, msg

        case True:
            return _commit_weights()

        case False:
            return _set_weights()


@shared_task
def trigger_run_admin_job_request(job_request_id: int):
    async_to_sync(run_admin_job_request)(job_request_id)


def get_keypair():
    return settings.BITTENSOR_WALLET().get_hotkey()


async def run_admin_job_request(job_request_id: int, callback=None):
    job_request: AdminJobRequest = await AdminJobRequest.objects.prefetch_related("miner").aget(
        id=job_request_id
    )
    try:
        miner = job_request.miner
        miner_axon_info = await get_miner_axon_info(miner.hotkey)
        job = await OrganicJob.objects.acreate(
            job_uuid=str(job_request.uuid),
            miner=miner,
            miner_address=miner_axon_info.ip,
            miner_address_ip_version=miner_axon_info.ip_type,
            miner_port=miner_axon_info.port,
            executor_class=job_request.executor_class,
            job_description="Validator Job from Admin Panel",
        )

        keypair = get_keypair()
        miner_client = MinerClient(
            miner_address=miner_axon_info.ip,
            miner_port=miner_axon_info.port,
            miner_hotkey=miner.hotkey,
            my_hotkey=keypair.ss58_address,
            job_uuid=job.job_uuid,
            batch_id=None,
            keypair=keypair,
        )

        job_request.status_message = "Job successfully triggered"
        print(job_request.status_message)
        await job_request.asave()
    except Exception as e:
        job_request.status_message = f"Job failed to trigger due to: {e}"
        print(job_request.status_message)
        await job_request.asave()
        return

    print(f"\nProcessing job request: {job_request}")
    await execute_organic_job(
        miner_client,
        job,
        job_request,
        total_job_timeout=job_request.timeout,
        notify_callback=callback,
    )


def save_weight_setting_event(type_: str, subtype: str, long_description: str, data: dict):
    SystemEvent.objects.using(settings.DEFAULT_DB_ALIAS).create(
        type=type_,
        subtype=subtype,
        long_description=long_description,
        data=data,
    )


def save_weight_setting_failure(subtype: str, long_description: str, data: dict):
    save_weight_setting_event(
        type_=SystemEvent.EventType.WEIGHT_SETTING_FAILURE,
        subtype=subtype,
        long_description=long_description,
        data=data,
    )


@contextlib.contextmanager
def save_event_on_error(subtype):
    try:
        yield
    except Exception:
        save_weight_setting_failure(subtype, traceback.format_exc(), {})


def get_subtensor(network):
    with save_event_on_error(SystemEvent.EventSubType.SUBTENSOR_CONNECTIVITY_ERROR):
        return bittensor.subtensor(network=network)


def get_metagraph(subtensor, netuid):
    with save_event_on_error(SystemEvent.EventSubType.SUBTENSOR_CONNECTIVITY_ERROR):
        return subtensor.metagraph(netuid=netuid)


def sigmoid(x, beta, delta):
    return 1 / (1 + np.exp(beta * (-x + delta)))


def reversed_sigmoid(x, beta, delta):
    return sigmoid(-x, beta=beta, delta=-delta)


def horde_score(benchmarks, alpha=0, beta=0, delta=0):
    """Proportionally scores horde benchmarks allowing increasing significance for chosen features

    By default scores are proportional to horde "strength" - having 10 executors would have the same
    score as separate 10 single executor miners. Subnet owner can control significance of defined features:

    alpha - controls significance of average score, so smaller horde can have higher score if executors are stronger;
            best values are from range [0, 1], with 0 meaning no effect
    beta - controls sigmoid function steepness; sigmoid function is over `-(1 / horde_size)`, so larger hordes can be
           more significant than smaller ones, even if summary strength of a horde is the same;
           best values are from range [0,5] (or more, but higher values does not change sigmoid steepnes much),
           with 0 meaning no effect
    delta - controls where sigmoid function has 0.5 value allowing for better control over effect of beta param;
            best values are from range [0, 1]
    """
    sum_agent = sum(benchmarks)
    inverted_n = 1 / len(benchmarks)
    avg_benchmark = sum_agent * inverted_n
    scaled_inverted_n = reversed_sigmoid(inverted_n, beta=beta, delta=delta)
    scaled_avg_benchmark = avg_benchmark**alpha
    return scaled_avg_benchmark * sum_agent * scaled_inverted_n


@app.task
def set_scores():
    with save_event_on_error(SystemEvent.EventSubType.GENERIC_ERROR):
        with transaction.atomic():
            try:
                get_weight_setting_lock()
            except Locked:
                logger.debug("Another thread already setting weights")
                return
            if not config.SERVING:
                logger.warning("Not setting scores, SERVING is disabled in constance config")
                return

            subtensor = get_subtensor(network=settings.BITTENSOR_NETWORK)
            metagraph = get_metagraph(subtensor, netuid=settings.BITTENSOR_NETUID)
            neurons = metagraph.neurons
            hotkey_to_uid = {n.hotkey: n.uid for n in neurons}
            score_per_uid = {}
            batches = list(
                SyntheticJobBatch.objects.prefetch_related("synthetic_jobs").filter(
                    scored=False,
                    started_at__gte=now() - timedelta(days=1),
                    accepting_results_until__lt=now(),
                )
            )
            if not batches:
                logger.info("No batches - nothing to score")
                return

            # scaling factor for avg_score of a horde - best in range [0, 1] (0 means no effect on score)
            alpha = settings.HORDE_SCORE_AVG_PARAM
            # sigmoid steepnes param - best in range [0, 5] (0 means no effect on score)
            beta = settings.HORDE_SCORE_SIZE_PARAM
            # horde size for 0.5 value of sigmoid - sigmoid is for 1 / horde_size
            central_horde_size = settings.HORDE_SCORE_CENTRAL_SIZE_PARAM
            delta = 1 / central_horde_size
            for batch in batches:
                batch_scores = defaultdict(list)
                for job in batch.synthetic_jobs.all():
                    uid = hotkey_to_uid.get(job.miner.hotkey)
                    if uid is None:
                        continue
                    batch_scores[uid].append(job.score)
                for uid, uid_batch_scores in batch_scores.items():
                    uid_horde_score = horde_score(
                        uid_batch_scores, alpha=alpha, beta=beta, delta=delta
                    )
                    score_per_uid[uid] = score_per_uid.get(uid, 0) + uid_horde_score
            if not score_per_uid:
                logger.info("No miners on the subnet to score")
                return
            uids = np.zeros(len(neurons), dtype=np.int64)
            weights = np.zeros(len(neurons), dtype=np.float32)
            for ind, n in enumerate(neurons):
                uids[ind] = n.uid
                weights[ind] = score_per_uid.get(n.uid, 0)

            uids, weights = process_weights_for_netuid(
                uids,
                weights,
                settings.BITTENSOR_NETUID,
                subtensor,
                metagraph,
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
                            data={"try_number": try_number},
                        )
                        continue
                except Exception:
                    logger.warning("Encountered when setting weights: ")
                    save_weight_setting_failure(
                        subtype=SystemEvent.EventSubType.WRITING_TO_CHAIN_GENERIC_ERROR,
                        long_description=traceback.format_exc(),
                        data={"try_number": try_number},
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
                    data={"try_number": WEIGHT_SETTING_ATTEMPTS},
                )


@app.task()
def reveal_scores(reveal_in_advance_num_blocks: int = 10) -> None:
    """
    Select latest Weights that are older than `commit_reveal_weights_interval`
    and haven't been revealed yet, and reveal them.
    """
    subtensor = get_subtensor(network=settings.BITTENSOR_NETWORK)

    try:
        hyperparams = subtensor.get_subnet_hyperparameters(netuid=settings.BITTENSOR_NETUID)
        commit_reveal_weights_interval = hyperparams.commit_reveal_weights_interval
    except Exception:
        logger.exception('Failed to fetch "commit_reveal_weights_enabled" hyperparameter')
        commit_reveal_weights_interval = 0

    metagraph = get_metagraph(subtensor, netuid=settings.BITTENSOR_NETUID)
    current_block_number = metagraph.block.item()

    last_weights = Weights.objects.order_by("-created_at").first()
    if (
        last_weights
        and last_weights.revealed_at is None
        and last_weights.block
        <= current_block_number - (commit_reveal_weights_interval - reveal_in_advance_num_blocks)
    ):
        logger.debug(
            "Scheduling revealing weights record %s created at %s",
            last_weights.id,
            last_weights.created_at,
        )
        do_reveal_weights.delay(weights_id=last_weights.id)


@app.task()
def do_reveal_weights(weights_id: int) -> None:
    with transaction.atomic():
        weights = (
            Weights.objects.filter(id=weights_id, revealed_at=None)
            .select_for_update(skip_locked=True)
            .first()
        )
        if not weights:
            logger.debug(
                "Weights have already been revealed or are being revealed at this moment: %s",
                weights_id,
            )
            return

        wallet = settings.BITTENSOR_WALLET()
        subtensor_ = get_subtensor(network=settings.BITTENSOR_NETWORK)
        is_success, msg = subtensor_.reveal_weights(
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
        if is_success:
            save_weight_setting_event(
                type_=SystemEvent.EventType.WEIGHT_SETTING_SUCCESS,
                subtype=SystemEvent.EventSubType.REVEAL_WEIGHTS_SUCCESS,
                long_description=msg,
                data={"weights_id": weights.id},
            )
            weights.revealed_at = now()
            weights.save()
        else:
            save_weight_setting_failure(
                subtype=SystemEvent.EventSubType.REVEAL_WEIGHTS_ERROR,
                long_description=msg,
                data={"weights_id": weights.id},
            )


@app.task
def fetch_receipts_from_miner(hotkey: str, ip: str, port: int):
    logger.debug(f"Fetching receipts from miner. {hotkey=} {ip} {port=}")
    try:
        receipts = get_miner_receipts(hotkey, ip, port)
    except Exception as e:
        comment = f"Failed to fetch receipts from miner {hotkey} {ip}:{port}: {e!r}"
        logger.warning(comment)
        save_receipt_event(
            subtype=SystemEvent.EventSubType.RECEIPT_FETCH_ERROR,
            long_description=comment,
            data={"miner_hotkey": hotkey, "miner_ip": ip, "miner_port": port},
        )
        return
    logger.debug(f"Miner returned {len(receipts)} receipts. {hotkey=}")

    tolerance = timedelta(hours=1)

    latest_job_started_receipt = (
        JobStartedReceipt.objects.filter(miner_hotkey=hotkey).order_by("-time_accepted").first()
    )
    job_started_receipt_cutoff_time = (
        latest_job_started_receipt.time_accepted - tolerance if latest_job_started_receipt else None
    )
    job_started_receipt_to_create = [
        JobStartedReceipt(
            job_uuid=receipt.payload.job_uuid,
            miner_hotkey=receipt.payload.miner_hotkey,
            validator_hotkey=receipt.payload.validator_hotkey,
            executor_class=receipt.payload.executor_class,
            time_accepted=receipt.payload.time_accepted,
            max_timeout=receipt.payload.max_timeout,
        )
        for receipt in receipts
        if isinstance(receipt.payload, JobStartedReceiptPayload)
        and (
            job_started_receipt_cutoff_time is None
            or receipt.payload.time_accepted > job_started_receipt_cutoff_time
        )
    ]
    logger.debug(f"Creating {len(job_started_receipt_to_create)} JobStartedReceipt. {hotkey=}")
    JobStartedReceipt.objects.bulk_create(job_started_receipt_to_create, ignore_conflicts=True)

    latest_job_finished_receipt = (
        JobFinishedReceipt.objects.filter(miner_hotkey=hotkey).order_by("-time_started").first()
    )
    job_finished_receipt_cutoff_time = (
        latest_job_finished_receipt.time_started - tolerance
        if latest_job_finished_receipt
        else None
    )
    job_finished_receipt_to_create = [
        JobFinishedReceipt(
            job_uuid=receipt.payload.job_uuid,
            miner_hotkey=receipt.payload.miner_hotkey,
            validator_hotkey=receipt.payload.validator_hotkey,
            time_started=receipt.payload.time_started,
            time_took_us=receipt.payload.time_took_us,
            score_str=receipt.payload.score_str,
        )
        for receipt in receipts
        if isinstance(receipt.payload, JobFinishedReceiptPayload)
        and (
            job_finished_receipt_cutoff_time is None
            or receipt.payload.time_started > job_finished_receipt_cutoff_time
        )
    ]
    logger.debug(f"Creating {len(job_finished_receipt_to_create)} JobFinishedReceipt. {hotkey=}")
    JobFinishedReceipt.objects.bulk_create(job_finished_receipt_to_create, ignore_conflicts=True)


@app.task
def fetch_receipts():
    """Fetch job receipts from the miners."""
    # Delete old receipts before fetching new ones
    JobStartedReceipt.objects.filter(time_accepted__lt=now() - timedelta(days=7)).delete()
    JobFinishedReceipt.objects.filter(time_started__lt=now() - timedelta(days=7)).delete()

    metagraph = bittensor.metagraph(
        netuid=settings.BITTENSOR_NETUID, network=settings.BITTENSOR_NETWORK
    )
    miners = [neuron for neuron in metagraph.neurons if neuron.axon_info.is_serving]
    for miner in miners:
        fetch_receipts_from_miner.delay(miner.hotkey, miner.axon_info.ip, miner.axon_info.port)


@shared_task
def send_events_to_facilitator():
    events = SystemEvent.objects.using(settings.DEFAULT_DB_ALIAS).filter(sent=False)
    if events.count() == 0:
        return

    if settings.STATS_COLLECTOR_URL == "":
        logger.warning("STATS_COLLECTOR_URL is not set, not sending system events")
        return

    keypair = get_keypair()
    hotkey = keypair.ss58_address
    signing_timestamp = int(time.time())
    to_sign = json.dumps(
        {"signing_timestamp": signing_timestamp, "validator_ss58_address": hotkey},
        sort_keys=True,
    )
    signature = f"0x{keypair.sign(to_sign).hex()}"

    data = [event.to_dict() for event in events]
    url = settings.STATS_COLLECTOR_URL + f"validator/{hotkey}/system_events"
    response = requests.post(
        url,
        json=data,
        headers={
            "Validator-Signature": signature,
            "Validator-Signing-Timestamp": str(signing_timestamp),
        },
    )

    if response.status_code == 201:
        logger.info(f"Sent {len(data)} system events to facilitator")
        events.update(sent=True)
    else:
        logger.error(f"Failed to send system events to facilitator: {response}")


@app.task
def fetch_dynamic_config() -> None:
    sync_dynamic_config(
        config_url=f"https://raw.githubusercontent.com/backend-developers-ltd/compute-horde-dynamic-config/master/validator-config-{settings.DYNAMIC_CONFIG_ENV}.json",
        namespace=config,
    )
