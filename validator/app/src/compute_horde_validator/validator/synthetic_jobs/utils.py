import asyncio
import contextlib
import datetime
import logging
import time
import traceback
import uuid
from collections.abc import Iterable
from functools import lru_cache, partial

import bittensor
from asgiref.sync import async_to_sync, sync_to_async
from channels.layers import get_channel_layer
from compute_horde.executor_class import DEFAULT_EXECUTOR_CLASS, ExecutorClass
from compute_horde.miner_client.base import (
    TransportConnectionError,
)
from compute_horde.mv_protocol.miner_requests import (
    V0DeclineJobRequest,
    V0ExecutorFailedRequest,
    V0ExecutorReadyRequest,
    V0JobFailedRequest,
    V0JobFinishedRequest,
)
from compute_horde.mv_protocol.validator_requests import (
    V0InitialJobRequest,
    V0JobRequest,
    VolumeType,
)
from django.conf import settings
from django.utils.timezone import now

from compute_horde_validator.validator.dynamic_config import aget_config, aget_weights_version
from compute_horde_validator.validator.miner_client import MinerClient, save_job_execution_event
from compute_horde_validator.validator.models import (
    Miner,
    MinerManifest,
    SyntheticJob,
    SyntheticJobBatch,
    SystemEvent,
)
from compute_horde_validator.validator.synthetic_jobs.generator import current
from compute_horde_validator.validator.utils import MACHINE_SPEC_GROUP_NAME

JOB_LENGTH = 300
TIMEOUT_SETUP = 30
TIMEOUT_LEEWAY = 1
TIMEOUT_MARGIN = 60
TIMEOUT_BARRIER = JOB_LENGTH - 65


logger = logging.getLogger(__name__)


@lru_cache(maxsize=100)
def batch_id_to_uuid(batch_id: int) -> uuid.UUID:
    return uuid.uuid4()


def create_and_run_synthetic_job_batch(netuid, network):
    if settings.DEBUG_MINER_KEY:
        miners: list[Miner] = []
        axons_by_key: dict[str, bittensor.AxonInfo] = {}
        for miner_index in range(settings.DEBUG_MINER_COUNT):
            hotkey = settings.DEBUG_MINER_KEY
            if miner_index > 0:
                # fake hotkey based on miner index if there are more than one miners
                hotkey = f"5u{miner_index:03}u{hotkey[6:]}"
            miner = Miner.objects.get_or_create(hotkey=hotkey)[0]
            miners.append(miner)
            axons_by_key[hotkey] = bittensor.AxonInfo(
                version=4,
                ip=settings.DEBUG_MINER_ADDRESS,
                ip_type=4,
                port=settings.DEBUG_MINER_PORT + miner_index,
                hotkey=hotkey,
                coldkey=hotkey,  # I hope it does not matter
            )
    else:
        try:
            metagraph = bittensor.metagraph(netuid, network=network)
        except Exception as e:
            msg = f"Failed to get metagraph - will not run synthetic jobs: {e}"
            logger.warning(msg)
            SystemEvent.objects.using(settings.DEFAULT_DB_ALIAS).create(
                type=SystemEvent.EventType.MINER_SYNTHETIC_JOB_FAILURE,
                subtype=SystemEvent.EventSubType.SUBTENSOR_CONNECTIVITY_ERROR,
                long_description=msg,
                data={},
            )
            return
        axons_by_key = {n.hotkey: n.axon_info for n in metagraph.neurons}
        miners = get_miners(metagraph)
        miners = [
            miner
            for miner in miners
            if miner.hotkey in axons_by_key and axons_by_key[miner.hotkey].is_serving
        ]

    execute_synthetic_batch(axons_by_key, miners)


@async_to_sync
async def execute_synthetic_batch(axons_by_key: dict[str, bittensor.AxonInfo], miners: list[Miner]):
    batch = await SyntheticJobBatch.objects.acreate(
        accepting_results_until=now() + datetime.timedelta(seconds=JOB_LENGTH)
    )
    miners_previous_online_executors = await get_previous_online_executors(miners, batch)

    tasks = [
        asyncio.create_task(
            asyncio.wait_for(
                execute_miner_synthetic_jobs(
                    batch.id,
                    miner.id,
                    miner.hotkey,
                    axons_by_key[miner.hotkey],
                    miners_previous_online_executors.get(miner.id),
                ),
                JOB_LENGTH,
            )
        )
        for miner in miners
    ]
    results = await asyncio.gather(*tasks, return_exceptions=True)
    await handle_synthetic_job_exceptions(results)


def save_receipt_event(subtype: str, long_description: str, data: dict):
    SystemEvent.objects.using(settings.DEFAULT_DB_ALIAS).create(
        type=SystemEvent.EventType.RECEIPT_FAILURE,
        subtype=subtype,
        long_description=long_description,
        data=data,
    )


async def execute_miner_synthetic_jobs(
    batch_id, miner_id, miner_hotkey, axon_info, miner_previous_online_executors
):
    key = settings.BITTENSOR_WALLET().get_hotkey()
    miner_client = MinerClient(
        miner_address=axon_info.ip,
        miner_port=axon_info.port,
        miner_hotkey=miner_hotkey,
        my_hotkey=key.ss58_address,
        job_uuid=None,
        batch_id=batch_id,
        keypair=key,
    )
    data = {"miner_hotkey": miner_client.miner_hotkey}
    async with contextlib.AsyncExitStack() as exit_stack:
        try:
            await exit_stack.enter_async_context(miner_client)
        except TransportConnectionError as exc:
            msg = f"Miner connection error: {exc}"
            logger.warning(msg)
            await save_job_execution_event(
                subtype=SystemEvent.EventSubType.MINER_CONNECTION_ERROR,
                long_description=msg,
                data=data,
            )
            return
        try:
            manifest = await asyncio.wait_for(miner_client.miner_manifest, 30)
        except TimeoutError:
            msg = f"Cannot send synthetic jobs to miner {miner_hotkey}: manifest future timed out"
            logger.warning(msg)
            await save_job_execution_event(
                subtype=SystemEvent.EventSubType.MANIFEST_ERROR, long_description=msg, data=data
            )
            return

        jobs = []
        for executor_class_manifest in manifest.executor_classes:
            # convert deprecated executor class 0 to default executor class
            if executor_class_manifest.executor_class == 0:
                executor_class_manifest.executor_class = DEFAULT_EXECUTOR_CLASS
            for _ in range(executor_class_manifest.count):
                job = SyntheticJob(
                    batch_id=batch_id,
                    miner_id=miner_id,
                    miner_address=axon_info.ip,
                    miner_address_ip_version=axon_info.ip_type,
                    miner_port=axon_info.port,
                    executor_class=executor_class_manifest.executor_class,
                    status=SyntheticJob.Status.PENDING,
                )
                miner_client.add_job(job.job_uuid)
                jobs.append(job)

        jobs = await SyntheticJob.objects.abulk_create(jobs)
        try:
            await execute_synthetic_jobs(miner_client, jobs, miner_previous_online_executors)
        except ExceptionGroup as e:
            msg = f"Multiple errors occurred during execution of some jobs for miner {miner_hotkey}: {e!r}"
            logger.warning(msg)
            await save_job_execution_event(
                subtype=SystemEvent.EventSubType.GENERIC_ERROR, long_description=msg
            )
        except Exception as e:
            msg = f"Failed to execute jobs for miner {miner_hotkey}: {e}"
            logger.warning(msg)
            logger.warning("".join(traceback.format_exception(type(e), e, e.__traceback__)))
            await save_job_execution_event(
                subtype=SystemEvent.EventSubType.GENERIC_ERROR, long_description=msg
            )
        await miner_client.save_manifest(manifest)


async def apply_manifest_incentive(
    score: float, previous_online_executors: int | None, current_online_executors: int
) -> float:
    weights_version = await aget_weights_version()
    multiplier = None
    if weights_version >= 2:
        if previous_online_executors is None:
            multiplier = await aget_config("DYNAMIC_MANIFEST_SCORE_MULTIPLIER")
        else:
            low, high = sorted([previous_online_executors, current_online_executors])
            # low can be 0 if previous_online_executors == 0, but we make it that way to
            # make this function correct for any kind of input
            threshold = await aget_config("DYNAMIC_MANIFEST_DANCE_RATIO_THRESHOLD")
            if low == 0 or high / low >= threshold:
                multiplier = await aget_config("DYNAMIC_MANIFEST_SCORE_MULTIPLIER")
    if multiplier is not None:
        return score * multiplier
    return score


async def _execute_synthetic_job(
    miner_client: MinerClient, job: SyntheticJob, previous_online_executors
):
    data = {"job_uuid": str(job.job_uuid), "miner_hotkey": job.miner.hotkey}
    save_event = partial(save_job_execution_event, data=data)

    async def handle_send_error_event(msg: str):
        await save_event(subtype=SystemEvent.EventSubType.MINER_SEND_ERROR, long_description=msg)

    job_state = miner_client.get_job_state(str(job.job_uuid))
    job_executor_class = ExecutorClass(job.executor_class)
    job_generator = await current.synthetic_job_generator_factory.create(job_executor_class)
    await job_generator.ainit()
    job.job_description = job_generator.job_description()
    await job.asave()
    await miner_client.send_model(
        V0InitialJobRequest(
            job_uuid=str(job.job_uuid),
            executor_class=job_executor_class,
            base_docker_image_name=job_generator.base_docker_image_name(),
            timeout_seconds=job_generator.timeout_seconds(),
            volume_type=VolumeType.inline.value,
        ),
        error_event_callback=handle_send_error_event,
    )
    try:
        msg = await asyncio.wait_for(job_state.miner_ready_or_declining_future, TIMEOUT_BARRIER)
        miner_client.online_executor_count += 1
    except TimeoutError:
        msg = None

    # wait for barrier even for declined and failed requests
    try:
        await asyncio.wait_for(miner_client.get_barrier().wait(), TIMEOUT_BARRIER)
    except TimeoutError:
        logger.info(
            f"Miner {miner_client.miner_name} barrier timeout - some executors would not be tested."
        )

    current_online_executors = miner_client.online_executor_count

    if isinstance(msg, V0DeclineJobRequest | V0ExecutorFailedRequest) or msg is None:
        comment = f"Miner {miner_client.miner_name} didn't accept job:" + (
            "timeout" if msg is None else str(msg.model_dump_json())
        )
        job.status = SyntheticJob.Status.FAILED
        job.comment = comment
        await job.asave()

        logger.info(comment)
        await save_event(
            subtype=SystemEvent.EventSubType.JOB_NOT_STARTED,
            long_description=job.comment,
        )
        return
    elif isinstance(msg, V0ExecutorReadyRequest):
        logger.debug(f"Miner {miner_client.miner_name} ready for job: {msg}")
    else:
        comment = f"Unexpected msg from miner {miner_client.miner_name}: {msg}"
        await save_event(
            subtype=SystemEvent.EventSubType.UNEXPECTED_MESSAGE, long_description=comment
        )
        raise ValueError(comment)

    # Send job started receipt to miner
    synthetic_job_execution_timeout = job_generator.timeout_seconds() + TIMEOUT_LEEWAY
    try:
        receipt_message = miner_client.generate_job_started_receipt_message(
            job,
            time.time(),
            synthetic_job_execution_timeout + TIMEOUT_MARGIN + TIMEOUT_SETUP,
        )
        await miner_client.send_model(receipt_message, error_event_callback=handle_send_error_event)
        logger.debug(f"Sent job started receipt for {job.job_uuid}")
    except Exception as e:
        comment = f"Failed to send job started receipt to miner {miner_client.miner_name} for job {job.job_uuid}: {e}"
        logger.warning(comment)
        await sync_to_async(save_receipt_event)(
            subtype=SystemEvent.EventSubType.RECEIPT_SEND_ERROR,
            long_description=comment,
            data=data,
        )

    # generate before locking on barrier
    volume_contents = await job_generator.volume_contents()

    await miner_client.send_model(
        V0JobRequest(
            job_uuid=str(job.job_uuid),
            executor_class=job_executor_class,
            docker_image_name=job_generator.docker_image_name(),
            docker_run_options_preset=job_generator.docker_run_options_preset(),
            docker_run_cmd=job_generator.docker_run_cmd(),
            raw_script=job_generator.raw_script(),
            volume={
                "volume_type": VolumeType.inline.value,
                "contents": volume_contents,
            },
            output_upload=None,
        ),
        error_event_callback=handle_send_error_event,
    )
    full_job_sent = time.time()
    msg = None
    try:
        msg = await asyncio.wait_for(
            job_state.miner_finished_or_failed_future,
            synthetic_job_execution_timeout + TIMEOUT_MARGIN,
        )
        time_took = job_state.miner_finished_or_failed_timestamp - full_job_sent
        if time_took > (synthetic_job_execution_timeout):
            comment = f"Miner {miner_client.miner_name} sent a job result but too late: {msg}"
            logger.info(
                comment
                + f"{time_took=} {synthetic_job_execution_timeout=} "
                + f"{job_state.miner_finished_or_failed_timestamp=} {full_job_sent=}"
            )
            await save_event(
                subtype=SystemEvent.EventSubType.JOB_EXECUTION_TIMEOUT, long_description=comment
            )
            raise TimeoutError

        if time_took < 10:
            logger.warning(
                f"Miner {miner_client.miner_name} finished job {job.job_uuid} too quickly: {time_took} - will default to 10s"
            )
            time_took = 10

    except TimeoutError:
        comment = f"Miner {miner_client.miner_name} timed out"
        job.status = SyntheticJob.Status.FAILED
        job.comment = comment
        await job.asave()

        logger.info(job.comment)
        await save_event(
            subtype=SystemEvent.EventSubType.JOB_EXECUTION_TIMEOUT, long_description=job.comment
        )
        return

    if isinstance(msg, V0JobFailedRequest):
        comment = f"Miner {miner_client.miner_name} failed: {msg.model_dump_json()}"
        job.status = SyntheticJob.Status.FAILED
        job.comment = comment
        await job.asave()

        logger.info(comment)
        await save_event(subtype=SystemEvent.EventSubType.FAILURE, long_description=comment)
        return

    elif isinstance(msg, V0JobFinishedRequest):
        success, comment, score = job_generator.verify(msg, time_took)

        # Send machine specs to facilitator
        if job_state.miner_machine_specs is not None:
            logger.debug(
                f"Miner {miner_client.miner_name} sent machine specs: {job_state.miner_machine_specs.specs}"
            )
            channel_layer = get_channel_layer()
            await channel_layer.group_send(
                MACHINE_SPEC_GROUP_NAME,
                {
                    "type": "machine.specs",
                    "miner_hotkey": job.miner.hotkey,
                    "batch_id": str(batch_id_to_uuid(job.batch_id)),
                    "specs": job_state.miner_machine_specs.specs,
                },
            )

        if success:
            comment = f"Miner {miner_client.miner_name} finished: {msg.model_dump_json()}"
            job.status = SyntheticJob.Status.COMPLETED
            job.comment = comment
            await job.asave()

            logger.info(comment)
            await save_event(
                subtype=SystemEvent.EventSubType.SUCCESS, long_description=comment, success=True
            )

            # if job passed, save synthetic job score
            job.score = await apply_manifest_incentive(
                score, previous_online_executors, current_online_executors
            )
            await job.asave()

            # Send job finished receipt to miner
            try:
                receipt_message = miner_client.generate_job_finished_receipt_message(
                    job, full_job_sent, time_took, job.score
                )
                await miner_client.send_model(
                    receipt_message, error_event_callback=handle_send_error_event
                )
                logger.debug(f"Sent job finished receipt for {job.job_uuid}")
            except Exception as e:
                comment = f"Failed to send job finished receipt to miner {miner_client.miner_name} for job {job.job_uuid}: {e}"
                logger.warning(comment)
                await sync_to_async(save_receipt_event)(
                    subtype=SystemEvent.EventSubType.RECEIPT_SEND_ERROR,
                    long_description=comment,
                    data=data,
                )

        else:
            comment = f"Miner {miner_client.miner_name} finished but {comment}"
            job.status = SyntheticJob.Status.FAILED
            job.comment = comment
            await job.asave()

            logger.info(comment)
            await save_job_execution_event(
                subtype=SystemEvent.EventSubType.FAILURE, long_description=comment
            )
            return
    else:
        raise ValueError(f"Unexpected msg from miner {miner_client.miner_name}: {msg}")


async def execute_synthetic_job(
    miner_client: MinerClient, synthetic_job_id, miner_previous_online_executors
):
    synthetic_job: SyntheticJob = await SyntheticJob.objects.prefetch_related("miner").aget(
        id=synthetic_job_id
    )
    await _execute_synthetic_job(miner_client, synthetic_job, miner_previous_online_executors)


async def execute_synthetic_jobs(
    miner_client: MinerClient,
    synthetic_jobs: Iterable[SyntheticJob],
    miner_previous_online_executors,
):
    tasks = [
        asyncio.create_task(
            asyncio.wait_for(
                execute_synthetic_job(
                    miner_client, synthetic_job.id, miner_previous_online_executors
                ),
                JOB_LENGTH,
            )
        )
        for synthetic_job in synthetic_jobs
    ]
    results = await asyncio.gather(*tasks, return_exceptions=True)
    await handle_synthetic_job_exceptions(results)


async def handle_synthetic_job_exceptions(results):
    exceptions = []
    for r in results:
        if isinstance(r, TimeoutError):
            msg = f"Synthetic Job has exceeded timeout {JOB_LENGTH}: {r}"
            logger.warning(msg)
            await SystemEvent.objects.using(settings.DEFAULT_DB_ALIAS).acreate(
                type=SystemEvent.EventType.MINER_SYNTHETIC_JOB_FAILURE,
                subtype=SystemEvent.EventSubType.JOB_EXECUTION_TIMEOUT,
                long_description=msg,
                data={},
            )
        elif isinstance(r, asyncio.CancelledError):
            msg = f"Synthetic Job with {JOB_LENGTH} timeout has been cancelled: {r}"
            logger.warning(msg)
            await SystemEvent.objects.using(settings.DEFAULT_DB_ALIAS).acreate(
                type=SystemEvent.EventType.MINER_SYNTHETIC_JOB_FAILURE,
                subtype=SystemEvent.EventSubType.JOB_EXECUTION_TIMEOUT,
                long_description=msg,
                data={},
            )
        elif isinstance(r, Exception):
            logger.warning("Error occurred", exc_info=r)
            exceptions.append(r)
    if exceptions:
        raise ExceptionGroup("exceptions raised while executing synthetic job task(s)", exceptions)


def get_miners(metagraph) -> list[Miner]:
    existing = list(Miner.objects.filter(hotkey__in=[n.hotkey for n in metagraph.neurons]))
    existing_keys = [m.hotkey for m in existing]
    new_miners = Miner.objects.bulk_create(
        [Miner(hotkey=n.hotkey) for n in metagraph.neurons if n.hotkey not in existing_keys]
    )
    data = {"block": metagraph.block.item()}
    if new_miners:
        data["new_miners"] = len(new_miners)
    SystemEvent.objects.using(settings.DEFAULT_DB_ALIAS).create(
        type=SystemEvent.EventType.VALIDATOR_MINERS_REFRESH,
        subtype=SystemEvent.EventSubType.SUCCESS,
        data=data,
    )
    return existing + new_miners


async def get_previous_online_executors(
    miners: list[Miner], batch: SyntheticJobBatch
) -> dict[int, int]:
    miner_ids = {miner.id for miner in miners}
    previous_batch = (
        await SyntheticJobBatch.objects.filter(id__lt=batch.id).order_by("-id").afirst()
    )
    if previous_batch is None:
        return {}
    previous_online_executors = {
        manifest.miner_id: manifest.online_executor_count
        async for manifest in MinerManifest.objects.filter(batch_id=previous_batch.id)
        if manifest.miner_id in miner_ids
    }
    return previous_online_executors
