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
from compute_horde.base_requests import BaseRequest
from compute_horde.executor_class import DEFAULT_EXECUTOR_CLASS, ExecutorClass
from compute_horde.miner_client.base import (
    AbstractMinerClient,
    MinerConnectionError,
    UnsupportedMessageReceived,
)
from compute_horde.mv_protocol import miner_requests, validator_requests
from compute_horde.mv_protocol.miner_requests import (
    BaseMinerRequest,
    ExecutorManifest,
    UnauthorizedError,
    V0AcceptJobRequest,
    V0DeclineJobRequest,
    V0ExecutorFailedRequest,
    V0ExecutorManifestRequest,
    V0ExecutorReadyRequest,
    V0JobFailedRequest,
    V0JobFinishedRequest,
    V0MachineSpecsRequest,
)
from compute_horde.mv_protocol.validator_requests import (
    AuthenticationPayload,
    JobFinishedReceiptPayload,
    JobStartedReceiptPayload,
    V0AuthenticateRequest,
    V0InitialJobRequest,
    V0JobFinishedReceiptRequest,
    V0JobRequest,
    V0JobStartedReceiptRequest,
    VolumeType,
)
from compute_horde.utils import MachineSpecs
from django.conf import settings
from django.utils.timezone import now

from compute_horde_validator.validator.dynamic_config import aget_config, aget_weights_version
from compute_horde_validator.validator.models import (
    JobBase,
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


class JobState:
    def __init__(self):
        loop = asyncio.get_running_loop()
        self.miner_ready_or_declining_future = loop.create_future()
        self.miner_ready_or_declining_timestamp: int = 0
        self.miner_finished_or_failed_future = loop.create_future()
        self.miner_finished_or_failed_timestamp: int = 0
        self.miner_machine_specs: MachineSpecs | None = None


class MinerClient(AbstractMinerClient):
    def __init__(
        self,
        miner_address: str,
        my_hotkey: str,
        miner_hotkey: str,
        miner_port: int,
        job_uuid: None | str | uuid.UUID,
        batch_id: None | int,
        keypair: bittensor.Keypair,
    ):
        super().__init__(f"{miner_hotkey}({miner_address}:{miner_port})")
        self.miner_hotkey = miner_hotkey
        self.my_hotkey = my_hotkey
        self.miner_address = miner_address
        self.miner_port = miner_port
        self.job_states = {}
        if job_uuid is not None:
            self.add_job(job_uuid)
        self.batch_id = batch_id
        self.keypair = keypair
        self._barrier = None
        loop = asyncio.get_running_loop()
        self.miner_manifest = loop.create_future()
        self.online_executor_count = 0

    def add_job(self, job_uuid: str | uuid.UUID):
        job_state = JobState()
        self.job_states[str(job_uuid)] = job_state
        return job_state

    def get_job_state(self, job_uuid: str | uuid.UUID):
        return self.job_states.get(str(job_uuid))

    def get_barrier(self):
        if self._barrier is None:
            self._barrier = asyncio.Barrier(len(self.job_states))
        return self._barrier

    def miner_url(self) -> str:
        return (
            f"ws://{self.miner_address}:{self.miner_port}/v0.1/validator_interface/{self.my_hotkey}"
        )

    def accepted_request_type(self) -> type[BaseRequest]:
        return BaseMinerRequest

    def incoming_generic_error_class(self):
        return miner_requests.GenericError

    def outgoing_generic_error_class(self):
        return validator_requests.GenericError

    async def handle_message(self, msg: BaseRequest):
        if isinstance(msg, self.incoming_generic_error_class()):
            msg = f"Received error message from miner {self.miner_name}: {msg.model_dump_json()}"
            logger.warning(msg)
            await save_job_execution_event(
                subtype=SystemEvent.EventSubType.GENERIC_ERROR, long_description=msg
            )
            return
        elif isinstance(msg, UnauthorizedError):
            logger.error(f"Unauthorized in {self.miner_name}: {msg.code}, details: {msg.details}")
            await save_job_execution_event(
                subtype=SystemEvent.EventSubType.UNAUTHORIZED, long_description=msg
            )
            return
        elif isinstance(msg, V0ExecutorManifestRequest):
            try:
                self.miner_manifest.set_result(msg.manifest)
            except asyncio.InvalidStateError:
                logger.warning(f"Received manifest from {msg} but future was already set")
            return

        job_state = self.get_job_state(msg.job_uuid)
        if job_state is None:
            logger.info(f"Received info about another job: {msg}")
            return

        if isinstance(msg, V0AcceptJobRequest):
            logger.info(f"Miner {self.miner_name} accepted job")
        elif isinstance(
            msg, V0DeclineJobRequest | V0ExecutorFailedRequest | V0ExecutorReadyRequest
        ):
            try:
                job_state.miner_ready_or_declining_future.set_result(msg)
                job_state.miner_ready_or_declining_timestamp = time.time()
            except asyncio.InvalidStateError:
                logger.warning(f"Received {msg} from {self.miner_name} but future was already set")
        elif isinstance(msg, V0JobFailedRequest | V0JobFinishedRequest):
            try:
                job_state.miner_finished_or_failed_future.set_result(msg)
                job_state.miner_finished_or_failed_timestamp = time.time()
            except asyncio.InvalidStateError:
                logger.warning(f"Received {msg} from {self.miner_name} but future was already set")
        elif isinstance(msg, V0MachineSpecsRequest):
            job_state.miner_machine_specs = msg.specs
        else:
            raise UnsupportedMessageReceived(msg)

    async def save_manifest(self, manifest: ExecutorManifest):
        miner = await Miner.objects.aget(hotkey=self.miner_hotkey)
        if self.batch_id:
            await MinerManifest.objects.acreate(
                miner=miner,
                batch_id=self.batch_id,
                executor_count=manifest.total_count,
                online_executor_count=self.online_executor_count,
            )

    def generate_authentication_message(self):
        payload = AuthenticationPayload(
            validator_hotkey=self.my_hotkey,
            miner_hotkey=self.miner_hotkey,
            timestamp=int(time.time()),
        )
        return V0AuthenticateRequest(
            payload=payload, signature=f"0x{self.keypair.sign(payload.blob_for_signing()).hex()}"
        )

    def generate_job_started_receipt_message(
        self, job: JobBase, accepted_timestamp: float, max_timeout: int
    ) -> V0JobStartedReceiptRequest:
        time_accepted = datetime.datetime.fromtimestamp(accepted_timestamp, datetime.UTC)
        receipt_payload = JobStartedReceiptPayload(
            job_uuid=str(job.job_uuid),
            miner_hotkey=job.miner.hotkey,
            validator_hotkey=self.my_hotkey,
            executor_class=ExecutorClass(job.executor_class),
            time_accepted=time_accepted,
            max_timeout=max_timeout,
        )
        return V0JobStartedReceiptRequest(
            payload=receipt_payload,
            signature=f"0x{self.keypair.sign(receipt_payload.blob_for_signing()).hex()}",
        )

    def generate_job_finished_receipt_message(
        self, job: JobBase, started_timestamp: float, time_took_seconds: float, score: float
    ) -> V0JobFinishedReceiptRequest:
        time_started = datetime.datetime.fromtimestamp(started_timestamp, datetime.UTC)
        receipt_payload = JobFinishedReceiptPayload(
            job_uuid=str(job.job_uuid),
            miner_hotkey=job.miner.hotkey,
            validator_hotkey=self.my_hotkey,
            time_started=time_started,
            time_took_us=int(time_took_seconds * 1_000_000),
            score_str=f"{score:.6f}",
        )
        return V0JobFinishedReceiptRequest(
            payload=receipt_payload,
            signature=f"0x{self.keypair.sign(receipt_payload.blob_for_signing()).hex()}",
        )

    async def _connect(self):
        ws = await super()._connect()
        await ws.send(self.generate_authentication_message().model_dump_json())
        return ws


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
    handle_synthetic_job_exceptions(results)


async def save_job_execution_event(subtype: str, long_description: str, data={}, success=False):
    await SystemEvent.objects.using(settings.DEFAULT_DB_ALIAS).acreate(
        type=SystemEvent.EventType.MINER_SYNTHETIC_JOB_SUCCESS
        if success
        else SystemEvent.EventType.MINER_SYNTHETIC_JOB_FAILURE,
        subtype=subtype,
        long_description=long_description,
        data=data,
    )


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
        except MinerConnectionError as exc:
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
            await _execute_synthetic_jobs(miner_client, jobs, miner_previous_online_executors)
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
        )
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
        await miner_client.send_model(receipt_message)
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
        )
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
                await miner_client.send_model(receipt_message)
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


async def _execute_synthetic_job_id(
    miner_client: MinerClient, synthetic_job_id, miner_previous_online_executors
):
    synthetic_job: SyntheticJob = await SyntheticJob.objects.prefetch_related("miner").aget(
        id=synthetic_job_id
    )
    await _execute_synthetic_job(miner_client, synthetic_job, miner_previous_online_executors)


async def _execute_synthetic_jobs(
    miner_client: MinerClient,
    synthetic_jobs: Iterable[SyntheticJob],
    miner_previous_online_executors,
):
    tasks = [
        asyncio.create_task(
            asyncio.wait_for(
                _execute_synthetic_job_id(
                    miner_client, synthetic_job.id, miner_previous_online_executors
                ),
                JOB_LENGTH,
            )
        )
        for synthetic_job in synthetic_jobs
    ]
    results = await asyncio.gather(*tasks, return_exceptions=True)
    handle_synthetic_job_exceptions(results)


def handle_synthetic_job_exceptions(results):
    exceptions = []
    for r in results:
        if isinstance(r, TimeoutError):
            msg = f"Synthetic Job has exceeded timeout {JOB_LENGTH}: {r}"
            logger.warning(msg)
            SystemEvent.objects.using(settings.DEFAULT_DB_ALIAS).create(
                type=SystemEvent.EventType.MINER_SYNTHETIC_JOB_FAILURE,
                subtype=SystemEvent.EventSubType.JOB_EXECUTION_TIMEOUT,
                long_description=msg,
            )
        elif isinstance(r, asyncio.CancelledError):
            msg = f"Synthetic Job with {JOB_LENGTH} timeout has been cancelled: {r}"
            logger.warning(msg)
            SystemEvent.objects.using(settings.DEFAULT_DB_ALIAS).create(
                type=SystemEvent.EventType.MINER_SYNTHETIC_JOB_FAILURE,
                subtype=SystemEvent.EventSubType.JOB_EXECUTION_TIMEOUT,
                long_description=msg,
            )
        elif isinstance(r, Exception):
            logger.warning("Error occurred", exc_info=r)
            exceptions.append(r)
    if exceptions:
        raise ExceptionGroup("exceptions raised while executing synthetic job task(s)", exceptions)


def get_miners(metagraph) -> list[Miner]:
    keys = {n.hotkey for n in metagraph.neurons}
    existing = list(Miner.objects.filter(hotkey__in=keys))
    existing_keys = {m.hotkey for m in existing}
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
