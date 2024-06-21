import asyncio
import datetime
import logging
import time
import uuid
from collections.abc import Iterable
from functools import lru_cache

import bittensor
from asgiref.sync import async_to_sync
from channels.layers import get_channel_layer
from compute_horde.base_requests import BaseRequest
from compute_horde.miner_client.base import AbstractMinerClient, UnsupportedMessageReceived
from compute_horde.mv_protocol import miner_requests, validator_requests
from compute_horde.mv_protocol.miner_requests import (
    BaseMinerRequest,
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
    ReceiptPayload,
    V0AuthenticateRequest,
    V0InitialJobRequest,
    V0JobRequest,
    V0ReceiptRequest,
    VolumeType,
)
from compute_horde.utils import MachineSpecs
from django.conf import settings
from django.utils.timezone import now

from compute_horde_validator.validator.models import JobBase, Miner, SyntheticJob, SyntheticJobBatch
from compute_horde_validator.validator.synthetic_jobs.generator import current
from compute_horde_validator.validator.utils import MACHINE_SPEC_GROUP_NAME

JOB_LENGTH = 300
TIMEOUT_LEEWAY = 1
TIMEOUT_MARGIN = 60
TIMEOUT_BARRIER = JOB_LENGTH - 65


logger = logging.getLogger(__name__)


@lru_cache(maxsize=100)
def batch_id_to_uuid(batch_id: int) -> uuid.UUID:
    return uuid.uuid4()


class JobState:
    def __init__(self):
        self.miner_ready_or_declining_future = asyncio.Future()
        self.miner_ready_or_declining_timestamp: int = 0
        self.miner_finished_or_failed_future = asyncio.Future()
        self.miner_finished_or_failed_timestamp: int = 0
        self.miner_machine_specs: MachineSpecs | None = None


class MinerClient(AbstractMinerClient):
    def __init__(
        self,
        loop: asyncio.AbstractEventLoop,
        miner_address: str,
        my_hotkey: str,
        miner_hotkey: str,
        miner_port: int,
        job_uuid: str,
        keypair: bittensor.Keypair,
    ):
        super().__init__(loop, f"{miner_hotkey}({miner_address}:{miner_port})")
        self.miner_hotkey = miner_hotkey
        self.my_hotkey = my_hotkey
        self.miner_address = miner_address
        self.miner_port = miner_port
        self.job_states = {}
        if job_uuid is not None:
            self.add_job(job_uuid)
        self.keypair = keypair
        self._barrier = None
        self.miner_manifest = asyncio.Future()

    def add_job(self, job_uuid: str):
        job_state = JobState()
        self.job_states[str(job_uuid)] = job_state
        return job_state

    def get_job_state(self, job_uuid: str):
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
        if isinstance(msg, UnauthorizedError):
            logger.error(f"Unauthorized in {self.miner_name}: {msg.code}, details: {msg.details}")
            return
        if isinstance(msg, V0ExecutorManifestRequest):
            self.miner_manifest.set_result(msg.manifest)
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
            job_state.miner_ready_or_declining_timestamp = time.time()
            job_state.miner_ready_or_declining_future.set_result(msg)
        elif isinstance(msg, V0JobFailedRequest | V0JobFinishedRequest):
            job_state.miner_finished_or_failed_future.set_result(msg)
            job_state.miner_finished_or_failed_timestamp = time.time()
        elif isinstance(msg, V0MachineSpecsRequest):
            job_state.miner_machine_specs = msg.specs
        else:
            raise UnsupportedMessageReceived(msg)

    def generate_authentication_message(self):
        payload = AuthenticationPayload(
            validator_hotkey=self.my_hotkey,
            miner_hotkey=self.miner_hotkey,
            timestamp=int(time.time()),
        )
        return V0AuthenticateRequest(
            payload=payload, signature=f"0x{self.keypair.sign(payload.blob_for_signing()).hex()}"
        )

    def generate_receipt_message(
        self, job: JobBase, started_timestamp: float, time_took_seconds: float, score: float
    ) -> V0ReceiptRequest:
        time_started = datetime.datetime.fromtimestamp(started_timestamp, datetime.UTC)
        receipt_payload = ReceiptPayload(
            job_uuid=str(job.job_uuid),
            miner_hotkey=job.miner.hotkey,
            validator_hotkey=self.my_hotkey,
            time_started=time_started,
            time_took_us=int(time_took_seconds * 1_000_000),
            score_str=f"{score:.6f}",
        )
        return V0ReceiptRequest(
            payload=receipt_payload,
            signature=f"0x{self.keypair.sign(receipt_payload.blob_for_signing()).hex()}",
        )

    async def _connect(self):
        ws = await super()._connect()
        await ws.send(self.generate_authentication_message().model_dump_json())
        return ws


def create_and_run_sythethic_job_batch(netuid, network) -> list[SyntheticJob]:
    batch = SyntheticJobBatch.objects.create(
        accepting_results_until=now() + datetime.timedelta(seconds=JOB_LENGTH)
    )
    if settings.DEBUG_MINER_KEY:
        miners = [Miner.objects.get_or_create(hotkey=settings.DEBUG_MINER_KEY)[0]]
        axons_by_key = {
            settings.DEBUG_MINER_KEY: bittensor.AxonInfo(
                version=4,
                ip=settings.DEBUG_MINER_ADDRESS,
                ip_type=4,
                port=settings.DEBUG_MINER_PORT,
                hotkey=settings.DEBUG_MINER_KEY,
                coldkey=settings.DEBUG_MINER_KEY,  # I hope it does not matter
            )
        }
    else:
        metagraph = bittensor.metagraph(netuid, network=network)
        axons_by_key = {n.hotkey: n.axon_info for n in metagraph.neurons}
        miners = get_miners(metagraph)
    miners = [(miner.id, miner.hotkey) for miner in miners]
    execute_synthetic_batch(axons_by_key, batch.id, miners)


@async_to_sync
async def execute_synthetic_batch(axons_by_key, batch_id, miners):
    serving_miners = [
        (miner_id, miner_key)
        for miner_id, miner_key in miners
        if axons_by_key[miner_key].is_serving
    ]

    tasks = [
        asyncio.create_task(
            asyncio.wait_for(
                execute_miner_synthetic_jobs(
                    batch_id, miner_id, miner_key, axons_by_key[miner_key]
                ),
                JOB_LENGTH,
            )
        )
        for miner_id, miner_key in serving_miners
    ]
    await asyncio.gather(*tasks, return_exceptions=True)


async def execute_miner_synthetic_jobs(batch_id, miner_id, miner_hotkey, axon_info):
    loop = asyncio.get_event_loop()
    key = settings.BITTENSOR_WALLET().get_hotkey()
    client = MinerClient(
        loop=loop,
        miner_address=axon_info.ip,
        miner_port=axon_info.port,
        miner_hotkey=miner_hotkey,
        my_hotkey=key.ss58_address,
        job_uuid=None,
        keypair=key,
    )
    async with client:
        try:
            manifest = await asyncio.wait_for(client.miner_manifest, 30)
        except TimeoutError:
            logger.error(
                "Cannot send synthetic jobs to miner %s: manifest future timed out", miner_hotkey
            )
            return
        executor_count = 0
        for executor_class_manifest in manifest.executor_classes:
            if executor_class_manifest.executor_class != 0:
                logger.warning("Executor classed other than 0 are not supported yet")
                continue
            executor_count = executor_class_manifest.count
            break
        jobs = list(
            await SyntheticJob.objects.abulk_create(
                [
                    SyntheticJob(
                        batch_id=batch_id,
                        miner_id=miner_id,
                        miner_address=axon_info.ip,
                        miner_address_ip_version=axon_info.ip_type,
                        miner_port=axon_info.port,
                        status=SyntheticJob.Status.PENDING,
                    )
                    for i in range(executor_count)
                ]
            )
        )
        for job in jobs:
            client.add_job(str(job.job_uuid))
        await execute_jobs(client, jobs)


async def _execute_job(
    client, job: JobBase
) -> tuple[
    float | None,
    V0DeclineJobRequest | V0ExecutorFailedRequest | V0JobFailedRequest | V0JobFinishedRequest,
]:
    job_state = client.get_job_state(str(job.job_uuid))
    job_generator = current.SyntheticJobGenerator()
    await job_generator.ainit()
    job.job_description = job_generator.job_description()
    await job.asave()
    await client.send_model(
        V0InitialJobRequest(
            job_uuid=str(job.job_uuid),
            base_docker_image_name=job_generator.base_docker_image_name(),
            timeout_seconds=job_generator.timeout_seconds(),
            volume_type=VolumeType.inline.value,
        )
    )
    try:
        msg = await asyncio.wait_for(job_state.miner_ready_or_declining_future, JOB_LENGTH)
    except TimeoutError:
        msg = None

    # wait for barrier even for declined and failed requests
    try:
        await asyncio.wait_for(client.get_barrier().wait(), TIMEOUT_BARRIER)
    except TimeoutError:
        logger.info(
            f"Miner {client.miner_name} barrier timeout - some executors would not be tested."
        )

    if isinstance(msg, V0DeclineJobRequest | V0ExecutorFailedRequest) or msg is None:
        logger.info(f"Miner {client.miner_name} won't do job: {msg} [{job.job_description}]")
        job.status = JobBase.Status.FAILED
        if msg is not None:
            job.comment = f"Miner didn't accept the job saying: {msg.model_dump_json()}"
        else:
            job.comment = "Miner didn't accept the job - times out."
        await job.asave()
        return None, msg
    elif isinstance(msg, V0ExecutorReadyRequest):
        logger.debug(f"Miner {client.miner_name} ready for job: {msg} [{job.job_description}]")
    else:
        raise ValueError(f"Unexpected msg: {msg}")

    # generate before locking on barrier
    volume_contents = await job_generator.volume_contents()

    await client.send_model(
        V0JobRequest(
            job_uuid=str(job.job_uuid),
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
            job_generator.timeout_seconds() + TIMEOUT_LEEWAY + TIMEOUT_MARGIN,
        )
        time_took = job_state.miner_finished_or_failed_timestamp - full_job_sent
        if time_took > (job_generator.timeout_seconds() + TIMEOUT_LEEWAY):
            logger.info(f"Miner {client.miner_name} sent a job result but too late: {msg}")
            logger.info(
                f"Miner {client.miner_name} sent a job result but too late: "
                f"{time_took} {job_generator.timeout_seconds() + TIMEOUT_LEEWAY} "
                f"{job_state.miner_finished_or_failed_timestamp} {full_job_sent}"
            )
            raise TimeoutError
        if time_took < 10:
            logger.warning(
                f"Miner {client.miner_name} finished job too quickly: {time_took} - will default to 10s"
            )
            time_took = 10
    except TimeoutError:
        logger.info(f"Miner {client.miner_name} timed out")
        job.status = SyntheticJob.Status.FAILED
        job.comment = "Miner timed out"
        await job.asave()
        return None, msg

    if isinstance(msg, V0JobFailedRequest):
        logger.info(f"Miner {client.miner_name} failed: {msg}")
        job.status = job.Status.FAILED
        job.comment = f"Miner failed: {msg.model_dump_json()}"
        await job.asave()
        return None, msg
    elif isinstance(msg, V0JobFinishedRequest):
        success, comment, score = job_generator.verify(msg, time_took)

        # Send machine specs to facilitator
        if job_state.miner_machine_specs is not None:
            logger.debug(
                f"Miner {client.miner_name} sent machine specs: {job_state.miner_machine_specs.specs}"
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
            logger.info(f"Miner {client.miner_name} finished: {time_took=} {score=} {msg}")
            job.status = JobBase.Status.COMPLETED
            job.comment = f"Miner finished: {msg.model_dump_json()}"
            await job.asave()
            try:
                receipt_message = client.generate_receipt_message(
                    job, full_job_sent, time_took, score
                )
                await client.send_model(receipt_message)
                logger.info("Receipt message sent")
            except Exception:
                logger.exception(
                    f"Failed to send receipt to miner {client.miner_name} for job {job.job_uuid}"
                )
            finally:
                return score, msg
        else:
            logger.info(f"Miner {client.miner_name} finished but {comment}")
            job.status = JobBase.Status.FAILED
            job.comment = f"Miner finished but {comment}"
            await job.asave()
            return None, msg
    else:
        raise ValueError(f"Unexpected msg: {msg}")


async def execute_job(client, synthetic_job_id):
    synthetic_job: SyntheticJob = await SyntheticJob.objects.prefetch_related("miner").aget(
        id=synthetic_job_id
    )
    score, msg = await _execute_job(client, synthetic_job)
    if score is not None:
        synthetic_job.score = score
        await synthetic_job.asave()


async def execute_jobs(client, synthetic_jobs: Iterable[SyntheticJob]):
    tasks = [
        asyncio.create_task(asyncio.wait_for(execute_job(client, synthetic_job.id), JOB_LENGTH))
        for synthetic_job in synthetic_jobs
    ]
    await asyncio.gather(*tasks, return_exceptions=True)


def get_miners(metagraph) -> list[Miner]:
    existing = list(Miner.objects.filter(hotkey__in=[n.hotkey for n in metagraph.neurons]))
    existing_keys = [m.hotkey for m in existing]
    new_miners = Miner.objects.bulk_create(
        [Miner(hotkey=n.hotkey) for n in metagraph.neurons if n.hotkey not in existing_keys]
    )
    return existing + new_miners
