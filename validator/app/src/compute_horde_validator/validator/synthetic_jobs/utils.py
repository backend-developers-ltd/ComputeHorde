import asyncio
import datetime
import logging
import time
from collections.abc import Iterable

import bittensor
from compute_horde.base_requests import BaseRequest
from compute_horde.miner_client.base import AbstractMinerClient, UnsupportedMessageReceived
from compute_horde.mv_protocol import miner_requests, validator_requests
from compute_horde.mv_protocol.miner_requests import (
    BaseMinerRequest,
    UnauthorizedError,
    V0AcceptJobRequest,
    V0DeclineJobRequest,
    V0ExecutorFailedRequest,
    V0ExecutorReadyRequest,
    V0JobFailedRequest,
    V0JobFinishedRequest,
)
from compute_horde.mv_protocol.validator_requests import (
    AuthenticationPayload,
    V0AuthenticateRequest,
    V0InitialJobRequest,
    V0JobRequest,
    VolumeType,
)
from django.conf import settings
from django.utils.timezone import now

from compute_horde_validator.validator.models import JobBase, Miner, SyntheticJob, SyntheticJobBatch
from compute_horde_validator.validator.synthetic_jobs.generator import current

JOB_LENGTH = 300
TIMEOUT_LEEWAY = 1
TIMEOUT_MARGIN = 10


logger = logging.getLogger(__name__)


class MinerClient(AbstractMinerClient):
    def __init__(self, loop: asyncio.AbstractEventLoop, miner_address: str, my_hotkey: str, miner_hotkey: str,
                 miner_port: int, job_uuid: str, keypair: bittensor.Keypair):
        super().__init__(loop, f'{miner_hotkey}({miner_address}:{miner_port})')
        self.miner_hotkey = miner_hotkey
        self.my_hotkey = my_hotkey
        self.miner_address = miner_address
        self.miner_port = miner_port
        self.job_uuid = job_uuid
        self.keypair = keypair

        self.miner_ready_or_declining_future = asyncio.Future()
        self.miner_ready_or_declining_timestamp: int = 0
        self.miner_finished_or_failed_future = asyncio.Future()
        self.miner_finished_or_failed_timestamp: int = 0

    def miner_url(self) -> str:
        return f'ws://{self.miner_address}:{self.miner_port}/v0/validator_interface/{self.my_hotkey}'

    def accepted_request_type(self) -> type[BaseRequest]:
        return BaseMinerRequest

    def incoming_generic_error_class(self):
        return miner_requests.GenericError

    def outgoing_generic_error_class(self):
        return validator_requests.GenericError

    async def handle_message(self, msg: BaseRequest):
        if isinstance(msg, UnauthorizedError):
            logger.error(f'Unauthorized in {self.miner_name}: {msg.code}, details: {msg.details}')
            return
        if msg.job_uuid != self.job_uuid:
            logger.info(f'Recevied info about another job: {msg}')
            return
        if isinstance(msg, V0AcceptJobRequest):
            logger.info(f'Miner {self.miner_name} accepted job')
        elif isinstance(
                msg,
                V0DeclineJobRequest | V0ExecutorFailedRequest | V0ExecutorReadyRequest
        ):
            self.miner_ready_or_declining_timestamp = time.time()
            self.miner_ready_or_declining_future.set_result(msg)
        elif isinstance(
            msg,
            V0JobFailedRequest | V0JobFinishedRequest
        ):
            self.miner_finished_or_failed_future.set_result(msg)
            self.miner_finished_or_failed_timestamp = time.time()
        else:
            raise UnsupportedMessageReceived(msg)

    def generate_authentication_message(self):
        payload = AuthenticationPayload(
            validator_hotkey=self.my_hotkey,
            miner_hotkey=self.miner_hotkey,
            timestamp=int(time.time()),
        )
        return V0AuthenticateRequest(
            payload=payload,
            signature=f"0x{self.keypair.sign(payload.blob_for_signing()).hex()}"
        )

    async def _connect(self):
        ws = await super()._connect()
        await ws.send(self.generate_authentication_message().json())
        return ws


def initiate_jobs(netuid, network) -> list[SyntheticJob]:
    metagraph = bittensor.metagraph(netuid, network=network)
    neurons_by_key = {n.hotkey: n for n in metagraph.neurons}
    batch = SyntheticJobBatch.objects.create(
        accepting_results_until=now() + datetime.timedelta(seconds=JOB_LENGTH)
    )
    if settings.DEBUG_MINER_KEY:
        return [SyntheticJob.objects.create(
            batch=batch,
            miner=Miner.objects.get_or_create(hotkey=settings.DEBUG_MINER_KEY)[0],
            miner_address=settings.DEBUG_MINER_ADDRESS,
            miner_address_ip_version=4,
            miner_port=settings.DEBUG_MINER_PORT,
            status=SyntheticJob.Status.PENDING
        )]
    miners = get_miners(metagraph)
    return list(SyntheticJob.objects.bulk_create([
        SyntheticJob(
            batch=batch,
            miner=miner,
            miner_address=neurons_by_key[miner.hotkey].axon_info.ip,
            miner_address_ip_version=neurons_by_key[miner.hotkey].axon_info.ip_type,
            miner_port=neurons_by_key[miner.hotkey].axon_info.port,
            status=SyntheticJob.Status.PENDING
        ) for miner in miners if neurons_by_key[miner.hotkey].axon_info.is_serving]
    ))


async def _execute_job(job: JobBase) -> tuple[
    float | None,
    V0DeclineJobRequest | V0ExecutorFailedRequest | V0JobFailedRequest | V0JobFinishedRequest
]:
    job_generator = current.SyntheticJobGenerator()
    job.job_description = job_generator.job_description()
    await job.asave()
    loop = asyncio.get_event_loop()
    key = settings.BITTENSOR_WALLET().get_hotkey()
    client = MinerClient(
        loop=loop,
        miner_address=job.miner_address,
        miner_port=job.miner_port,
        miner_hotkey=job.miner.hotkey,
        my_hotkey=key.ss58_address,
        job_uuid=str(job.job_uuid),
        keypair=key,
    )
    async with client:
        await client.send_model(V0InitialJobRequest(
            job_uuid=str(job.job_uuid),
            base_docker_image_name=job_generator.base_docker_image_name(),
            timeout_seconds=job_generator.timeout_seconds(),
            volume_type=VolumeType.inline.value,
        ))
        msg = await client.miner_ready_or_declining_future
        if isinstance(msg, V0DeclineJobRequest | V0ExecutorFailedRequest):
            logger.info(f'Miner {client.miner_name} won\'t do job: {msg}')
            job.status = JobBase.Status.FAILED
            job.comment = f'Miner didn\'t accept the job saying: {msg.json()}'
            await job.asave()
            return None, msg
        elif isinstance(msg, V0ExecutorReadyRequest):
            logger.debug(f'Miner {client.miner_name} ready for job: {msg}')
        else:
            raise ValueError(f'Unexpected msg: {msg}')

        await client.send_model(V0JobRequest(
            job_uuid=str(job.job_uuid),
            docker_image_name=job_generator.docker_image_name(),
            docker_run_options_preset=job_generator.docker_run_options_preset(),
            docker_run_cmd=job_generator.docker_run_cmd(),
            volume={
                'volume_type': VolumeType.inline.value,
                'contents': job_generator.volume_contents(),
            },
            output_upload=None,
        ))
        full_job_sent = time.time()
        msg = None
        try:
            msg = await asyncio.wait_for(
                client.miner_finished_or_failed_future,
                job_generator.timeout_seconds() + TIMEOUT_LEEWAY + TIMEOUT_MARGIN
            )
            time_took = client.miner_finished_or_failed_timestamp - full_job_sent
            if time_took > (job_generator.timeout_seconds() + TIMEOUT_LEEWAY):
                logger.info(f'Miner {client.miner_name} sent a job result but too late: {msg}')
                raise TimeoutError
        except TimeoutError:
            logger.info(f'Miner {client.miner_name} timed out out')
            job.status = SyntheticJob.Status.FAILED
            job.comment = 'Miner timed out'
            await job.asave()
            return None, msg

        if isinstance(msg, V0JobFailedRequest):
            logger.info(f'Miner {client.miner_name} failed: {msg}')
            job.status = job.Status.FAILED
            job.comment = f'Miner failed: {msg.json()}'
            await job.asave()
            return None, msg
        elif isinstance(msg, V0JobFinishedRequest):
            success, comment, score = job_generator.verify(msg, time_took)
            if success:
                logger.info(f'Miner {client.miner_name} finished: {msg}')
                job.status = JobBase.Status.COMPLETED
                job.comment = f'Miner finished: {msg.json()}'
                await job.asave()
                return score, msg
            else:
                logger.info(f'Miner {client.miner_name} finished but {comment}')
                job.status = JobBase.Status.FAILED
                job.comment = f'Miner finished but {comment}'
                await job.asave()
                return None, msg
        else:
            raise ValueError(f'Unexpected msg: {msg}')


async def execute_job(synthetic_job_id):
    synthetic_job: SyntheticJob = await SyntheticJob.objects.prefetch_related('miner').aget(id=synthetic_job_id)
    score, msg = await _execute_job(synthetic_job)
    if score is not None:
        synthetic_job.score = score
        await synthetic_job.asave()


async def execute_jobs(synthetic_jobs: Iterable[SyntheticJob]):
    tasks = [asyncio.create_task(asyncio.wait_for(execute_job(synthetic_job.id), JOB_LENGTH))
             for synthetic_job in synthetic_jobs]
    await asyncio.wait(tasks)


def get_miners(metagraph) -> list[Miner]:
    existing = list(Miner.objects.filter(hotkey__in=[n.hotkey for n in metagraph.neurons]))
    existing_keys = [m.hotkey for m in existing]
    new_miners = Miner.objects.bulk_create([
        Miner(hotkey=n.hotkey)
        for n in metagraph.neurons if n.hotkey not in existing_keys
    ])
    return existing + new_miners
