import asyncio
import base64
import datetime
import io
import logging
import random
import string
import time
import zipfile

import bittensor
from django.conf import settings
from django.utils.timezone import now

from compute_horde.base_requests import BaseRequest
from compute_horde.miner_client.base import AbstractMinerClient, UnsupportedMessageReceived
from compute_horde.mv_protocol import miner_requests, validator_requests
from compute_horde.mv_protocol.miner_requests import (
    BaseMinerRequest,
    V0AcceptJobRequest,
    V0DeclineJobRequest,
    V0ExecutorReadyRequest,
    V0ExecutorFailedRequest,
    V0JobFailedRequest,
    V0JobFinishedRequest,
)
from compute_horde.mv_protocol.validator_requests import V0InitialJobRequest, VolumeType, V0JobRequest
from validator.app.src.compute_horde_validator.validator.models import Miner, SyntheticJob, SyntheticJobBatch


BASE_DOCKER_IMAGE = "alpine"
DOCKER_IMAGE = "ghcr.io/reef-technologies/computehorde/echo:latest"
JOB_LENGTH = 20
SINGLE_JOB_TIMEOUT = 3
TIMEOUT_LEEWAY = 1
TIMEOUT_MARGIN = 10


logger = logging.getLogger(__name__)


class MinerClient(AbstractMinerClient):
    def __init__(self, loop: asyncio.AbstractEventLoop, miner_address: str, my_hotkey: str, miner_hotkey: str,
                 miner_port: int):
        super().__init__(loop, f'{miner_hotkey}({miner_address}:{miner_port})')
        self.miner_hotkey = miner_hotkey
        self.my_hotkey = my_hotkey
        self.miner_address = miner_address
        self.miner_port = miner_port

        self.miner_ready_or_declining_future = asyncio.Future()
        self.miner_ready_or_declining_timestamp: int = 0
        self.miner_finished_or_failed_future = asyncio.Future()
        self.miner_finished_or_failed_timestamp: int = 0

    def miner_url(self) -> str:
        return f'wss://{self.miner_address}:{self.miner_port}/v0/validator_interface/{self.my_hotkey}'

    def accepted_request_type(self) -> type[BaseRequest]:
        return BaseMinerRequest

    def incoming_generic_error_class(self):
        return miner_requests.GenericError

    def outgoing_generic_error_class(self):
        return validator_requests.GenericError

    async def handle_message(self, msg: BaseRequest):
        if isinstance(msg, V0AcceptJobRequest):
            logger.info(f'Miner {self.miner_name} accepted job')
        elif isinstance(
                msg,
                (V0DeclineJobRequest, V0ExecutorFailedRequest, V0ExecutorReadyRequest)
        ):
            self.miner_ready_or_declining_timestamp = time.time()
            self.miner_ready_or_declining_future.set_result(msg)
        elif isinstance(
            msg,
            (V0JobFailedRequest, V0JobFinishedRequest)
        ):
            self.miner_finished_or_failed_future.set_result(msg)
            self.miner_finished_or_failed_timestamp = time.time()
        else:
            raise UnsupportedMessageReceived(msg)


def initiate_jobs(netuid, network) -> list[SyntheticJob]:
    metagraph = bittensor.metagraph(netuid, network=network)
    neurons_by_key = {n.hotkey: n for n in metagraph.neurons}
    batch = SyntheticJobBatch.objects.create(
        accepting_results_until=now() + datetime.timedelta(seconds=JOB_LENGTH)
    )

    miners = get_miners(metagraph)
    return list(SyntheticJob.objects.bulk_create([
        SyntheticJob(
            batch=batch,
            miner=miner,
            miner_address=neurons_by_key[miner.hotkey].axon_info.ip,
            miner_address_ip_version=neurons_by_key[miner.hotkey].axon_info.ip_type,
            miner_port=neurons_by_key[miner.hotkey].axon_info.port,
            status=SyntheticJob.Status.PENDING
        ) for miner in miners]
    ))


def get_synthetic_job_contents(payload):
    in_memory_output = io.BytesIO()
    zipf = zipfile.ZipFile(in_memory_output, 'w')
    zipf.writestr('payload.txt', payload)
    zipf.close()
    in_memory_output.seek(0)
    zip_contents = in_memory_output.read()
    return base64.b64encode(zip_contents).decode()


def verify_result(msg: V0JobFinishedRequest, payload: str):
    return msg.docker_process_stdout == payload


async def execute_job(synthetic_job_id):
    synthetic_job: SyntheticJob = await SyntheticJob.objects.aget(id=synthetic_job_id)
    loop = asyncio.get_event_loop()
    client = MinerClient(
        loop=loop,
        miner_address=synthetic_job.miner_address,
        miner_port=synthetic_job.miner_port,
        miner_hotkey=synthetic_job.miner_hotkey,
        my_hotkey=settings.BITTENSOR_HOTKEY,
    )
    async with client:
        await client.send_model(V0InitialJobRequest(
            job_uuid=synthetic_job.job_uuid,
            base_docker_image_name=BASE_DOCKER_IMAGE,
            timeout_seconds=SINGLE_JOB_TIMEOUT,
            volume_type=VolumeType.inline.value,
        ))
        msg = await client.miner_ready_or_declining_future
        if isinstance(msg, (V0DeclineJobRequest, V0ExecutorFailedRequest)):
            logger.info(f'Miner {client.miner_name} won\'t do job: {msg}')
            synthetic_job.status = SyntheticJob.Status.FAILED
            synthetic_job.comment = f'Miner didn\'t accept the job saying: {msg.json()}'
            await synthetic_job.asave()
            return
        elif isinstance(msg, V0ExecutorReadyRequest):
            logger.debug(f'Miner {client.miner_name} ready for job: {msg}')
        else:
            raise ValueError(f'Unexpected msg: {msg}')

        payload = ''.join(random.choice(string.ascii_uppercase + string.digits) for _ in range(32))
        await client.send_model(V0JobRequest(
            job_uuid=synthetic_job.job_uuid,
            docker_image_name=DOCKER_IMAGE,
            volume={
                'volume_type': VolumeType.inline.value,
                'contents': get_synthetic_job_contents(payload),
            }
        ))
        full_job_sent = time.time()
        try:
            msg = await asyncio.wait_for(
                client.miner_finished_or_failed_future,
                SINGLE_JOB_TIMEOUT + TIMEOUT_LEEWAY + TIMEOUT_MARGIN
            )
            if (client.miner_finished_or_failed_timestamp - full_job_sent) > SINGLE_JOB_TIMEOUT + TIMEOUT_LEEWAY:
                logger.info(f'Miner {client.miner_name} sent a job result but too late: {msg}')
                raise TimeoutError
        except TimeoutError:
            logger.info(f'Miner {client.miner_name} timed out out')
            synthetic_job.status = SyntheticJob.Status.FAILED
            synthetic_job.comment = f'Miner timed out'
            await synthetic_job.asave()
            return

        if isinstance(msg, V0JobFailedRequest):
            logger.info(f'Miner {client.miner_name} failed: {msg}')
            synthetic_job.status = SyntheticJob.Status.FAILED
            synthetic_job.comment = f'Miner failed: {msg.json()}'
            await synthetic_job.asave()
            return
        elif isinstance(msg, V0JobFinishedRequest):
            success = verify_result(msg, payload)
            if success:
                logger.info(f'Miner {client.miner_name} finished: {msg}')
                synthetic_job.status = SyntheticJob.Status.COMPLETED
                synthetic_job.comment = f'Miner finished: {msg.json()}'
                await synthetic_job.asave()
                return
            else:
                logger.info(f'Miner {client.miner_name} finished but result does not match payload: {payload=} {msg=}')
                synthetic_job.status = SyntheticJob.Status.FAILED
                synthetic_job.comment = f'Miner finished but result does not match payload: {payload=}: {msg.json()}'
                await synthetic_job.asave()
                return
        else:
            raise ValueError(f'Unexpected msg: {msg}')


def get_miners(metagraph) -> list[Miner]:
    existing = Miner.objects.filter(hotkey__in=[n.hotkey for n in metagraph.neurons])
    existing_keys = [m.hotkey for m in existing]
    new_miners = Miner.objects.bulk_create([
        Miner(hotkey=n.hotkey)
        for n in metagraph.neurons if n.hotkey not in existing_keys
    ])
    return existing + new_miners


async def debug_run_synthetic_jobs():
    """
    For running in dev environment, not in production
    """
    jobs = initiate_jobs(settings.BITTENSOR_NETUID, settings.BITTENSOR_NETWORK)
    await asyncio.wait_for(asyncio.gather(*[execute_job(job.id) for job in jobs]), JOB_LENGTH)
