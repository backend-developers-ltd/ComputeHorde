import asyncio
import base64
import io
import logging
import time
import zipfile
from functools import cache
from typing import Literal, Self
from typing import NoReturn

import bittensor
import pydantic
import websockets
from asgiref.sync import async_to_sync, sync_to_async
from django.conf import settings
from django.core.management.base import BaseCommand
from pydantic import BaseModel, Extra, Field

from compute_horde.mv_protocol.miner_requests import (
    V0DeclineJobRequest,
    V0ExecutorFailedRequest,
    V0ExecutorReadyRequest,
    V0JobFailedRequest,
    V0JobFinishedRequest,
)
from compute_horde.mv_protocol.validator_requests import V0InitialJobRequest, V0JobRequest, Volume, VolumeType, \
    OutputUpload, OutputUploadType
from compute_horde_validator.validator.models import OrganicJob, Miner
from compute_horde_validator.validator.synthetic_jobs.utils import MinerClient

logger = logging.getLogger(__name__)

PREPARE_WAIT_TIMEOUT = 60
JOB_WAIT_TIMEOUT = 300


class Error(BaseModel, extra=Extra.allow):
    msg: str
    type: str
    help: str = ''


class Response(BaseModel, extra=Extra.forbid):
    """ Message sent from facilitator to validator in response to AuthenticationRequest & JobStatusUpdate """
    status: Literal['error', 'success']
    errors: list[Error] = []


class AuthenticationRequest(BaseModel, extra=Extra.forbid):
    """ Message sent from validator to facilitator to authenticate itself """
    public_key: str
    signature: str

    @classmethod
    def from_keypair(cls, keypair: bittensor.Keypair) -> Self:
        return cls(
            public_key=keypair.public_key.hex(),
            signature=f'0x{keypair.sign(keypair.public_key).hex()}',
        )


class JobRequest(BaseModel, extra=Extra.forbid):
    """ Message sent from facilitator to validator to request a job execution """

    # this points to a `ValidatorConsumer.job_new` handler (fuck you django-channels!)
    type: str = Field('job.new', const=True)

    uuid: str
    miner_hotkey: str
    docker_image: str
    raw_script: str
    args: list[str]
    env: dict[str, str]
    use_gpu: bool
    input_url: str
    output_url: str


class JobStatusUpdate(BaseModel, extra=Extra.forbid):
    """
    Message sent from validator to facilitator in response to NewJobRequest.
    """

    uuid: str
    status: Literal['failed', 'rejected', 'accepted', 'completed']
    metadata: dict = {}


@cache
def get_dummy_inline_zip_volume() -> str:
    in_memory_output = io.BytesIO()
    with zipfile.ZipFile(in_memory_output, 'w'):
        pass
    in_memory_output.seek(0)
    zip_contents = in_memory_output.read()
    base64_zip_contents = base64.b64encode(zip_contents)
    return base64_zip_contents.decode()


@sync_to_async
def get_miner_axon_info(hotkey: str) -> bittensor.AxonInfo:
    metagraph = bittensor.metagraph(netuid=settings.BITTENSOR_NETUID, network=settings.BITTENSOR_NETWORK)
    neurons = [n for n in metagraph.neurons if n.hotkey == hotkey]
    if not neurons:
        raise ValueError(f'Miner with {hotkey=} not present in this subnetowrk')
    return neurons[0].axon_info


class FacilitatorClient:
    MINER_CLIENT_CLASS = MinerClient

    def __init__(self, keypair: bittensor.Keypair, facilitator_address: str, facilitator_port: int):
        self.keypair = keypair
        self.ws: websockets.WebSocketClientProtocol | None = None
        self.facilitator_address = facilitator_address
        self.facilitator_port = facilitator_port
        self.miner_drivers = asyncio.Queue()
        self.miner_driver_awaiter_task = asyncio.create_task(self.miner_driver_awaiter())

    def connect(self):
        """ Create an awaitable/async-iterable websockets.connect() object """
        facilitator_url = f"ws://{self.facilitator_address}:{self.facilitator_port}/ws/v0/"
        return websockets.connect(facilitator_url)

    async def miner_driver_awaiter(self):
        """ avoid memory leak by awaiting miner driver tasks """
        while True:
            task = await self.miner_drivers.get()
            if task is None:
                return

            try:
                await task
            except Exception as exc:
                logger.error("Error occurred during driving a miner client: %r", exc)

    async def __aenter__(self):
        pass

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.miner_drivers.put(None)
        await self.miner_driver_awaiter_task

    def my_hotkey(self) -> str:
        return self.keypair.ss58_address

    async def run_forever(self) -> NoReturn:
        """ connect (and re-connect) to facilitator and keep reading messages ... forever """
        async for ws in self.connect():
            try:
                await self.handle_connection(ws)
            except websockets.ConnectionClosed as exc:
                logger.warning("validator connection closed with code %r and reason %r, reconnecting...",
                               exc.code, exc.reason)

    async def handle_connection(self, ws: websockets.WebSocketClientProtocol):
        """ handle a single websocket connection """
        await ws.send(AuthenticationRequest.from_keypair(self.keypair).json())
        self.ws = ws

        async for raw_msg in ws:
            await self.handle_message(raw_msg)

    async def send_model(self, msg: BaseModel):
        retry_count = 0
        while True:
            try:
                if self.ws is None:
                    raise websockets.ConnectionClosed
                await self.ws.send(msg.json())
                return
            except websockets.ConnectionClosed:
                # wait for run_forever loop to reconnect
                logger.warning("Failed to send message to facilitator, waiting for re-connection")
                if retry_count > 7:
                    raise
                await asyncio.sleep(2 ** retry_count)
                retry_count += 1

    async def handle_message(self, raw_msg: str | bytes):
        """ handle message received from facilitator """
        try:
            msg = pydantic.parse_raw_as(Response | JobRequest, raw_msg)  # type: ignore[arg-type]
        except pydantic.ValidationError:
            logger.error("unsupported message received from facilitator: %s", raw_msg)
            return

        if isinstance(msg, Response):
            if msg.status != 'success':
                logger.error("received error response from facilitator: %r", msg)
        elif isinstance(msg, JobRequest):
            task = asyncio.create_task(self.miner_driver(msg))
            await self.miner_drivers.put(task)

    async def miner_driver(self, job_request: JobRequest):
        """ drive a miner client from job start to completion, the close miner connection """

        miner, _ = await Miner.objects.aget_or_create(hotkey=job_request.miner_hotkey)
        miner_axon_info = await get_miner_axon_info(job_request.miner_hotkey)
        job = await OrganicJob.objects.acreate(
            job_uuid=job_request.uuid,
            miner=miner,
            miner_address=miner_axon_info.ip,
            miner_address_ip_version=miner_axon_info.ip_type,
            miner_port=miner_axon_info.port,
            job_description="User job from facilitator",
        )

        miner_client = self.MINER_CLIENT_CLASS(
            loop=asyncio.get_event_loop(),
            miner_address=miner_axon_info.ip,
            miner_port=miner_axon_info.port,
            miner_hotkey=job_request.miner_hotkey,
            my_hotkey=self.my_hotkey(),
            job_uuid=job_request.uuid,
            keypair=self.keypair,
        )
        async with miner_client:
            await miner_client.send_model(V0InitialJobRequest(
                job_uuid=job_request.uuid,
                base_docker_image_name=job_request.docker_image,
                timeout_seconds=JOB_WAIT_TIMEOUT,
                volume_type=VolumeType.zip_url,
            ))

            try:
                msg = await asyncio.wait_for(
                    miner_client.miner_ready_or_declining_future,
                    timeout=PREPARE_WAIT_TIMEOUT,
                )
            except TimeoutError:
                logger.error(f'Miner {miner_client.miner_name} timed out out while preparing executor for job {job_request.uuid}')
                await self.send_model(JobStatusUpdate(
                    uuid=job_request.uuid,
                    status='failed',
                    metadata={'comment': 'Miner timed out while preparing executor'},
                ))
                job.status = OrganicJob.Status.FAILED
                job.comment = f'Miner timed out while preparing executor'
                await job.asave()
                return

            if isinstance(msg, V0DeclineJobRequest | V0ExecutorFailedRequest):
                logger.info(f"Miner {miner_client.miner_name} won't do job: {msg}")
                await self.send_model(JobStatusUpdate(
                    uuid=job_request.uuid,
                    status='rejected',
                    metadata={
                        'comment': "Miner didn't accept the job",
                        'miner_response': msg.dict(),
                    },
                ))
                job.status = OrganicJob.Status.FAILED
                job.comment = f"Miner didn't accept the job saying: {msg.json()}"
                await job.asave()
                return
            elif isinstance(msg, V0ExecutorReadyRequest):
                logger.debug(f'Miner {miner_client.miner_name} ready for job: {msg}')
                await self.send_model(JobStatusUpdate(
                    uuid=job_request.uuid,
                    status='accepted',
                    metadata={},
                ))
            else:
                raise ValueError(f'Unexpected msg: {msg}')

            docker_run_options_preset = 'nvidia_all' if job_request.use_gpu else 'none'
            if job_request.input_url:
                volume = Volume(volume_type=VolumeType.zip_url, contents=job_request.input_url)
            else:
                volume = Volume(volume_type=VolumeType.inline, contents=get_dummy_inline_zip_volume())

            if job_request.output_url:
                output_upload = OutputUpload(
                    output_upload_type=OutputUploadType.zip_and_http_put,
                    url=job_request.output_url,
                )
            else:
                output_upload = None

            await miner_client.send_model(V0JobRequest(
                job_uuid=job_request.uuid,
                docker_image_name=job_request.docker_image,
                docker_run_options_preset=docker_run_options_preset,
                docker_run_cmd=job_request.args,
                volume=volume,  # TODO: raw scripts
                output_upload=output_upload,
            ))
            full_job_sent = time.time()
            try:
                msg = await asyncio.wait_for(
                    miner_client.miner_finished_or_failed_future,
                    timeout=JOB_WAIT_TIMEOUT,
                )
                time_took = miner_client.miner_finished_or_failed_timestamp - full_job_sent
                logger.info(f"Miner took {time_took} seconds to finish {job_request.uuid}")
            except TimeoutError:
                logger.error(f'Miner {miner_client.miner_name} timed out out')
                await self.send_model(JobStatusUpdate(
                    uuid=job_request.uuid,
                    status='failed',
                    metadata={'comment': 'Miner timed out'},
                ))
                job.status = OrganicJob.Status.FAILED
                job.comment = 'Miner timed out'
                await job.asave()
                return
            if isinstance(msg, V0JobFailedRequest):
                logger.info(f'Miner {miner_client.miner_name} failed: {msg}')
                await self.send_model(JobStatusUpdate(
                    uuid=job_request.uuid,
                    status='failed',
                    metadata={'comment': 'Miner failed', 'miner_response': msg.dict()},
                ))
                job.status = OrganicJob.Status.FAILED
                job.comment = f'Miner failed: {msg.json()}'
                await job.asave()
                return
            elif isinstance(msg, V0JobFinishedRequest):
                logger.info(f'Miner {miner_client.miner_name} finished: {msg}')
                await self.send_model(JobStatusUpdate(
                    uuid=job_request.uuid,
                    status='completed',
                    metadata={'comment': 'Miner finished', 'miner_response': msg.dict()},
                ))
                job.status = OrganicJob.Status.COMPLETED
                job.comment = f'Miner finished: {msg.json()}'
                await job.asave()
                return
            else:
                raise ValueError(f'Unexpected msg: {msg}')


class Command(BaseCommand):
    FACILITATOR_CLIENT_CLASS = FacilitatorClient

    @async_to_sync
    async def handle(self, *args, **options):
        keypair = settings.BITTENSOR_WALLET().get_hotkey()
        facilitator_client = self.FACILITATOR_CLIENT_CLASS(
            keypair, settings.FACILITATOR_ADDRESS, settings.FACILITATOR_PORT
        )
        with facilitator_client:
            await facilitator_client.run_forever()
