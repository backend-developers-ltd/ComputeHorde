import asyncio
import logging
import time
from typing import Literal, Self
from typing import NoReturn

import bittensor
import pydantic
import websockets
from asgiref.sync import async_to_sync
from bittensor import Keypair
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
from compute_horde_validator.validator.synthetic_jobs.utils import MinerClient

logger = logging.getLogger(__name__)

DEFAULT_JOB_TIMEOUT = 300


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
    def from_keypair(cls, keypair: Keypair) -> Self:
        return cls(
            public_key=keypair.public_key.hex(),
            signature=f'0x{keypair.sign(keypair.public_key).hex()}',
        )

    def verify_signature(self) -> bool:
        public_key_bytes = bytes.fromhex(self.public_key)
        keypair = Keypair(public_key=public_key_bytes, ss58_format=42)
        return keypair.verify(public_key_bytes, self.signature)

    @property
    def ss58_address(self) -> str:
        return Keypair(public_key=bytes.fromhex(self.public_key), ss58_format=42).ss58_address


class JobRequest(BaseModel, extra=Extra.forbid):
    """ Message sent from facilitator to validator to request a job execution """

    # this points to a `ValidatorConsumer.job_new` handler (fuck you django-channels!)
    type: str = Field('job.new', const=True)

    uuid: str
    docker_image_url: str
    raw_script: str
    args: list[str]
    env: dict
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


class FacilitatorClient:
    def __init__(self, keypair: bittensor.Keypair):
        self.keypair = keypair
        self.ws: websockets.WebSocketClientProtocol | None = None
        self.ws_connect = websockets.connect(f"ws://{settings.FACILITATOR_ADDRESS}:{settings.FACILITATOR_PORT}/ws/v0/")
        self.miner_drivers = asyncio.Queue()
        self.miner_driver_awaiter_task = asyncio.create_task(self.miner_driver_awaiter())

    async def miner_driver_awaiter(self):
        """ avoid memory leak by awaiting miner driver tasks """
        while True:
            task = await self.miner_drivers.get()
            if task is None:
                return

            try:
                await task
            except Exception as exc:
                logger.error("Error occurred during waiting for a miner driver: %r", exc)

    async def __aenter__(self):
        pass

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.miner_drivers.put(None)
        await self.miner_drivers.join()
        await self.miner_driver_awaiter_task

    def my_hotkey(self) -> str:
        return self.keypair.ss58_address

    async def run_forever(self) -> NoReturn:
        """ connect (and re-connect) to facilitator and keep reading messages ... forever """
        async for ws in self.ws_connect:
            await ws.send(AuthenticationRequest.from_keypair(self.keypair).json())
            self.ws = ws

            try:
                raw_msg = await asyncio.wait_for(ws.recv(), timeout=10)
                await self.handle_message(raw_msg)
            except websockets.ConnectionClosed as exc:
                logger.warning("validator connection closed with code %r and reason %r, reconnecting...", exc.code, exc.reason)

    async def send_model(self, msg: BaseModel):
        retry_count = 0
        while True:
            try:
                if self.ws is None:
                    raise websockets.ConnectionClosed
                await self.ws.send(msg.json())
            except websockets.ConnectionClosed:
                # wait for run_forever loop to reconnect
                if retry_count > 10:
                    raise
                await asyncio.sleep(0.1 * 2 ** retry_count)
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

        # TODO: if passed from validator, get them from job_request
        miner_address = ...
        miner_port = ...
        miner_hotkey = ...

        miner_client = MinerClient(
            loop=asyncio.get_event_loop(),
            miner_address=miner_address,
            miner_port=miner_port,
            miner_hotkey=miner_hotkey,
            my_hotkey=self.my_hotkey(),
            job_uuid=job_request.uuid,
            keypair=self.keypair,
        )
        async with miner_client:
            await miner_client.send_model(V0InitialJobRequest(
                job_uuid=job_request.uuid,
                base_docker_image_name=job_request.docker_image_url,
                timeout_seconds=DEFAULT_JOB_TIMEOUT,
                volume_type=VolumeType.zip_url,
            ))
            msg = await miner_client.miner_ready_or_declining_future
            if isinstance(msg, V0DeclineJobRequest | V0ExecutorFailedRequest):
                logger.info(f"Miner {miner_client.miner_name} won't do job: {msg}")
                await self.send_model(JobStatusUpdate(
                    uuid=job_request.uuid,
                    status='rejected',
                    metadata={'comment': f"Miner didn't accept the job saying: {msg.json()}"},
                ))
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
            await miner_client.send_model(V0JobRequest(
                job_uuid=job_request.uuid,
                docker_image_name=job_request.docker_image_url,
                docker_run_options_preset=docker_run_options_preset,
                docker_run_cmd=job_request.args,
                volume=Volume(volume_type=VolumeType.zip_url, contents=job_request.input_url),  # TODO: raw scripts
                output_upload=OutputUpload(
                    output_upload_type=OutputUploadType.zip_and_http_put,
                    url=job_request.output_url,
                ),
            ))
            full_job_sent = time.time()
            try:
                msg = await asyncio.wait_for(
                    miner_client.miner_finished_or_failed_future,
                    timeout=DEFAULT_JOB_TIMEOUT,
                )
                time_took = miner_client.miner_finished_or_failed_timestamp - full_job_sent
            except TimeoutError:
                logger.error(f'Miner {miner_client.miner_name} timed out out')
                await self.send_model(JobStatusUpdate(
                    uuid=job_request.uuid,
                    status='failed',
                    metadata={'comment': 'Miner timed out'},
                ))
                return
            if isinstance(msg, V0JobFailedRequest):
                logger.info(f'Miner {miner_client.miner_name} failed: {msg}')
                await self.send_model(JobStatusUpdate(
                    uuid=job_request.uuid,
                    status='failed',
                    metadata={'comment': f'Miner failed: {msg.json()}'},
                ))
                return
            elif isinstance(msg, V0JobFinishedRequest):
                logger.info(f'Miner {miner_client.miner_name} finished: {msg}')
                await self.send_model(JobStatusUpdate(
                    uuid=job_request.uuid,
                    status='completed',
                    metadata={'comment': f'Miner finished: {msg.json()}'},
                ))
                return
            else:
                raise ValueError(f'Unexpected msg: {msg}')


class Command(BaseCommand):
    @async_to_sync
    async def handle(self, *args, **options):
        keypair = settings.BITTENSOR_WALLET().get_hotkey()
        vc = FacilitatorClient(keypair)
        await vc.run_forever()
