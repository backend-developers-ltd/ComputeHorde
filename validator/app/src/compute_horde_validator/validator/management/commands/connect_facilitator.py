import asyncio
import base64
import contextlib
import io
import logging
import os
import time
import zipfile
from functools import cache
from typing import Any, Literal, NoReturn, Self

import bittensor
import pydantic
import tenacity
import websockets
from asgiref.sync import async_to_sync, sync_to_async
from compute_horde.miner_client.base import MinerConnectionError
from compute_horde.mv_protocol.miner_requests import (
    V0DeclineJobRequest,
    V0ExecutorFailedRequest,
    V0ExecutorReadyRequest,
    V0JobFailedRequest,
    V0JobFinishedRequest,
)
from compute_horde.mv_protocol.validator_requests import (
    OutputUpload,
    OutputUploadType,
    V0InitialJobRequest,
    V0JobRequest,
    Volume,
    VolumeType,
)
from django.conf import settings
from django.core.management.base import BaseCommand
from pydantic import BaseModel, Extra, Field, root_validator

from compute_horde_validator.validator.models import Miner, OrganicJob
from compute_horde_validator.validator.synthetic_jobs.utils import MinerClient
from compute_horde_validator.validator.utils import Timer

logger = logging.getLogger(__name__)

PREPARE_WAIT_TIMEOUT = 300
TOTAL_JOB_TIMEOUT = 300


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
    message_type: str = 'V0AuthenticationRequest'
    public_key: str
    signature: str

    @classmethod
    def from_keypair(cls, keypair: bittensor.Keypair) -> Self:
        return cls(
            public_key=keypair.public_key.hex(),
            signature=f'0x{keypair.sign(keypair.public_key).hex()}',
        )


class AuthenticationError(Exception):
    def __init__(self, reason: str, errors: list[Error]):
        self.reason = reason
        self.errors = errors


class JobRequest(BaseModel, extra=Extra.forbid):
    """ Message sent from facilitator to validator to request a job execution """

    # this points to a `ValidatorConsumer.job_new` handler (fuck you django-channels!)
    type: str = Field('job.new', const=True)
    message_type: str = 'V0JobRequest'

    uuid: str
    miner_hotkey: str
    docker_image: str
    raw_script: str
    args: list[str]
    env: dict[str, str]
    use_gpu: bool
    input_url: str
    output_url: str

    @root_validator()
    def validate(cls, values: dict[str, Any]) -> dict[str, Any]:
        if not (bool(values.get("docker_image_name")) or bool(values.get("raw_script"))):
            raise ValueError("Expected at least one of `docker_image_name` or `raw_script`")
        return values


class Heartbeat(BaseModel, extra=Extra.forbid):
    message_type: str = 'V0Heartbeat'


class MinerResponse(BaseModel, extra=Extra.allow):
    job_uuid: str
    message_type: str
    docker_process_stderr: str
    docker_process_stdout: str


class JobStatusMetadata(BaseModel, extra=Extra.allow):
    comment: str
    miner_response: MinerResponse | None = None


class JobStatusUpdate(BaseModel, extra=Extra.forbid):
    """
    Message sent from validator to facilitator in response to NewJobRequest.
    """

    message_type: str = 'V0JobStatusUpdate'
    uuid: str
    status: Literal['failed', 'rejected', 'accepted', 'completed']
    metadata: JobStatusMetadata | None = None


@cache
def get_dummy_inline_zip_volume() -> str:
    in_memory_output = io.BytesIO()
    with zipfile.ZipFile(in_memory_output, 'w'):
        pass
    in_memory_output.seek(0)
    zip_contents = in_memory_output.read()
    base64_zip_contents = base64.b64encode(zip_contents)
    return base64_zip_contents.decode()


class AsyncMetagraphClient:
    def __init__(self):
        self._metagraph_future = None
        self._future_lock = asyncio.Lock()

    async def get_metagraph(self):
        future = None
        set_result = False
        async with self._future_lock:
            if self._metagraph_future is None:
                future = self._metagraph_future = asyncio.Future()
                set_result = True
            else:
                future = self._metagraph_future
        if set_result:
            try:
                result = await self._get_metagraph()
            except Exception as exc:
                future.set_exception(exc)
                raise
            else:
                future.set_result(result)
                return result
            finally:
                async with self._future_lock:
                    self._metagraph_future = None
        else:
            return await future

    @sync_to_async(thread_sensitive=False)
    def _get_metagraph(self):
        return bittensor.metagraph(netuid=settings.BITTENSOR_NETUID, network=settings.BITTENSOR_NETWORK)


async_metagraph_client = AsyncMetagraphClient()


class FacilitatorClient:
    MINER_CLIENT_CLASS = MinerClient
    HEARTBEAT_PERIOD = 60

    def __init__(self, keypair: bittensor.Keypair, facilitator_uri: str):
        self.keypair = keypair
        self.ws: websockets.WebSocketClientProtocol | None = None
        self.facilitator_uri = facilitator_uri
        self.miner_drivers = asyncio.Queue()
        self.miner_driver_awaiter_task = asyncio.create_task(self.miner_driver_awaiter())
        self.heartbeat_task = asyncio.create_task(self.heartbeat())

    def connect(self):
        """ Create an awaitable/async-iterable websockets.connect() object """
        extra_headers = {
            'X-Validator-Runner-Version': os.environ.get('VALIDATOR_RUNNER_VERSION', 'unknown'),
            'X-Validator-Version': os.environ.get('VALIDATOR_VERSION', 'unknown'),
        }
        return websockets.connect(self.facilitator_uri, extra_headers=extra_headers)

    async def miner_driver_awaiter(self):
        """ avoid memory leak by awaiting miner driver tasks """
        while True:
            task = await self.miner_drivers.get()
            if task is None:
                return

            try:
                await task
            except Exception:
                logger.error("Error occurred during driving a miner client", exc_info=True)

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

        raw_msg = await ws.recv()
        try:
            response = Response.parse_raw(raw_msg)
        except pydantic.ValidationError as exc:
            raise AuthenticationError("did not receive Response for AuthenticationRequest", []) from exc
        if response.status != 'success':
            raise AuthenticationError("auth request received failed response", response.errors)

        self.ws = ws

        async for raw_msg in ws:
            await self.handle_message(raw_msg)

    async def heartbeat(self):
        while True:
            if self.ws is not None:
                try:
                    await self.send_model(Heartbeat())
                except Exception as exc:
                    logger.error("Error occurred", exc_info=exc)
            await asyncio.sleep(self.HEARTBEAT_PERIOD)

    @tenacity.retry(
        stop=tenacity.stop_after_attempt(7),
        wait=tenacity.wait_exponential(multiplier=1, exp_base=2, min=1, max=10),
        retry=tenacity.retry_if_exception_type(websockets.ConnectionClosed)
    )
    async def send_model(self, msg: BaseModel):
        if self.ws is None:
            raise websockets.ConnectionClosed
        await self.ws.send(msg.json())

    async def handle_message(self, raw_msg: str | bytes):
        """ handle message received from facilitator """
        try:
            response = Response.parse_raw(raw_msg)
        except pydantic.ValidationError:
            logger.debug("could not parse raw message as Response")
        else:
            if response.status != 'success':
                logger.error("received error response from facilitator: %r", response)
            return

        try:
            job_request = JobRequest.parse_raw(raw_msg)
        except pydantic.ValidationError:
            logger.debug("could not parse raw message as JobRequest")
        else:
            task = asyncio.create_task(self.miner_driver(job_request))
            await self.miner_drivers.put(task)
            return

        logger.error("unsupported message received from facilitator: %s", raw_msg)

    async def get_miner_axon_info(self, hotkey: str) -> bittensor.AxonInfo:
        metagraph = await async_metagraph_client.get_metagraph()
        neurons = [n for n in metagraph.neurons if n.hotkey == hotkey]
        if not neurons:
            raise ValueError(f'Miner with {hotkey=} not present in this subnetowrk')
        return neurons[0].axon_info

    async def miner_driver(self, job_request: JobRequest):
        """ drive a miner client from job start to completion, the close miner connection """

        miner, _ = await Miner.objects.aget_or_create(hotkey=job_request.miner_hotkey)
        miner_axon_info = await self.get_miner_axon_info(job_request.miner_hotkey)
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
        async with contextlib.AsyncExitStack() as exit_stack:
            try:
                await exit_stack.enter_async_context(miner_client)
            except MinerConnectionError as exc:
                comment = f'Miner connection error: {exc}'
                logger.error(comment)
                await self.send_model(JobStatusUpdate(
                    uuid=job_request.uuid,
                    status='failed',
                    metadata=JobStatusMetadata(comment=comment),
                ))
                job.status = OrganicJob.Status.FAILED
                job.comment = comment
                await job.asave()
                return

            job_timer = Timer(timeout=TOTAL_JOB_TIMEOUT)
            if job_request.input_url:
                volume = Volume(volume_type=VolumeType.zip_url, contents=job_request.input_url)
            else:
                volume = Volume(volume_type=VolumeType.inline, contents=get_dummy_inline_zip_volume())

            await miner_client.send_model(V0InitialJobRequest(
                job_uuid=job_request.uuid,
                base_docker_image_name=job_request.docker_image or None,
                timeout_seconds=TOTAL_JOB_TIMEOUT,
                volume_type=volume.volume_type.value,
            ))

            try:
                msg = await asyncio.wait_for(
                    miner_client.miner_ready_or_declining_future,
                    timeout=min(job_timer.time_left(), PREPARE_WAIT_TIMEOUT),
                )
            except TimeoutError:
                logger.error(
                    f'Miner {miner_client.miner_name} timed out out while preparing executor for job {job_request.uuid}'
                    f' after {PREPARE_WAIT_TIMEOUT} seconds'
                )
                await self.send_model(JobStatusUpdate(
                    uuid=job_request.uuid,
                    status='failed',
                    metadata=JobStatusMetadata(
                        comment=f'Miner timed out while preparing executor after {PREPARE_WAIT_TIMEOUT} seconds',
                    ),
                ))
                job.status = OrganicJob.Status.FAILED
                job.comment = 'Miner timed out while preparing executor'
                await job.asave()
                return

            if isinstance(msg, V0DeclineJobRequest | V0ExecutorFailedRequest):
                logger.info(f"Miner {miner_client.miner_name} won't do job: {msg}")
                await self.send_model(JobStatusUpdate(
                    uuid=job_request.uuid,
                    status='rejected',
                    metadata=JobStatusMetadata(comment=f"Miner didn't accept the job. Miner sent {msg.message_type}"),
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
                    metadata=JobStatusMetadata(comment="Miner accepted job"),
                ))
            else:
                raise ValueError(f'Unexpected msg: {msg}')

            docker_run_options_preset = 'nvidia_all' if job_request.use_gpu else 'none'

            if job_request.output_url:
                output_upload = OutputUpload(
                    output_upload_type=OutputUploadType.zip_and_http_put,
                    url=job_request.output_url,
                )
            else:
                output_upload = None

            await miner_client.send_model(V0JobRequest(
                job_uuid=job_request.uuid,
                docker_image_name=job_request.docker_image or None,
                raw_script=job_request.raw_script or None,
                docker_run_options_preset=docker_run_options_preset,
                docker_run_cmd=job_request.args,
                volume=volume,  # TODO: raw scripts
                output_upload=output_upload,
            ))
            full_job_sent = time.time()
            try:
                msg = await asyncio.wait_for(
                    miner_client.miner_finished_or_failed_future,
                    timeout=job_timer.time_left(),
                )
                time_took = miner_client.miner_finished_or_failed_timestamp - full_job_sent
                logger.info(f"Miner took {time_took} seconds to finish {job_request.uuid}")
            except TimeoutError:
                logger.error(f'Miner {miner_client.miner_name} timed out after {TOTAL_JOB_TIMEOUT} seconds')
                await self.send_model(JobStatusUpdate(
                    uuid=job_request.uuid,
                    status='failed',
                    metadata=JobStatusMetadata(comment=f'Miner timed out after {TOTAL_JOB_TIMEOUT} seconds'),
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
                    metadata=JobStatusMetadata(
                        comment='Miner failed',
                        miner_response=MinerResponse(
                            job_uuid=msg.job_uuid,
                            message_type=msg.message_type.value,
                            docker_process_stderr=msg.docker_process_stderr,
                            docker_process_stdout=msg.docker_process_stdout,
                        ),
                    ),
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
                    metadata=JobStatusMetadata(
                        comment='Miner finished',
                        miner_response=MinerResponse(
                            job_uuid=msg.job_uuid,
                            message_type=msg.message_type.value,
                            docker_process_stderr=msg.docker_process_stderr,
                            docker_process_stdout=msg.docker_process_stdout,
                        ),
                    ),
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
        facilitator_client = self.FACILITATOR_CLIENT_CLASS(keypair, settings.FACILITATOR_URI)
        async with facilitator_client:
            await facilitator_client.run_forever()
