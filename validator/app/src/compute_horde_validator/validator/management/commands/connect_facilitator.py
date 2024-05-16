import asyncio
import logging
import os
from typing import Any, Literal, NoReturn, Self

import bittensor
import pydantic
import tenacity
import websockets
from asgiref.sync import async_to_sync
from channels.layers import get_channel_layer
from django.conf import settings
from django.core.management.base import BaseCommand
from pydantic import BaseModel, Extra, Field, root_validator

from compute_horde_validator.validator.miner_driver import run_miner_job
from compute_horde_validator.validator.models import Miner
from compute_horde_validator.validator.synthetic_jobs.utils import MinerClient
from compute_horde_validator.validator.utils import (
    MACHINE_SPEC_GROUP_NAME,
)

logger = logging.getLogger(__name__)

PREPARE_WAIT_TIMEOUT = 300
TOTAL_JOB_TIMEOUT = 300


class Error(BaseModel, extra=Extra.allow):
    msg: str
    type: str
    help: str = ""


class Response(BaseModel, extra=Extra.forbid):
    """Message sent from facilitator to validator in response to AuthenticationRequest & JobStatusUpdate"""

    status: Literal["error", "success"]
    errors: list[Error] = []


class AuthenticationRequest(BaseModel, extra=Extra.forbid):
    """Message sent from validator to facilitator to authenticate itself"""

    message_type: str = "V0AuthenticationRequest"
    public_key: str
    signature: str

    @classmethod
    def from_keypair(cls, keypair: bittensor.Keypair) -> Self:
        return cls(
            public_key=keypair.public_key.hex(),
            signature=f"0x{keypair.sign(keypair.public_key).hex()}",
        )


class AuthenticationError(Exception):
    def __init__(self, reason: str, errors: list[Error]):
        self.reason = reason
        self.errors = errors


class JobRequest(BaseModel, extra=Extra.forbid):
    """Message sent from facilitator to validator to request a job execution"""

    # this points to a `ValidatorConsumer.job_new` handler (fuck you django-channels!)
    type: str = Field("job.new", const=True)
    message_type: str = "V0JobRequest"

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
        if not (bool(values.get("docker_image")) or bool(values.get("raw_script"))):
            raise ValueError("Expected at least one of `docker_image` or `raw_script`")
        return values


class Heartbeat(BaseModel, extra=Extra.forbid):
    message_type: str = "V0Heartbeat"


class MachineSpecsUpdate(BaseModel, extra=Extra.forbid):
    message_type: str = "V0MachineSpecsUpdate"
    miner_hotkey: str
    validator_hotkey: str
    specs: dict[str, Any]

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
        self.channel_layer = get_channel_layer()
        self.channel_name = None

    def connect(self):
        """Create an awaitable/async-iterable websockets.connect() object"""
        extra_headers = {
            "X-Validator-Runner-Version": os.environ.get("VALIDATOR_RUNNER_VERSION", "unknown"),
            "X-Validator-Version": os.environ.get("VALIDATOR_VERSION", "unknown"),
        }
        return websockets.connect(self.facilitator_uri, extra_headers=extra_headers)

    async def miner_driver_awaiter(self):
        """avoid memory leak by awaiting miner driver tasks"""
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
        """connect (and re-connect) to facilitator and keep reading messages ... forever"""

        # setup channel for receiving machine specs
        self.channel_name = await self.channel_layer.new_channel()
        await self.channel_layer.group_add(MACHINE_SPEC_GROUP_NAME, self.channel_name)

        # send machine specs to facilitator
        self.specs_task = asyncio.create_task(self.wait_for_specs())
        async for ws in self.connect():
            try:
                await self.handle_connection(ws)
            except websockets.ConnectionClosed as exc:
                logger.warning(
                    "validator connection closed with code %r and reason %r, reconnecting...",
                    exc.code,
                    exc.reason,
                )

    async def handle_connection(self, ws: websockets.WebSocketClientProtocol):
        """handle a single websocket connection"""
        await ws.send(AuthenticationRequest.from_keypair(self.keypair).json())

        raw_msg = await ws.recv()
        try:
            response = Response.parse_raw(raw_msg)
        except pydantic.ValidationError as exc:
            raise AuthenticationError(
                "did not receive Response for AuthenticationRequest", []
            ) from exc
        if response.status != "success":
            raise AuthenticationError("auth request received failed response", response.errors)

        self.ws = ws

        async for raw_msg in ws:
            await self.handle_message(raw_msg)

    async def wait_for_specs(self):
        specs_queue = []
        while True:
            validator_hotkey = settings.BITTENSOR_WALLET().hotkey.ss58_address
            msg = await self.channel_layer.receive(self.channel_name)
            specs = MachineSpecsUpdate(
                specs=msg["specs"],
                miner_hotkey=msg["miner_hotkey"],
                validator_hotkey=validator_hotkey,
            )
            logger.debug(f"sending machine specs update to facilitator: {specs}")

            if self.ws is None:
                specs_queue.append(specs)
            else:
                while len(specs_queue) > 0:
                    await self.send_model(specs_queue.pop(0))
                await self.send_model(specs)

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
        retry=tenacity.retry_if_exception_type(websockets.ConnectionClosed),
    )
    async def send_model(self, msg: BaseModel):
        if self.ws is None:
            raise websockets.ConnectionClosed
        await self.ws.send(msg.json())

    async def handle_message(self, raw_msg: str | bytes):
        """handle message received from facilitator"""
        try:
            response = Response.parse_raw(raw_msg)
        except pydantic.ValidationError:
            logger.debug("could not parse raw message as Response")
        else:
            if response.status != "success":
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

    async def miner_driver(self, job_request: JobRequest):
        """ drive a miner client from job start to completion, then close miner connection """
        miner, _ = await Miner.objects.aget_or_create(hotkey=job_request.miner_hotkey)
        await run_miner_job(
            job_request=job_request,
            miner=miner, 
            job_description="User job from facilitator", 
            keypair=self.keypair, 
            total_job_timeout=TOTAL_JOB_TIMEOUT, 
            wait_timeout=PREPARE_WAIT_TIMEOUT, 
            notify_callback=self.send_model
        )


class Command(BaseCommand):
    FACILITATOR_CLIENT_CLASS = FacilitatorClient

    @async_to_sync
    async def handle(self, *args, **options):
        keypair = settings.BITTENSOR_WALLET().get_hotkey()
        facilitator_client = self.FACILITATOR_CLIENT_CLASS(keypair, settings.FACILITATOR_URI)
        async with facilitator_client:
            await facilitator_client.run_forever()
