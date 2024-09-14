import asyncio
import logging
import os
from collections import deque
from typing import NoReturn

import bittensor
import pydantic
import tenacity
import websockets
from channels.layers import get_channel_layer
from django.conf import settings
from pydantic import BaseModel

from compute_horde_validator.validator.metagraph_client import (
    create_metagraph_refresh_task,
    get_miner_axon_info,
)
from compute_horde_validator.validator.models import Miner, OrganicJob, SystemEvent
from compute_horde_validator.validator.organic_jobs.facilitator_api import (
    AuthenticationRequest,
    Error,
    Heartbeat,
    JobRequest,
    MachineSpecsUpdate,
    Response,
)
from compute_horde_validator.validator.organic_jobs.miner_client import MinerClient
from compute_horde_validator.validator.organic_jobs.miner_driver import execute_organic_job
from compute_horde_validator.validator.utils import MACHINE_SPEC_CHANNEL

logger = logging.getLogger(__name__)

PREPARE_WAIT_TIMEOUT = 300
TOTAL_JOB_TIMEOUT = 300


class AuthenticationError(Exception):
    def __init__(self, reason: str, errors: list[Error]):
        self.reason = reason
        self.errors = errors


async def save_facilitator_event(subtype: str, long_description: str, data={}, success=False):
    await SystemEvent.objects.using(settings.DEFAULT_DB_ALIAS).acreate(
        type=SystemEvent.EventType.FACILITATOR_CLIENT_ERROR,
        subtype=subtype,
        long_description=long_description,
        data=data,
    )


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
        self.refresh_metagraph_task = self.create_metagraph_refresh_task()

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

        # send machine specs to facilitator
        self.specs_task = asyncio.create_task(self.wait_for_specs())
        reconnects = 0
        try:
            async for ws in self.connect():
                try:
                    await self.handle_connection(ws)
                except websockets.ConnectionClosed as exc:
                    self.ws = None
                    logger.warning(
                        "validator connection closed with code %r and reason %r, reconnecting...",
                        exc.code,
                        exc.reason,
                    )
                except asyncio.exceptions.CancelledError:
                    self.ws = None
                    logger.warning("Facilitator client received cancel, stopping")
                except Exception as exc:
                    self.ws = None
                    logger.error(str(exc), exc_info=exc)
                reconnects += 1
                if reconnects > 5:
                    # stop facilitator connector after 5 reconnects
                    # allow restart policy to run it again, maybe fixing some broken async tasks
                    # this allow facilitator to cause restart by disconnecting 5 times
                    break

        except asyncio.exceptions.CancelledError:
            self.ws = None
            logger.error("Facilitator client received cancel, stopping")

    async def handle_connection(self, ws: websockets.WebSocketClientProtocol):
        """handle a single websocket connection"""
        await ws.send(AuthenticationRequest.from_keypair(self.keypair).model_dump_json())

        raw_msg = await ws.recv()
        try:
            response = Response.model_validate_json(raw_msg)
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
        specs_queue = deque()
        channel_layer = get_channel_layer()

        while True:
            validator_hotkey = settings.BITTENSOR_WALLET().hotkey.ss58_address
            try:
                msg = await asyncio.wait_for(
                    channel_layer.receive(MACHINE_SPEC_CHANNEL), timeout=20 * 60
                )

                specs = MachineSpecsUpdate(
                    specs=msg["specs"],
                    miner_hotkey=msg["miner_hotkey"],
                    batch_id=msg["batch_id"],
                    validator_hotkey=validator_hotkey,
                )
                logger.debug(f"sending machine specs update to facilitator: {specs}")

                specs_queue.append(specs)
                if self.ws is not None:
                    while specs_queue:
                        spec_to_send = specs_queue.popleft()
                        try:
                            await self.send_model(spec_to_send)
                        except Exception as exc:
                            specs_queue.appendleft(spec_to_send)
                            msg = f"Error occurred while sending specs: {exc}"
                            await save_facilitator_event(
                                subtype=SystemEvent.EventSubType.SPECS_SEND_ERROR,
                                long_description=msg,
                                data={
                                    "miner_hotkey": spec_to_send.miner_hotkey,
                                    "batch_id": spec_to_send.batch_id,
                                },
                            )
                            logger.warning(msg)
                            break
            except TimeoutError:
                logger.debug("wait_for_specs still running")

    async def heartbeat(self):
        while True:
            if self.ws is not None:
                try:
                    await self.send_model(Heartbeat())
                except Exception as exc:
                    msg = f"Error occurred while sending heartbeat: {exc}"
                    logger.warning(msg)
                    await save_facilitator_event(
                        subtype=SystemEvent.EventSubType.HEARTBEAT_ERROR,
                        long_description=msg,
                    )
            await asyncio.sleep(self.HEARTBEAT_PERIOD)

    def create_metagraph_refresh_task(self, period=None):
        return create_metagraph_refresh_task(period=period)

    @tenacity.retry(
        stop=tenacity.stop_after_attempt(7),
        wait=tenacity.wait_exponential(multiplier=1, exp_base=2, min=1, max=10),
        retry=tenacity.retry_if_exception_type(websockets.ConnectionClosed),
    )
    async def send_model(self, msg: BaseModel):
        if self.ws is None:
            raise websockets.ConnectionClosed(rcvd=None, sent=None)
        await self.ws.send(msg.model_dump_json())
        # Summary: https://github.com/python-websockets/websockets/issues/867
        # Longer discussion: https://github.com/python-websockets/websockets/issues/865
        await asyncio.sleep(0)

    async def handle_message(self, raw_msg: str | bytes):
        """handle message received from facilitator"""
        try:
            response = Response.model_validate_json(raw_msg)
        except pydantic.ValidationError:
            logger.debug("could not parse raw message as Response")
        else:
            if response.status != "success":
                logger.error("received error response from facilitator: %r", response)
            return

        try:
            job_request = pydantic.TypeAdapter(JobRequest).validate_json(raw_msg)
        except pydantic.ValidationError as exc:
            logger.debug("could not parse raw message as JobRequest: %s", exc)
        else:
            task = asyncio.create_task(self.miner_driver(job_request))
            await self.miner_drivers.put(task)
            return

        logger.error("unsupported message received from facilitator: %s", raw_msg)

    async def get_miner_axon_info(self, hotkey: str) -> bittensor.AxonInfo:
        return await get_miner_axon_info(hotkey)

    async def miner_driver(self, job_request: JobRequest):
        """drive a miner client from job start to completion, then close miner connection"""
        miner, _ = await Miner.objects.aget_or_create(hotkey=job_request.miner_hotkey)
        miner_axon_info = await self.get_miner_axon_info(job_request.miner_hotkey)
        job = await OrganicJob.objects.acreate(
            job_uuid=job_request.uuid,
            miner=miner,
            miner_address=miner_axon_info.ip,
            miner_address_ip_version=miner_axon_info.ip_type,
            miner_port=miner_axon_info.port,
            executor_class=job_request.executor_class,
            job_description="User job from facilitator",
        )

        miner_client = self.MINER_CLIENT_CLASS(
            miner_hotkey=job_request.miner_hotkey,
            miner_address=miner_axon_info.ip,
            miner_port=miner_axon_info.port,
            job_uuid=job_request.uuid,
            my_keypair=self.keypair,
        )
        await execute_organic_job(
            miner_client,
            job,
            job_request,
            total_job_timeout=TOTAL_JOB_TIMEOUT,
            wait_timeout=PREPARE_WAIT_TIMEOUT,
            notify_callback=self.send_model,
        )
