import asyncio
import logging
import os
import random
from collections import deque
from datetime import timedelta

import bittensor
import pydantic
import tenacity
import websockets
from channels.layers import get_channel_layer
from compute_horde.executor_class import ExecutorClass
from compute_horde.fv_protocol.facilitator_requests import Error, JobRequest, Response, V2JobRequest
from compute_horde.fv_protocol.validator_requests import (
    V0AuthenticationRequest,
    V0Heartbeat,
    V0MachineSpecsUpdate,
)
from compute_horde.receipts.models import JobStartedReceipt
from compute_horde.signature import verify_signature
from django.conf import settings
from django.db.models import (
    Count,
    DateTimeField,
    ExpressionWrapper,
    F,
    IntegerField,
    OuterRef,
    Subquery,
)
from django.utils import timezone
from pydantic import BaseModel

from compute_horde_validator.validator.dynamic_config import aget_config
from compute_horde_validator.validator.metagraph_client import (
    create_metagraph_refresh_task,
    get_miner_axon_info,
)
from compute_horde_validator.validator.models import (
    Miner,
    MinerManifest,
    OrganicJob,
    SystemEvent,
    ValidatorWhitelist,
)
from compute_horde_validator.validator.organic_jobs.miner_client import MinerClient
from compute_horde_validator.validator.organic_jobs.miner_driver import execute_organic_job
from compute_horde_validator.validator.tasks import get_subtensor
from compute_horde_validator.validator.utils import MACHINE_SPEC_CHANNEL

logger = logging.getLogger(__name__)


async def verify_job_request(job_request: V2JobRequest):
    # check if signer is in validator whitelist
    if job_request.signature is None:
        raise ValueError("Signature is None")

    signature = job_request.signature
    signer = signature.signatory
    signed_fields = job_request.get_signed_fields()

    my_keypair = settings.BITTENSOR_WALLET().get_hotkey()
    if signer != my_keypair.ss58_address:
        whitelisted = await ValidatorWhitelist.objects.filter(hotkey=signer).aexists()
        if not whitelisted:
            raise ValueError(f"Signatory {signer} is not in validator whitelist")

    # verify signed payload
    verify_signature(signed_fields.model_dump_json(), signature)


class AuthenticationError(Exception):
    def __init__(self, reason: str, errors: list[Error]):
        self.reason = reason
        self.errors = errors


async def save_facilitator_event(
    subtype: str, long_description: str, data: dict[str, str] | None = None
):
    await SystemEvent.objects.using(settings.DEFAULT_DB_ALIAS).acreate(
        type=SystemEvent.EventType.FACILITATOR_CLIENT_ERROR,
        subtype=subtype,
        long_description=long_description,
        data=data or {},
    )


class FacilitatorClient:
    MINER_CLIENT_CLASS = MinerClient
    HEARTBEAT_PERIOD = 60

    def __init__(self, keypair: bittensor.Keypair, facilitator_uri: str):
        self.keypair = keypair
        self.ws: websockets.ClientConnection | None = None
        self.facilitator_uri = facilitator_uri
        self.miner_drivers: asyncio.Queue[asyncio.Task[None] | None] = asyncio.Queue()
        self.miner_driver_awaiter_task = asyncio.create_task(self.miner_driver_awaiter())
        self.heartbeat_task = asyncio.create_task(self.heartbeat())
        self.refresh_metagraph_task = self.create_metagraph_refresh_task()
        self.specs_task: asyncio.Task[None] | None = None

    def connect(self) -> websockets.connect:
        """Create an awaitable/async-iterable websockets.connect() object"""
        additional_headers = {
            "X-Validator-Runner-Version": os.environ.get("VALIDATOR_RUNNER_VERSION", "unknown"),
            "X-Validator-Version": os.environ.get("VALIDATOR_VERSION", "unknown"),
        }
        return websockets.connect(self.facilitator_uri, additional_headers=additional_headers)

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
        return str(self.keypair.ss58_address)

    async def run_forever(self):
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

    async def handle_connection(self, ws: websockets.ClientConnection):
        """handle a single websocket connection"""
        await ws.send(V0AuthenticationRequest.from_keypair(self.keypair).model_dump_json())

        raw_msg = await ws.recv()
        try:
            response = Response.model_validate_json(raw_msg)
        except pydantic.ValidationError as exc:
            raise AuthenticationError(
                "did not receive Response for V0AuthenticationRequest", []
            ) from exc
        if response.status != "success":
            raise AuthenticationError("auth request received failed response", response.errors)

        self.ws = ws

        async for raw_msg in ws:
            await self.handle_message(raw_msg)

    async def wait_for_specs(self):
        specs_queue: deque[V0MachineSpecsUpdate] = deque()
        channel_layer = get_channel_layer()

        while True:
            validator_hotkey = settings.BITTENSOR_WALLET().hotkey.ss58_address
            try:
                msg = await asyncio.wait_for(
                    channel_layer.receive(MACHINE_SPEC_CHANNEL), timeout=20 * 60
                )

                specs = V0MachineSpecsUpdate(
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
                    await self.send_model(V0Heartbeat())
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
            job_request: JobRequest = pydantic.TypeAdapter(JobRequest).validate_json(raw_msg)
        except pydantic.ValidationError as exc:
            logger.debug("could not parse raw message as JobRequest: %s", exc)
        else:
            task = asyncio.create_task(self.process_job_request(job_request))
            await self.miner_drivers.put(task)
            return

        logger.error("unsupported message received from facilitator: %s", raw_msg)

    async def get_miner_axon_info(self, hotkey: str) -> bittensor.AxonInfo:
        return await get_miner_axon_info(hotkey)

    async def pick_miner_for_job(self, executor_class: ExecutorClass) -> Miner | None:
        """
        Goes through all miners with recent manifests and online executors of the given executor class
        And returns a random miner that seems to have a non-busy executor.
        """

        now = timezone.now()

        jobs_count_subq = JobStartedReceipt.objects.annotate(
            valid_until=ExpressionWrapper(
                F("timestamp") + F("ttl") * timedelta(seconds=1),
                output_field=DateTimeField()
            ),
        ).filter(
            executor_class=executor_class,
            timestamp__lte=now,
            valid_until__gte=now,
            miner_hotkey=OuterRef("miner__hotkey"),
        ).values(
            "miner_hotkey"
        ).annotate(
            count=Count("id")
        ).values("count")

        relevant_manifests_q = MinerManifest.objects.select_related("miner").annotate(
            ongoing_jobs=Subquery(jobs_count_subq, output_field=IntegerField()),
        ).filter(
            executor_class=str(executor_class),
            online_executor_count__gt=F("ongoing_jobs"),
            created_at__gte=timezone.now() - timedelta(hours=4),
        )

        available_manifests = [m async for m in relevant_manifests_q.all()]

        if not available_manifests:
            return None

        selected = random.choice(available_manifests)
        return selected.miner

    async def process_job_request(self, job_request: JobRequest):
        max_retries = await aget_config("DYNAMIC_ORGANIC_JOB_MAX_RETRIES")

        if isinstance(job_request, V2JobRequest):
            logger.debug(f"Received signed job request: {job_request}")
            try:
                await verify_job_request(job_request)
            except Exception as e:
                logger.warning(f"Failed to verify signed payload: {e} - will not run job")
                return

            for i in range(max_retries):
                miner = await self.pick_miner_for_job(job_request.executor_class)
                if miner is None:
                    logger.warning(
                        f"No available miners with executor class: {job_request.executor_class} - will not run job"
                    )
                    return

                try:
                    # if exists, delete organic job from the previous attempt
                    await OrganicJob.objects.filter(job_uuid=str(job_request.uuid)).adelete()
                    job = await self.miner_driver(miner, job_request)
                    if job.status == OrganicJob.Status.COMPLETED:
                        break
                except Exception as e:
                    logger.warning(
                        f"Error running organic job {job_request.uuid}: {e} - {max_retries-i-1} retries left"
                    )
        else:
            # normal organic job flow
            miner, _ = await Miner.objects.aget_or_create(hotkey=job_request.miner_hotkey)
            await self.miner_driver(miner, job_request)

    async def miner_driver(self, miner: Miner, job_request: JobRequest) -> OrganicJob:
        """drive a miner client from job start to completion, then close miner connection"""
        subtensor_ = get_subtensor(network=settings.BITTENSOR_NETWORK)
        current_block = subtensor_.get_current_block()

        if miner.hotkey == settings.DEBUG_MINER_KEY:
            miner_ip = settings.DEBUG_MINER_ADDRESS
            miner_port = settings.DEBUG_MINER_PORT
            ip_type = 4
        else:
            miner_axon_info = await self.get_miner_axon_info(miner.hotkey)
            miner_ip = miner_axon_info.ip
            miner_port = miner_axon_info.port
            ip_type = miner_axon_info.ip_type

        job = await OrganicJob.objects.acreate(
            job_uuid=str(job_request.uuid),
            miner=miner,
            miner_address=miner_ip,
            miner_address_ip_version=ip_type,
            miner_port=miner_port,
            executor_class=job_request.executor_class,
            job_description="User job from facilitator",
            block=current_block,
        )

        miner_client = self.MINER_CLIENT_CLASS(
            miner_hotkey=miner.hotkey,
            miner_address=job.miner_address,
            miner_port=job.miner_port,
            job_uuid=str(job.job_uuid),
            my_keypair=self.keypair,
        )

        total_job_timeout = await aget_config("DYNAMIC_ORGANIC_JOB_TIMEOUT")
        wait_timeout = await aget_config("DYNAMIC_ORGANIC_JOB_WAIT_TIMEOUT")
        await execute_organic_job(
            miner_client,
            job,
            job_request,
            total_job_timeout=total_job_timeout,
            wait_timeout=wait_timeout,
            notify_callback=self.send_model,
        )

        return job
