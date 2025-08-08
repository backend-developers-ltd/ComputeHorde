import asyncio
import logging
import os
from collections import deque
from typing import Any, Literal

import bittensor_wallet
import httpx
import pydantic
import tenacity
import websockets
from channels.layers import get_channel_layer
from compute_horde.fv_protocol.facilitator_requests import (
    Error,
    OrganicJobRequest,
    Response,
    V0JobCheated,
    V2JobRequest,
)
from compute_horde.fv_protocol.validator_requests import (
    HordeFailureDetails,
    JobFailureDetails,
    JobRejectionDetails,
    JobStatusMetadata,
    JobStatusUpdate,
    V0AuthenticationRequest,
    V0Heartbeat,
    V0MachineSpecsUpdate,
)
from compute_horde.protocol_consts import (
    HordeFailureReason,
    JobFailureReason,
    JobFailureStage,
    JobParticipantType,
    JobRejectionReason,
    JobStatus,
)
from compute_horde_core.signature import SignedRequest, verify_signature
from django.conf import settings
from pydantic import BaseModel, JsonValue

from compute_horde_validator.validator.dynamic_config import aget_config
from compute_horde_validator.validator.models import (
    MinerBlacklist,
    OrganicJob,
    SystemEvent,
    ValidatorWhitelist,
)
from compute_horde_validator.validator.organic_jobs import routing
from compute_horde_validator.validator.tasks import (
    execute_organic_job_request_on_worker,
    slash_collateral_task,
)
from compute_horde_validator.validator.utils import MACHINE_SPEC_CHANNEL

logger = logging.getLogger(__name__)


class JobRequestVerificationFailed(Exception):
    def __init__(self, message: str):
        self.message = message
        super().__init__(message, JobRejectionReason.INVALID_SIGNATURE)


async def verify_request_or_fail(job_request: SignedRequest) -> None:
    # check if signer is in validator whitelist
    if job_request.signature is None:
        raise JobRequestVerificationFailed("Signature is empty")

    signature = job_request.signature
    signer = signature.signatory
    signed_payload = job_request.get_signed_payload()

    my_keypair = settings.BITTENSOR_WALLET().get_hotkey()
    if signer != my_keypair.ss58_address:
        whitelisted = await ValidatorWhitelist.objects.filter(hotkey=signer).aexists()
        if not whitelisted:
            raise JobRequestVerificationFailed(f"Signatory {signer} is not in validator whitelist")

    # verify signed payload
    try:
        verify_signature(signed_payload, signature)
    except Exception as e:
        raise JobRequestVerificationFailed("Bad signature") from e


class AuthenticationError(Exception):
    def __init__(self, reason: str, errors: list[Error]) -> None:
        self.reason = reason
        self.errors = errors


async def save_facilitator_event(
    subtype: str, long_description: str, data: dict[str, str] | None = None
) -> None:
    await SystemEvent.objects.using(settings.DEFAULT_DB_ALIAS).acreate(
        type=SystemEvent.EventType.FACILITATOR_CLIENT_ERROR,
        subtype=subtype,
        long_description=long_description,
        data=data or {},
    )


class _JobStatusChannelEnvelope(BaseModel):
    type: Literal["job_status_update"] = "job_status_update"
    payload: JobStatusUpdate


class FacilitatorClient:
    HEARTBEAT_PERIOD = 60

    def __init__(self, keypair: bittensor_wallet.Keypair, facilitator_uri: str) -> None:
        self.keypair = keypair
        self.ws: websockets.ClientConnection | None = None
        self.facilitator_uri = facilitator_uri
        self.tasks_to_reap: asyncio.Queue[asyncio.Task[None] | None] = asyncio.Queue()
        # Sends periodic heartbeats
        self.heartbeat_task: asyncio.Task[None] | None = None
        # Sends machine specs to facilitator
        self.specs_task: asyncio.Task[None] | None = None
        # Disposes of tasks, brings up any exceptions
        self.reaper_task: asyncio.Task[None] | None = None

    def connect(self) -> websockets.connect:
        """Create an awaitable/async-iterable websockets.connect() object"""
        additional_headers = {
            "X-Validator-Runner-Version": os.environ.get("VALIDATOR_RUNNER_VERSION", "unknown"),
            "X-Validator-Version": os.environ.get("VALIDATOR_VERSION", "unknown"),
        }
        return websockets.connect(self.facilitator_uri, additional_headers=additional_headers)

    async def reap_tasks(self) -> None:
        """
        Avoid memory leak by awaiting job tasks
        Notify of job exceptions
        """
        while True:
            task = await self.tasks_to_reap.get()
            if task is None:
                return
            try:
                await task
            except Exception:
                logger.error("Error in job task", exc_info=True)

    async def __aenter__(self):
        self.heartbeat_task = asyncio.create_task(self.heartbeat())
        self.reaper_task = asyncio.create_task(self.reap_tasks())
        self.specs_task = asyncio.create_task(self.wait_for_specs())

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        tasks: list[asyncio.Task[Any]] = []

        for task in [
            self.reaper_task,
            self.heartbeat_task,
            self.specs_task,
        ]:
            if task is not None:
                task.cancel()
                tasks.append(task)

        await asyncio.gather(*tasks, return_exceptions=True)

    def my_hotkey(self) -> str:
        return str(self.keypair.ss58_address)

    async def run_forever(self) -> None:
        """connect (and re-connect) to facilitator and keep reading messages ... forever"""

        reconnects = 0
        try:
            async for ws in self.connect():
                try:
                    logger.info("connected to facilitator")
                    await self.handle_connection(ws)
                except websockets.ConnectionClosed as exc:
                    self.ws = None
                    logger.warning("Facilitator connection closed: %s, reconnecting...", exc)
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

    async def handle_connection(self, ws: websockets.ClientConnection) -> None:
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

        if settings.DEBUG_CONNECT_FACILITATOR_WEBHOOK:
            try:
                async with httpx.AsyncClient() as client:
                    await client.get(settings.DEBUG_CONNECT_FACILITATOR_WEBHOOK)
            except Exception:
                logger.info("when calling connect webhook:", exc_info=True)

        self.ws = ws

        async for raw_msg in ws:
            await self.handle_message(raw_msg)

    async def wait_for_specs(self) -> None:
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

    async def heartbeat(self) -> None:
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

    async def handle_job_status_updates(self, job_uuid: str):
        """
        Relay job status updates for given job back to the Facilitator.
        Loop until a terminal status is received.
        """
        logger.debug(f"Listening for job status updates for job {job_uuid}")
        try:
            while True:
                msg = await get_channel_layer().receive(f"job_status_updates__{job_uuid}")
                try:
                    envelope = _JobStatusChannelEnvelope.model_validate(msg)
                    logger.debug(
                        f"Received job status update for job {job_uuid}: status={envelope.payload.status}"
                    )
                    task = asyncio.create_task(self.send_job_status_update(envelope.payload))
                    await self.tasks_to_reap.put(task)
                    if not envelope.payload.status.is_in_progress():
                        return
                except pydantic.ValidationError as exc:
                    logger.warning("Received malformed job status update: %s", exc)
        finally:
            logger.debug(f"Finished listening for job status updates for job {job_uuid}")

    @tenacity.retry(
        stop=tenacity.stop_after_attempt(7),
        wait=tenacity.wait_exponential(multiplier=1, exp_base=2, min=1, max=10),
        retry=tenacity.retry_if_exception_type(websockets.ConnectionClosed),
    )
    async def send_model(self, msg: BaseModel) -> None:
        if self.ws is None:
            raise websockets.ConnectionClosed(rcvd=None, sent=None)
        await self.ws.send(msg.model_dump_json())
        # Summary: https://github.com/python-websockets/websockets/issues/867
        # Longer discussion: https://github.com/python-websockets/websockets/issues/865
        await asyncio.sleep(0)

    async def handle_message(self, raw_msg: str | bytes) -> None:
        """handle message received from facilitator"""
        try:
            response = Response.model_validate_json(raw_msg)
        except pydantic.ValidationError:
            pass
        else:
            if response.status != "success":
                logger.error("received error response from facilitator: %r", response)
            return

        try:
            job_request: OrganicJobRequest = pydantic.TypeAdapter(OrganicJobRequest).validate_json(
                raw_msg
            )
        except pydantic.ValidationError:
            pass
        else:
            task = asyncio.create_task(self.process_job_request(job_request))
            await self.tasks_to_reap.put(task)
            return

        try:
            cheated_job_report = pydantic.TypeAdapter(V0JobCheated).validate_json(raw_msg)
        except pydantic.ValidationError:
            pass
        else:
            await self.process_miner_cheat_report(cheated_job_report)
            return

        logger.error("unsupported or malformed message received from facilitator: %s", raw_msg)

    async def process_miner_cheat_report(self, cheated_job_request: V0JobCheated) -> None:
        try:
            await verify_request_or_fail(cheated_job_request)
        except Exception as e:
            logger.warning(f"Failed to verify signed payload: {e} - will ignore")
            return
        job_uuid = cheated_job_request.job_uuid
        try:
            job = await OrganicJob.objects.prefetch_related("miner").aget(job_uuid=job_uuid)
        except OrganicJob.DoesNotExist:
            logger.error(f"Job {job_uuid} reported for cheating does not exist")
            return

        if job.cheated:
            logger.warning(f"Job {job_uuid} already marked as cheated - ignoring")
            return

        if job.status != OrganicJob.Status.COMPLETED:
            logger.info(f"Job {job_uuid} reported for cheating is not complete yet")
            return

        job.cheated = True
        await job.asave()

        blacklist_time = await aget_config("DYNAMIC_JOB_CHEATED_BLACKLIST_TIME_SECONDS")
        await routing.blacklist_miner(
            job, MinerBlacklist.BlacklistReason.JOB_CHEATED, blacklist_time
        )
        await SystemEvent.objects.using(settings.DEFAULT_DB_ALIAS).acreate(
            type=SystemEvent.EventType.MINER_ORGANIC_JOB_FAILURE,
            subtype=SystemEvent.EventSubType.JOB_CHEATED,
            long_description="Job was reported as cheated",
            data={
                "job_uuid": str(job.job_uuid),
                "miner_hotkey": job.miner.hotkey,
            },
        )

        if not job.slashed:
            slash_collateral_task.delay(str(job.job_uuid))

    async def process_job_request(self, job_request: OrganicJobRequest) -> None:
        try:
            await self._process_job_request(job_request)
        except JobRequestVerificationFailed as e:
            await self.send_job_rejected(
                job_uuid=job_request.uuid,
                message=e.message,
                rejected_by=JobParticipantType.VALIDATOR,
                reason=JobRejectionReason.INVALID_SIGNATURE,
            )
        except routing.JobRoutingException as e:
            await self.send_job_rejected(
                job_uuid=job_request.uuid,
                message="Job could not be routed to a miner",
                rejected_by=JobParticipantType.VALIDATOR,
                reason=JobRejectionReason.UNKNOWN,
                context={"exception_type": type(e).__qualname__},
            )
        except Exception as e:
            await self.send_horde_failed(
                job_uuid=job_request.uuid,
                message="Uncaught exception during handling of job",
                reported_by=JobParticipantType.VALIDATOR,
                reason=HordeFailureReason.UNCAUGHT_EXCEPTION,
                context={"exception_type": type(e).__qualname__},
            )

    async def _process_job_request(self, job_request: OrganicJobRequest) -> None:
        if isinstance(job_request, V2JobRequest):
            logger.debug(f"Received signed job request: {job_request}")
            await verify_request_or_fail(job_request)

        await self.send_job_status_update(
            JobStatusUpdate(
                uuid=job_request.uuid,
                status=JobStatus.RECEIVED,
            )
        )

        miner = await routing.pick_miner_for_job_request(job_request)
        logger.info(f"Selected miner {miner.hotkey} for job {job_request.uuid}")

        logger.info(f"Submitting job {job_request.uuid} to worker")
        job_status_task = asyncio.create_task(self.handle_job_status_updates(job_request.uuid))
        await self.tasks_to_reap.put(job_status_task)
        job = await execute_organic_job_request_on_worker(job_request, miner)
        logger.info(
            f"Job {job_request.uuid} finished with status: {job.status} (comment={job.comment})"
        )

        if job.status == OrganicJob.Status.FAILED:
            await routing.report_miner_failed_job(job)

    async def send_job_status_update(self, status_update: JobStatusUpdate) -> None:
        await self.send_model(status_update)

    async def send_job_rejected(
        self,
        job_uuid: str,
        message: str,
        rejected_by: JobParticipantType,
        reason: JobRejectionReason,
        context: dict[str, JsonValue] | None = None,
    ) -> None:
        await self.send_job_status_update(
            JobStatusUpdate(
                uuid=job_uuid,
                status=JobStatus.REJECTED,
                metadata=JobStatusMetadata(
                    comment=message,
                    job_rejection_details=JobRejectionDetails(
                        rejected_by=rejected_by,
                        reason=reason,
                        message=message,
                        context=context,
                    ),
                ),
            )
        )

    async def send_job_failed(
        self,
        job_uuid: str,
        message: str,
        stage: JobFailureStage,
        reason: JobFailureReason,
        context: dict[str, JsonValue] | None = None,
    ) -> None:
        await self.send_job_status_update(
            JobStatusUpdate(
                uuid=job_uuid,
                status=JobStatus.FAILED,
                metadata=JobStatusMetadata(
                    comment=message,
                    job_failure_details=JobFailureDetails(
                        reason=reason,
                        stage=stage,
                        message=message,
                        context=context,
                    ),
                ),
            )
        )

    async def send_horde_failed(
        self,
        job_uuid: str,
        message: str,
        reported_by: JobParticipantType,
        reason: HordeFailureReason,
        context: dict[str, JsonValue] | None = None,
    ) -> None:
        await self.send_job_status_update(
            JobStatusUpdate(
                uuid=job_uuid,
                status=JobStatus.HORDE_FAILED,
                metadata=JobStatusMetadata(
                    comment=message,
                    horde_failure_details=HordeFailureDetails(
                        reported_by=reported_by,
                        reason=reason,
                        message=message,
                        context=context,
                    ),
                ),
            )
        )
