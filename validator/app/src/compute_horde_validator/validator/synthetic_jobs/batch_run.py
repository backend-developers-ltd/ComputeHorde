import asyncio
import logging
import random
import time
import uuid
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import Any

import bittensor
from asgiref.sync import async_to_sync, sync_to_async
from channels.layers import get_channel_layer
from compute_horde.base.volume import InlineVolume
from compute_horde.base_requests import BaseRequest
from compute_horde.executor_class import DEFAULT_EXECUTOR_CLASS, EXECUTOR_CLASS, ExecutorClass
from compute_horde.miner_client.base import (
    AbstractMinerClient,
    MinerConnectionError,
    UnsupportedMessageReceived,
)
from compute_horde.mv_protocol import miner_requests, validator_requests
from compute_horde.mv_protocol.miner_requests import (
    BaseMinerRequest,
    ExecutorManifest,
    GenericError,
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
    JobFinishedReceiptPayload,
    JobStartedReceiptPayload,
    V0AuthenticateRequest,
    V0InitialJobRequest,
    V0JobFinishedReceiptRequest,
    V0JobRequest,
    V0JobStartedReceiptRequest,
    VolumeType,
)
from django.conf import settings
from django.db import transaction
from pydantic import BaseModel

from compute_horde_validator.validator.models import (
    JobFinishedReceipt,
    JobStartedReceipt,
    Miner,
    MinerManifest,
    SyntheticJob,
    SyntheticJobBatch,
    SystemEvent,
)
from compute_horde_validator.validator.synthetic_jobs.generator import current
from compute_horde_validator.validator.synthetic_jobs.generator.base import (
    BaseSyntheticJobGenerator,
)
from compute_horde_validator.validator.utils import MACHINE_SPEC_GROUP_NAME

logger = logging.getLogger(__name__)

_CLOSE_TIMEOUT = 1
_SEND_RECEIPT_TIMEOUT = 5
_SEND_MACHINE_SPECS_TIMEOUT = 5

# extra time to wait for a job response, so we can record the
# responses of slow executors.
# it is not taken into account when scoring, jobs will still
# fail if they take too long, but we'll know how long they took
_JOB_RESPONSE_EXTRA_TIMEOUT = 5 * 60

# these two should match, the manifest timeout
# should be enough to fit max debounce count retries
_GET_MANIFEST_TIMEOUT = 35
_MAX_MINER_CLIENT_DEBOUNCE_COUNT = 4  # approximately 32 seconds


class MinerClient(AbstractMinerClient):
    def __init__(
        self,
        ctx: "BatchContext",
        miner_hotkey: str,
        miner_address: str,
        miner_port: int,
    ):
        miner_name = f"{miner_hotkey}({miner_address}:{miner_port})"
        super().__init__(miner_name)
        self.max_debounce_count = _MAX_MINER_CLIENT_DEBOUNCE_COUNT

        self.ctx = ctx
        self.own_hotkey = ctx.own_keypair.ss58_address
        self.own_keypair = ctx.own_keypair
        self.miner_hotkey = miner_hotkey
        self.miner_address = miner_address
        self.miner_port = miner_port

    def miner_url(self) -> str:
        return f"ws://{self.miner_address}:{self.miner_port}/v0.1/validator_interface/{self.own_hotkey}"

    def accepted_request_type(self) -> type[BaseRequest]:
        return BaseMinerRequest

    def incoming_generic_error_class(self) -> type[BaseRequest]:
        return miner_requests.GenericError

    def outgoing_generic_error_class(self) -> type[BaseRequest]:
        return validator_requests.GenericError

    async def handle_message(self, msg: BaseRequest) -> None:
        if isinstance(msg, GenericError):
            text = f"Received error message from miner {self.miner_name}: {msg.model_dump_json()}"
            logger.warning(text)
            is_unauthorized = msg.details is not None and msg.details.startswith(
                ("Unknown validator", "Inactive validator")
            )
            subtype = (
                SystemEvent.EventSubType.UNAUTHORIZED
                if is_unauthorized
                else SystemEvent.EventSubType.GENERIC_ERROR
            )
            self.ctx.system_event(
                type=SystemEvent.EventType.MINER_SYNTHETIC_JOB_FAILURE,
                subtype=subtype,
                description=text,
                miner_hotkey=self.miner_hotkey,
            )
            if is_unauthorized:
                # miner doesn't recognize our authority, close the connection to avoid retries
                logger.warning("Closing connection to miner %s", self.miner_name)
                await self.close()
            return

        if isinstance(msg, UnauthorizedError):
            logger.warning(
                "Unauthorized in %s: %s, details: %s", self.miner_name, msg.code, msg.details
            )
            self.ctx.system_event(
                type=SystemEvent.EventType.MINER_SYNTHETIC_JOB_FAILURE,
                subtype=SystemEvent.EventSubType.UNAUTHORIZED,
                description=str(msg),
                miner_hotkey=self.miner_hotkey,
            )

        if isinstance(msg, V0ExecutorManifestRequest):
            if self.ctx.manifests[self.miner_hotkey] is None:
                self.ctx.manifests[self.miner_hotkey] = msg.manifest
                self.ctx.manifest_events[self.miner_hotkey].set()
            else:
                logger.warning(
                    "Duplicate message %s received from miner %s", msg.message_type, self.miner_name
                )
            return

        job_uuid = getattr(msg, "job_uuid", None)
        if job_uuid is not None:
            job = self.ctx.jobs.get(job_uuid)
            if job is not None:
                job.handle_message(msg, self.miner_name)
            else:
                logger.info(
                    "Received info about unexpected job %s from %s", job_uuid, self.miner_name
                )
            return

        raise UnsupportedMessageReceived(msg)

    def generate_authentication_message(self) -> V0AuthenticateRequest:
        payload = AuthenticationPayload(
            validator_hotkey=self.own_hotkey,
            miner_hotkey=self.miner_hotkey,
            timestamp=int(time.time()),
        )
        return V0AuthenticateRequest(
            payload=payload,
            signature=f"0x{self.own_keypair.sign(payload.blob_for_signing()).hex()}",
        )

    async def _connect(self):
        ws = await super()._connect()
        await ws.send(self.generate_authentication_message().model_dump_json())
        return ws


@dataclass
class ExceptionInfo:
    exception: Exception
    miner_hotkey: str | None = None
    job_uuid: str | None = None
    stage: str | None = None


@dataclass
class Job:
    ctx: "BatchContext"

    uuid: str
    miner_hotkey: str
    executor_class: ExecutorClass
    job_generator: BaseSyntheticJobGenerator
    volume_contents: str

    # responses

    exception: Exception | None = None
    # not-exact, approximate time since it's after asyncio.gather returns
    exception_time: datetime | None = None
    exception_stage: str | None = None

    accept_barrier_time: datetime | None = None
    accept_before_sent_time: datetime | None = None
    accept_after_sent_time: datetime | None = None
    accept_response: V0AcceptJobRequest | V0DeclineJobRequest | None = None
    accept_response_time: datetime | None = None
    accept_response_event: asyncio.Event = field(default_factory=asyncio.Event)

    executor_response: V0ExecutorFailedRequest | V0ExecutorReadyRequest | None = None
    executor_response_time: datetime | None = None
    executor_response_event: asyncio.Event = field(default_factory=asyncio.Event)

    job_barrier_time: datetime | None = None
    job_before_sent_time: datetime | None = None
    job_after_sent_time: datetime | None = None
    job_response: V0JobFailedRequest | V0JobFinishedRequest | None = None
    job_response_time: datetime | None = None
    job_response_event: asyncio.Event = field(default_factory=asyncio.Event)

    machine_specs: V0MachineSpecsRequest | None = None

    # receipts
    job_started_receipt: V0JobStartedReceiptRequest | None = None
    job_finished_receipt: V0JobFinishedReceiptRequest | None = None

    # scoring
    time_took: timedelta | None = None
    success: bool = False
    comment: str = "FAILED"
    score: float = 0

    def handle_message(self, msg: BaseRequest, miner_name: str) -> None:
        duplicate = False
        match msg:
            case V0AcceptJobRequest() | V0DeclineJobRequest():
                if self.accept_response is None:
                    self.accept_response = msg
                    self.accept_response_time = datetime.now(tz=UTC)
                    self.accept_response_event.set()
                else:
                    duplicate = True

            case V0ExecutorReadyRequest() | V0ExecutorFailedRequest():
                if self.executor_response is None:
                    self.executor_response = msg
                    self.executor_response_time = datetime.now(tz=UTC)
                    self.executor_response_event.set()
                else:
                    duplicate = True

            case V0JobFinishedRequest() | V0JobFailedRequest():
                if self.job_response is None:
                    self.job_response = msg
                    self.job_response_time = datetime.now(tz=UTC)
                    self.job_response_event.set()
                else:
                    duplicate = True

            case V0MachineSpecsRequest():
                self.machine_specs = msg

        if duplicate:
            logger.warning(
                "Duplicate message %s received from miner %s for job %s",
                msg.message_type,
                miner_name,
                self.uuid,
            )

    def system_event(
        self,
        *,
        type: SystemEvent.EventType,
        subtype: SystemEvent.EventSubType,
        description: str,
        data: dict[str, str] | None = None,
    ) -> None:
        self.ctx.system_event(
            type=type,
            subtype=subtype,
            description=description,
            data=data,
            job_uuid=self.uuid,
            miner_hotkey=self.miner_hotkey,
        )

    def emit_telemetry_event(self) -> None:
        data = dict(
            batch_uuid=self.ctx.uuid,
            job_uuid=self.uuid,
            miner_hotkey=self.miner_hotkey,
            validator_hotkey=self.ctx.own_keypair.ss58_address,
            executor_class=self.executor_class.value,
            base_docker_image_name=self.job_generator.base_docker_image_name(),
            docker_image_name=self.job_generator.docker_image_name(),
            docker_run_options_preset=self.job_generator.docker_run_options_preset(),
            timeout_seconds=self.job_generator.timeout_seconds(),
            volume_contents_size=len(self.volume_contents),
            exception=repr(self.exception) if self.exception is not None else None,
            exception_time=_datetime_dump(self.exception_time),
            exception_stage=self.exception_stage,
            accept_barrier_time=_datetime_dump(self.accept_barrier_time),
            accept_before_sent_time=_datetime_dump(self.accept_before_sent_time),
            accept_after_sent_time=_datetime_dump(self.accept_after_sent_time),
            accept_response=_model_dump(self.accept_response),
            accept_response_time=_datetime_dump(self.accept_response_time),
            executor_response=_model_dump(self.executor_response),
            executor_response_time=_datetime_dump(self.executor_response_time),
            job_barrier_time=_datetime_dump(self.job_barrier_time),
            job_before_sent_time=_datetime_dump(self.job_before_sent_time),
            job_after_sent_time=_datetime_dump(self.job_after_sent_time),
            job_response=_model_dump(self.job_response),
            job_response_time=_datetime_dump(self.job_response_time),
            machine_specs=_model_dump(self.machine_specs),
            time_took=self.time_took.total_seconds() if self.time_took is not None else None,
            success=self.success,
            comment=self.comment,
            score=self.score,
        )
        self.ctx.system_event(
            type=SystemEvent.EventType.VALIDATOR_TELEMETRY,
            subtype=SystemEvent.EventSubType.SYNTHETIC_JOB,
            description="Validator synthetic job telemetry",
            data=data,
        )


@dataclass
class BatchContext:
    uuid: str
    own_keypair: bittensor.Keypair

    # randomized, but order preserving list of miner.hotkeys
    # used to go from indices returned by asyncio.gather() back to miner.hotkey
    hotkeys: list[str]

    # all dictionaries have miner.hotkey as key
    axons: dict[str, bittensor.AxonInfo]
    miners: dict[str, Miner]
    clients: dict[str, MinerClient]
    executors: dict[str, defaultdict[ExecutorClass, int]]
    job_generators: dict[str, dict[ExecutorClass, list[BaseSyntheticJobGenerator]]]
    online_executor_count: dict[str, int]
    previous_online_executor_count: dict[str, int | None]

    manifests: dict[str, ExecutorManifest | None]
    manifest_events: dict[str, asyncio.Event]

    # randomized, but order preserving list of job.uuid
    # used to go from indices returned by asyncio.gather() back to job.uuid
    job_uuids: list[str]

    # job.uuid as key
    jobs: dict[str, Job]

    # telemetry
    events: list[SystemEvent]
    stage_start_time: dict[str, datetime]

    def system_event(
        self,
        *,
        type: SystemEvent.EventType,
        subtype: SystemEvent.EventSubType,
        description: str,
        data: dict[str, Any] | None = None,
        job_uuid: str | None = None,
        miner_hotkey: str | None = None,
    ) -> None:
        if data is None:
            data = {}
        if job_uuid is not None:
            data["job_uuid"] = job_uuid
        if miner_hotkey is not None:
            data["miner_hotkey"] = miner_hotkey
        try:
            event = SystemEvent(
                type=type,
                subtype=subtype,
                long_description=description,
                data=data,
            )
            self.events.append(event)
        except Exception as exc:
            logger.error("Failed to add system event: %r", exc)

    def emit_telemetry_event(self) -> None:
        received_msg_count: dict[str, int] = defaultdict(int)
        for job in self.jobs.values():
            for msg in (
                job.accept_response,
                job.executor_response,
                job.job_response,
                job.machine_specs,
            ):
                if msg is not None:
                    received_msg_count[msg.message_type.value] += 1

        data = dict(
            batch_uuid=self.uuid,
            validator_hotkey=self.own_keypair.ss58_address,
            miner_count=len(self.miners),
            synthetic_job_count=len(self.jobs),
            system_event_count=len(self.events),
            received_msg_count=received_msg_count,
            stage_start_time={
                stage: _datetime_dump(dt) for stage, dt in self.stage_start_time.items()
            },
        )
        self.system_event(
            type=SystemEvent.EventType.VALIDATOR_TELEMETRY,
            subtype=SystemEvent.EventSubType.SYNTHETIC_BATCH,
            description="Validator synthetic jobs batch telemetry",
            data=data,
        )


def _datetime_dump(dt: datetime | None) -> str | None:
    if dt is None:
        return None
    return dt.isoformat()


def _model_dump(model: BaseModel | None) -> dict | None:
    if model is None:
        return None
    return model.model_dump(mode="json")


def _handle_exceptions(ctx: BatchContext, exceptions: list[ExceptionInfo]) -> None:
    for exc_info in exceptions:
        text = (
            f"Job failed "
            f"{exc_info.miner_hotkey} {exc_info.job_uuid} "
            f"{exc_info.stage}: {exc_info.exception!r}"
        )
        logger.warning(text)

        if isinstance(exc_info.exception, TimeoutError | asyncio.CancelledError):
            subtype = SystemEvent.EventSubType.JOB_EXECUTION_TIMEOUT
        else:
            subtype = SystemEvent.EventSubType.GENERIC_ERROR

        ctx.system_event(
            type=SystemEvent.EventType.MINER_SYNTHETIC_JOB_FAILURE,
            subtype=subtype,
            description=text,
            miner_hotkey=exc_info.miner_hotkey,
            job_uuid=exc_info.job_uuid,
        )


def _init_context(
    axons: dict[str, bittensor.AxonInfo], serving_miners: list[Miner]
) -> BatchContext:
    start_time = datetime.now(tz=UTC)

    own_wallet = settings.BITTENSOR_WALLET()
    own_keypair = own_wallet.get_hotkey()

    ctx = BatchContext(
        uuid=str(uuid.uuid4()),
        own_keypair=own_keypair,
        hotkeys=[],
        axons={},
        miners={},
        clients={},
        executors={},
        job_generators={},
        online_executor_count={},
        previous_online_executor_count={},
        manifests={},
        manifest_events={},
        job_uuids=[],
        jobs={},
        events=[],
        stage_start_time={"_init_context": start_time},
    )

    for miner in serving_miners:
        hotkey = miner.hotkey
        axon = axons[hotkey]
        ctx.hotkeys.append(hotkey)
        ctx.axons[hotkey] = axon
        ctx.miners[hotkey] = miner
        ctx.clients[hotkey] = MinerClient(
            ctx=ctx,
            miner_hotkey=hotkey,
            miner_address=axon.ip,
            miner_port=axon.port,
        )
        ctx.executors[hotkey] = defaultdict(int)
        ctx.job_generators[hotkey] = {}
        ctx.online_executor_count[hotkey] = 0
        ctx.previous_online_executor_count[hotkey] = None
        ctx.manifests[hotkey] = None
        ctx.manifest_events[hotkey] = asyncio.Event()

    return ctx


def _get_max_spin_up_time(ctx: BatchContext) -> int:
    max_spin_up_time = 0
    for executors in ctx.executors.values():
        for executor_class in executors.keys():
            spin_up_time = EXECUTOR_CLASS[executor_class].spin_up_time
            assert spin_up_time is not None
            max_spin_up_time = max(max_spin_up_time, spin_up_time)
    return max_spin_up_time


def _get_total_executor_count(ctx: BatchContext) -> int:
    total_executor_count = 0
    total_executor_class_count: dict[ExecutorClass, int] = defaultdict(int)
    for executors in ctx.executors.values():
        for executor_class, count in executors.items():
            total_executor_count += count
            total_executor_class_count[executor_class] += count
    for executor_class, count in total_executor_class_count.items():
        logger.info("%s has %d total executors", executor_class, count)
    return total_executor_count


def _generate_job_started_receipt(ctx: BatchContext, job: Job) -> None:
    assert job.job_started_receipt is None

    assert job.executor_response_time is not None

    max_timeout = job.job_generator.timeout_seconds()
    payload = JobStartedReceiptPayload(
        job_uuid=job.uuid,
        miner_hotkey=job.miner_hotkey,
        validator_hotkey=ctx.own_keypair.ss58_address,
        executor_class=ExecutorClass(job.executor_class),
        time_accepted=job.executor_response_time,
        max_timeout=max_timeout,
    )
    job.job_started_receipt = V0JobStartedReceiptRequest(
        payload=payload,
        signature=f"0x{ctx.own_keypair.sign(payload.blob_for_signing()).hex()}",
    )


def _generate_job_finished_receipt(ctx: BatchContext, job: Job) -> None:
    assert job.job_finished_receipt is None

    assert job.success
    assert job.time_took is not None
    assert job.job_before_sent_time is not None

    payload = JobFinishedReceiptPayload(
        job_uuid=job.uuid,
        miner_hotkey=job.miner_hotkey,
        validator_hotkey=ctx.own_keypair.ss58_address,
        time_started=job.job_before_sent_time,
        time_took_us=int(job.time_took.total_seconds() * 1_000_000),
        score_str=f"{job.score:.6f}",
    )
    job.job_finished_receipt = V0JobFinishedReceiptRequest(
        payload=payload,
        signature=f"0x{ctx.own_keypair.sign(payload.blob_for_signing()).hex()}",
    )


async def _get_miner_manifest(
    ctx: BatchContext, start_barrier: asyncio.Barrier, miner_hotkey: str
) -> None:
    await start_barrier.wait()

    client = ctx.clients[miner_hotkey]

    async with asyncio.timeout(_GET_MANIFEST_TIMEOUT):
        try:
            await client.await_connect()
        except MinerConnectionError as exc:
            text = f"Miner connection error: {exc!r}"
            logger.warning(text)
            ctx.system_event(
                type=SystemEvent.EventType.MINER_SYNTHETIC_JOB_FAILURE,
                subtype=SystemEvent.EventSubType.MINER_CONNECTION_ERROR,
                description=text,
                miner_hotkey=miner_hotkey,
            )
            return

        await ctx.manifest_events[miner_hotkey].wait()

    manifest = ctx.manifests[miner_hotkey]
    assert manifest is not None

    executors = ctx.executors[miner_hotkey]
    for executor_class_manifest in manifest.executor_classes:
        executor_class = executor_class_manifest.executor_class
        # convert deprecated executor class 0 to default executor class
        if isinstance(executor_class, int):
            assert executor_class == 0
            executor_class = DEFAULT_EXECUTOR_CLASS
        if executor_class_manifest.count > 0:
            executors[executor_class] += executor_class_manifest.count


async def _generate_jobs(ctx: BatchContext) -> None:
    start_time = time.time()
    generated_job_count = 0

    for hotkey, executors in ctx.executors.items():
        for executor_class, count in executors.items():
            job_generators = []
            for _ in range(count):
                job_generator = await current.synthetic_job_generator_factory.create(executor_class)
                await job_generator.ainit()
                job_uuid = str(job_generator.uuid())
                ctx.jobs[job_uuid] = Job(
                    ctx=ctx,
                    uuid=job_uuid,
                    miner_hotkey=hotkey,
                    executor_class=executor_class,
                    job_generator=job_generator,
                    volume_contents=await job_generator.volume_contents(),
                )
                ctx.job_uuids.append(job_uuid)
                job_generators.append(job_generator)
                generated_job_count += 1
            ctx.job_generators[hotkey][executor_class] = job_generators

    duration = time.time() - start_time
    logger.info("Generated %d jobs in %.2f seconds", generated_job_count, duration)


async def _send_initial_job_request(
    ctx: BatchContext, start_barrier: asyncio.Barrier, max_spin_up_time: int, job_uuid: str
) -> None:
    await start_barrier.wait()
    barrier_time = datetime.now(tz=UTC)

    job = ctx.jobs[job_uuid]
    job.accept_barrier_time = barrier_time
    client = ctx.clients[job.miner_hotkey]

    spin_up_time = EXECUTOR_CLASS[job.executor_class].spin_up_time
    assert spin_up_time is not None
    stagger_wait_interval = max_spin_up_time - spin_up_time
    assert stagger_wait_interval >= 0

    request = V0InitialJobRequest(
        job_uuid=job.uuid,
        executor_class=job.executor_class,
        base_docker_image_name=job.job_generator.base_docker_image_name(),
        timeout_seconds=job.job_generator.timeout_seconds(),
        volume_type=VolumeType.inline,
    )
    request_json = request.model_dump_json()

    async with asyncio.timeout(max_spin_up_time):
        if stagger_wait_interval > 0:
            await asyncio.sleep(stagger_wait_interval)

        # send can block, so take a timestamp
        # on both sides to detect long send times
        job.accept_before_sent_time = datetime.now(tz=UTC)
        await client.send(request_json)
        job.accept_after_sent_time = datetime.now(tz=UTC)

        await job.accept_response_event.wait()
        if isinstance(job.accept_response, V0AcceptJobRequest):
            await job.executor_response_event.wait()

    # send the receipt from outside the timeout
    if isinstance(job.executor_response, V0ExecutorReadyRequest):
        _generate_job_started_receipt(ctx, job)
        assert job.job_started_receipt is not None
        try:
            async with asyncio.timeout(_SEND_RECEIPT_TIMEOUT):
                await client.send_model(job.job_started_receipt)
        except Exception as exc:
            text = f"{job.miner_hotkey} job {job.uuid} failed to send job started receipt: {exc!r}"
            logger.warning(text)
            job.system_event(
                type=SystemEvent.EventType.RECEIPT_FAILURE,
                subtype=SystemEvent.EventSubType.RECEIPT_SEND_ERROR,
                description=text,
            )


async def _send_job_request(
    ctx: BatchContext, start_barrier: asyncio.Barrier, job_uuid: str
) -> None:
    await start_barrier.wait()
    barrier_time = datetime.now(tz=UTC)

    job = ctx.jobs[job_uuid]
    job.job_barrier_time = barrier_time
    client = ctx.clients[job.miner_hotkey]

    request = V0JobRequest(
        job_uuid=job.uuid,
        executor_class=job.executor_class,
        docker_image_name=job.job_generator.docker_image_name(),
        docker_run_options_preset=job.job_generator.docker_run_options_preset(),
        docker_run_cmd=job.job_generator.docker_run_cmd(),
        raw_script=job.job_generator.raw_script(),
        volume=InlineVolume(contents=job.volume_contents),
        output_upload=None,
    )
    request_json = request.model_dump_json()

    timeout = job.job_generator.timeout_seconds() + _JOB_RESPONSE_EXTRA_TIMEOUT
    async with asyncio.timeout(timeout):
        # send can block, so take a timestamp
        # on both sides to detect long send times
        job.job_before_sent_time = datetime.now(tz=UTC)
        await client.send(request_json)
        job.job_after_sent_time = datetime.now(tz=UTC)

        await job.job_response_event.wait()


async def _send_job_finished_receipts(ctx: BatchContext) -> None:
    for job in ctx.jobs.values():
        if job.success:
            client = ctx.clients[job.miner_hotkey]
            _generate_job_finished_receipt(ctx, job)
            assert job.job_finished_receipt is not None
            try:
                async with asyncio.timeout(_SEND_RECEIPT_TIMEOUT):
                    await client.send_model(job.job_finished_receipt)
            except Exception as exc:
                text = f"{job.miner_hotkey} job {job.uuid} failed to send job finished receipt: {exc!r}"
                logger.warning(text)
                job.system_event(
                    type=SystemEvent.EventType.RECEIPT_FAILURE,
                    subtype=SystemEvent.EventSubType.RECEIPT_SEND_ERROR,
                    description=text,
                )


def _emit_decline_or_failure_events(ctx: BatchContext) -> None:
    for job in ctx.jobs.values():
        if isinstance(job.accept_response, V0DeclineJobRequest) or isinstance(
            job.executor_response, V0ExecutorFailedRequest
        ):
            text = f"{job.miner_hotkey} job {job.uuid} refused job"
            logger.warning(text)
            job.system_event(
                type=SystemEvent.EventType.MINER_SYNTHETIC_JOB_FAILURE,
                subtype=SystemEvent.EventSubType.JOB_NOT_STARTED,
                description=text,
            )
        if isinstance(job.job_response, V0JobFailedRequest):
            text = f"{job.miner_hotkey} job {job.uuid} failed job"
            logger.warning(text)
            job.system_event(
                type=SystemEvent.EventType.MINER_SYNTHETIC_JOB_FAILURE,
                subtype=SystemEvent.EventSubType.FAILURE,
                description=text,
            )


def _emit_telemetry_events(ctx: BatchContext) -> None:
    ctx.emit_telemetry_event()
    for job in ctx.jobs.values():
        job.emit_telemetry_event()


async def _send_machine_specs(ctx: BatchContext) -> None:
    channel_layer = get_channel_layer()
    assert channel_layer is not None

    for job in ctx.jobs.values():
        # only take into account machine specs from executors which
        # finished the job successfully, to prevent fake executors
        # from pushing specs for non-existing GPUs
        if job.success and job.machine_specs is not None:
            try:
                async with asyncio.timeout(_SEND_MACHINE_SPECS_TIMEOUT):
                    await channel_layer.group_send(
                        MACHINE_SPEC_GROUP_NAME,
                        {
                            "type": "machine.specs",
                            "batch_id": ctx.uuid,
                            "miner_hotkey": job.miner_hotkey,
                            "specs": job.machine_specs.specs,
                        },
                    )
            except Exception as exc:
                text = f"{job.miner_hotkey} job {job.uuid} failed to send machine specs: {exc!r}"
                logger.warning(text)
                job.system_event(
                    type=SystemEvent.EventType.VALIDATOR_CHANNEL_LAYER_ERROR,
                    subtype=SystemEvent.EventSubType.SPECS_SEND_ERROR,
                    description=text,
                )


async def _close_clients(ctx: BatchContext) -> None:
    # should we create a task for each client so
    # we can close them in parallel? seems unnecessary
    for hotkey, client in ctx.clients.items():
        try:
            async with asyncio.timeout(_CLOSE_TIMEOUT):
                await client.close()
        except Exception as exc:
            logger.warning("%s failed to close client: %r", hotkey, exc)


async def _multi_get_miner_manifest(ctx: BatchContext) -> None:
    start_barrier = asyncio.Barrier(len(ctx.hotkeys))
    tasks = [
        asyncio.create_task(
            _get_miner_manifest(ctx, start_barrier, miner_hotkey),
            name=f"{miner_hotkey}-get_manifest",
        )
        for miner_hotkey in ctx.hotkeys
    ]

    results = await asyncio.gather(*tasks, return_exceptions=True)

    for i, result in enumerate(results):
        if isinstance(result, Exception):
            hotkey = ctx.hotkeys[i]
            text = f"Failed to get manifest {hotkey}: {result}"
            logger.warning(text)

            if isinstance(result, TimeoutError | asyncio.CancelledError):
                subtype = SystemEvent.EventSubType.MANIFEST_TIMEOUT
            else:
                subtype = SystemEvent.EventSubType.MANIFEST_ERROR

            ctx.system_event(
                type=SystemEvent.EventType.MINER_SYNTHETIC_JOB_FAILURE,
                subtype=subtype,
                description=text,
                miner_hotkey=hotkey,
            )
        else:
            assert result is None


async def _multi_send_initial_job_request(ctx: BatchContext) -> None:
    max_spin_up_time = _get_max_spin_up_time(ctx)
    logger.debug("Max spin-up time: %d seconds", max_spin_up_time)

    logger.info("Sending initial job requests for %d jobs", len(ctx.job_uuids))
    start_barrier = asyncio.Barrier(len(ctx.job_uuids))
    tasks = [
        asyncio.create_task(
            _send_initial_job_request(ctx, start_barrier, max_spin_up_time, job_uuid),
            name=f"{job_uuid}-send_initial_job",
        )
        for job_uuid in ctx.job_uuids
    ]

    results = await asyncio.gather(*tasks, return_exceptions=True)

    exceptions: list[ExceptionInfo] = []
    for i, result in enumerate(results):
        if isinstance(result, Exception):
            job_uuid = ctx.job_uuids[i]
            job = ctx.jobs[job_uuid]
            job.exception = result
            job.exception_time = datetime.now(tz=UTC)
            job.exception_stage = "_send_initial_job_request"
            exceptions.append(
                ExceptionInfo(
                    exception=job.exception,
                    miner_hotkey=job.miner_hotkey,
                    job_uuid=job.uuid,
                    stage=job.exception_stage,
                )
            )
        else:
            assert result is None
    _handle_exceptions(ctx, exceptions)


async def _multi_send_job_request(ctx: BatchContext) -> None:
    executor_ready_job_uuids = [
        job.uuid
        for job in ctx.jobs.values()
        if isinstance(job.executor_response, V0ExecutorReadyRequest)
    ]
    logger.info("Sending job requests for %d ready jobs", len(executor_ready_job_uuids))
    start_barrier = asyncio.Barrier(len(executor_ready_job_uuids))
    tasks = [
        asyncio.create_task(
            _send_job_request(ctx, start_barrier, job_uuid),
            name=f"{job_uuid}-send_job",
        )
        for job_uuid in executor_ready_job_uuids
    ]

    results = await asyncio.gather(*tasks, return_exceptions=True)

    exceptions: list[ExceptionInfo] = []
    for i, result in enumerate(results):
        if isinstance(result, Exception):
            job_uuid = executor_ready_job_uuids[i]
            job = ctx.jobs[job_uuid]
            job.exception = result
            job.exception_time = datetime.now(tz=UTC)
            job.exception_stage = "_send_job_request"
            exceptions.append(
                ExceptionInfo(
                    exception=job.exception,
                    miner_hotkey=job.miner_hotkey,
                    job_uuid=job.uuid,
                    stage=job.exception_stage,
                )
            )
        else:
            assert result is None
    _handle_exceptions(ctx, exceptions)


def _check_send_times(ctx: BatchContext) -> None:
    for job in ctx.jobs.values():
        if job.job_after_sent_time is None:
            continue
        assert job.job_before_sent_time is not None
        duration = job.job_after_sent_time - job.job_before_sent_time
        if duration.total_seconds() > 1:
            logger.warning(
                "%s long websocket msg send time: %s [%s to %s]",
                job.name,
                duration,
                job.job_before_sent_time,
                job.job_after_sent_time,
            )


async def _score_job(ctx: BatchContext, job: Job) -> None:
    # TODO move to top, workaround for circular import
    from compute_horde_validator.validator.synthetic_jobs.utils import apply_manifest_incentive

    job.score = 0
    job.success = False

    if job.job_response is None:
        job.comment = "timed out"
        logger.info("%s job %s %s", job.miner_hotkey, job.uuid, job.comment)
        return

    if isinstance(job.job_response, V0JobFailedRequest):
        job.comment = f"failed with return code {job.job_response.docker_process_exit_status}"
        logger.info("%s job %s %s", job.miner_hotkey, job.uuid, job.comment)
        return

    assert isinstance(job.job_response, V0JobFinishedRequest)
    assert job.job_before_sent_time is not None
    assert job.job_response_time is not None
    job.time_took = job.job_response_time - job.job_before_sent_time
    assert job.time_took.total_seconds() >= 0

    if job.time_took.total_seconds() > job.job_generator.timeout_seconds():
        job.comment = f"took too long: {job.time_took.total_seconds():.2f} seconds"
        logger.info("%s job %s %s", job.miner_hotkey, job.uuid, job.comment)
        job.system_event(
            type=SystemEvent.EventType.MINER_SYNTHETIC_JOB_FAILURE,
            subtype=SystemEvent.EventSubType.JOB_EXECUTION_TIMEOUT,
            description=job.comment,
        )
        return

    job.success, job.comment, job.score = job.job_generator.verify(
        job.job_response,
        job.time_took.total_seconds(),
    )

    if job.success:
        job.system_event(
            type=SystemEvent.EventType.MINER_SYNTHETIC_JOB_SUCCESS,
            subtype=SystemEvent.EventSubType.SUCCESS,
            description=job.comment,
        )
        job.score = await apply_manifest_incentive(
            job.score,
            ctx.previous_online_executor_count[job.miner_hotkey],
            ctx.online_executor_count[job.miner_hotkey],
        )
    else:
        job.system_event(
            type=SystemEvent.EventType.MINER_SYNTHETIC_JOB_FAILURE,
            subtype=SystemEvent.EventSubType.FAILURE,
            description=job.comment,
        )

    logger.info(
        "%s job %s finished with %s in %.2f seconds with score %f: %s",
        job.miner_hotkey,
        job.uuid,
        "success" if job.success else "failure",
        job.time_took.total_seconds(),
        job.score,
        job.comment,
    )


async def _score_jobs(ctx: BatchContext) -> None:
    for job in ctx.jobs.values():
        # count both successful and failed jobs from an executor
        if job.job_response is not None:
            ctx.online_executor_count[job.miner_hotkey] += 1

    for job in ctx.jobs.values():
        try:
            await _score_job(ctx, job)
        except Exception as exc:
            text = f"{job.miner_hotkey} job {job.uuid} failed to score: {exc!r}"
            logger.warning(text)
            job.system_event(
                type=SystemEvent.EventType.MINER_SYNTHETIC_JOB_FAILURE,
                subtype=SystemEvent.EventSubType.MINER_SCORING_ERROR,
                description=text,
            )


# sync_to_async is needed since we use the sync Django ORM
@sync_to_async
def _db_get_previous_online_executor_count(ctx: BatchContext) -> None:
    previous_batch = SyntheticJobBatch.objects.order_by("-id").first()
    if previous_batch is None:
        return

    for manifest in MinerManifest.objects.filter(batch_id=previous_batch.id):
        # only update if the miner is still serving
        if manifest.miner.hotkey in ctx.previous_online_executor_count:
            ctx.previous_online_executor_count[manifest.miner.hotkey] = (
                manifest.online_executor_count
            )


# sync_to_async is needed since we use the sync Django ORM
@sync_to_async
def _db_persist(ctx: BatchContext) -> None:
    start_time = time.time()

    # persist the system events first
    SystemEvent.objects.bulk_create(ctx.events)

    with transaction.atomic():
        # accepting_results_until is not used anywhere, it doesn't
        # matter that we pick a somewhat arbitrary time for it
        now = datetime.now(tz=UTC)
        batch = SyntheticJobBatch.objects.create(
            started_at=ctx.stage_start_time["_init_context"],
            accepting_results_until=ctx.stage_start_time.get("_multi_send_job_request", now),
        )

        synthetic_jobs: list[SyntheticJob] = []
        for job in ctx.jobs.values():
            axon = ctx.axons[job.miner_hotkey]
            miner = ctx.miners[job.miner_hotkey]
            status = SyntheticJob.Status.COMPLETED if job.success else SyntheticJob.Status.FAILED
            synthetic_job = SyntheticJob(
                job_uuid=job.uuid,
                batch=batch,
                miner=miner,
                miner_address=axon.ip,
                miner_address_ip_version=axon.ip_type,
                miner_port=axon.port,
                executor_class=job.executor_class,
                status=status,
                comment=job.comment,
                job_description=job.job_generator.job_description(),
                score=job.score,
            )
            synthetic_jobs.append(synthetic_job)
        SyntheticJob.objects.bulk_create(synthetic_jobs)

        miner_manifests: list[MinerManifest] = []
        for miner in ctx.miners.values():
            manifest = ctx.manifests[miner.hotkey]
            if manifest is not None:
                miner_manifests.append(
                    MinerManifest(
                        miner=miner,
                        batch=batch,
                        executor_count=manifest.total_count,
                        online_executor_count=ctx.online_executor_count[miner.hotkey],
                    )
                )
        MinerManifest.objects.bulk_create(miner_manifests)

    job_started_receipts: list[JobStartedReceipt] = []
    for job in ctx.jobs.values():
        if job.job_started_receipt is not None:
            payload = job.job_started_receipt.payload
            job_started_receipts.append(
                JobStartedReceipt(
                    job_uuid=payload.job_uuid,
                    miner_hotkey=payload.miner_hotkey,
                    validator_hotkey=payload.validator_hotkey,
                    executor_class=payload.executor_class,
                    time_accepted=payload.time_accepted,
                    max_timeout=payload.max_timeout,
                )
            )
    JobStartedReceipt.objects.bulk_create(job_started_receipts)

    job_finished_receipts: list[JobFinishedReceipt] = []
    for job in ctx.jobs.values():
        if job.job_finished_receipt is not None:
            payload = job.job_finished_receipt.payload
            job_finished_receipts.append(
                JobFinishedReceipt(
                    job_uuid=payload.job_uuid,
                    miner_hotkey=payload.miner_hotkey,
                    validator_hotkey=payload.validator_hotkey,
                    time_started=payload.time_started,
                    time_took_us=payload.time_took_us,
                    score_str=payload.score_str,
                )
            )
    JobFinishedReceipt.objects.bulk_create(job_finished_receipts)

    duration = time.time() - start_time
    logger.info("Persisted to database in %.2f seconds", duration)


@async_to_sync
async def execute_synthetic_batch_run(
    axons: dict[str, bittensor.AxonInfo], serving_miners: list[Miner]
) -> None:
    if not axons or not serving_miners:
        logger.warning("No miners provided")
        return
    logger.info("Executing synthetic jobs batch for %d miners", len(serving_miners))

    # randomize the order of miners each batch to avoid systemic bias
    random.shuffle(serving_miners)

    logger.info("STAGE: _init_context")
    ctx = _init_context(axons, serving_miners)

    try:
        logger.info("STAGE: _db_get_previous_online_executor_count")
        ctx.stage_start_time["_db_get_previous_online_executor_count"] = datetime.now(tz=UTC)
        await _db_get_previous_online_executor_count(ctx)

        logger.info("STAGE: _multi_get_miner_manifest")
        ctx.stage_start_time["_multi_get_miner_manifest"] = datetime.now(tz=UTC)
        await _multi_get_miner_manifest(ctx)

        logger.info("STAGE: _get_total_executor_count")
        ctx.stage_start_time["_get_total_executor_count"] = datetime.now(tz=UTC)
        total_executor_count = _get_total_executor_count(ctx)

        if total_executor_count != 0:
            logger.info("STAGE: _generate_jobs")
            ctx.stage_start_time["_generate_jobs"] = datetime.now(tz=UTC)
            await _generate_jobs(ctx)

            # randomize the order of jobs each batch to avoid systemic bias
            random.shuffle(ctx.job_uuids)

            logger.info("STAGE: _multi_send_initial_job_request")
            ctx.stage_start_time["_multi_send_initial_job_request"] = datetime.now(tz=UTC)
            await _multi_send_initial_job_request(ctx)

            if any(
                isinstance(job.accept_response, V0AcceptJobRequest) for job in ctx.jobs.values()
            ):
                logger.info("STAGE: _multi_send_job_request")
                ctx.stage_start_time["_multi_send_job_request"] = datetime.now(tz=UTC)
                await _multi_send_job_request(ctx)

                logger.info("STAGE: _check_send_times")
                ctx.stage_start_time["_check_send_times"] = datetime.now(tz=UTC)
                _check_send_times(ctx)

                logger.info("STAGE: _score_jobs")
                ctx.stage_start_time["_score_jobs"] = datetime.now(tz=UTC)
                await _score_jobs(ctx)

                logger.info("STAGE: _send_job_finished_receipts")
                ctx.stage_start_time["_send_job_finished_receipts"] = datetime.now(tz=UTC)
                await _send_job_finished_receipts(ctx)

            else:
                logger.warning("No jobs accepted")

            logger.info("STAGE: _emit_decline_or_failure_events")
            ctx.stage_start_time["_emit_decline_or_failure_events"] = datetime.now(tz=UTC)
            _emit_decline_or_failure_events(ctx)

        else:
            logger.warning("No executors available")

        logger.info("STAGE: _close_clients")
        ctx.stage_start_time["_close_clients"] = datetime.now(tz=UTC)
        await _close_clients(ctx)

        logger.info("STAGE: _send_machine_specs")
        ctx.stage_start_time["_send_machine_specs"] = datetime.now(tz=UTC)
        await _send_machine_specs(ctx)

    except Exception as exc:
        msg = f"Synthetic jobs batch failure: {exc!r}"
        logger.error(msg)
        ctx.system_event(
            type=SystemEvent.EventType.VALIDATOR_FAILURE,
            subtype=SystemEvent.EventSubType.GENERIC_ERROR,
            description=msg,
        )

    try:
        logger.info("STAGE: _emit_telemetry_events")
        ctx.stage_start_time["_emit_telemetry_events"] = datetime.now(tz=UTC)
        _emit_telemetry_events(ctx)
    except Exception as exc:
        msg = f"Synthetic jobs batch failure: {exc!r}"
        logger.error(msg)

    logger.info("STAGE: _db_persist")
    ctx.stage_start_time["_db_persist"] = datetime.now(tz=UTC)
    await _db_persist(ctx)

    logger.info("BATCH DONE")
