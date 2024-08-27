import asyncio
import logging
import random
import statistics
import time
import uuid
from collections import defaultdict
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import Any

import bittensor
from asgiref.sync import sync_to_async
from channels.layers import get_channel_layer
from compute_horde.base.volume import InlineVolume
from compute_horde.base_requests import BaseRequest
from compute_horde.executor_class import DEFAULT_EXECUTOR_CLASS, EXECUTOR_CLASS, ExecutorClass
from compute_horde.miner_client.base import (
    AbstractMinerClient,
    TransportConnectionError,
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
from compute_horde.transport import AbstractTransport, WSTransport
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
from compute_horde_validator.validator.synthetic_jobs.scoring import get_manifest_multiplier
from compute_horde_validator.validator.utils import MACHINE_SPEC_GROUP_NAME

logger = logging.getLogger(__name__)

_GIVE_AVERAGE_JOB_SEND_TIME_BONUS = False

# always-on executor classes have spin_up_time=0, but realistically
# we need a bit more for all the back-and-forth messaging, especially
# when we talk with a lot of executors
_MIN_SPIN_UP_TIME = 2

_CLOSE_TIMEOUT = 1
_SEND_RECEIPT_TIMEOUT = 5
_SEND_MACHINE_SPECS_TIMEOUT = 5

# extra time to wait for a job response, so we can record the
# responses of slow executors.
# it is not taken into account when scoring, jobs will still
# fail if they take too long, but we'll know how long they took
# when debugging failed jobs
_JOB_RESPONSE_EXTRA_TIMEOUT = 2 * 60

# these two should match, the manifest timeout
# should be enough to fit max debounce count retries
_GET_MANIFEST_TIMEOUT = 35
_MAX_MINER_CLIENT_DEBOUNCE_COUNT = 4  # approximately 32 seconds

# Celery job timeouts
SYNTHETIC_JOBS_SOFT_LIMIT = 20 * 60
SYNTHETIC_JOBS_HARD_LIMIT = SYNTHETIC_JOBS_SOFT_LIMIT + 10


class MinerClient(AbstractMinerClient):
    def __init__(
        self,
        ctx: "BatchContext",
        miner_hotkey: str,
        transport: AbstractTransport | None = None,
    ):
        self.ctx = ctx
        self.own_hotkey = ctx.own_keypair.ss58_address
        self.own_keypair = ctx.own_keypair

        axon = ctx.axons[miner_hotkey]
        self.miner_hotkey = miner_hotkey
        self.miner_address = axon.ip
        self.miner_port = axon.port

        name = ctx.names[miner_hotkey]
        transport = transport or WSTransport(
            name, self.miner_url(), max_retries=_MAX_MINER_CLIENT_DEBOUNCE_COUNT
        )
        super().__init__(name, transport)

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
            logger.warning("%s error: %s", self.miner_name, msg.model_dump_json())
            is_unauthorized = msg.details is not None and msg.details.lower().startswith(
                ("unknown validator", "inactive validator")
            )
            subtype = (
                SystemEvent.EventSubType.UNAUTHORIZED
                if is_unauthorized
                else SystemEvent.EventSubType.GENERIC_ERROR
            )
            self.ctx.system_event(
                type=SystemEvent.EventType.MINER_SYNTHETIC_JOB_FAILURE,
                subtype=subtype,
                description=str(msg),
                miner_hotkey=self.miner_hotkey,
            )
            if is_unauthorized:
                # miner doesn't recognize our authority, close the connection to avoid retries
                logger.warning("%s closing connection", self.miner_name)
                await self.close()
            return

        if isinstance(msg, UnauthorizedError):
            logger.warning("%s unauthorized: %s %s", self.miner_name, msg.code, msg.details)
            self.ctx.system_event(
                type=SystemEvent.EventType.MINER_SYNTHETIC_JOB_FAILURE,
                subtype=SystemEvent.EventSubType.UNAUTHORIZED,
                description=str(msg),
                miner_hotkey=self.miner_hotkey,
            )
            return

        if isinstance(msg, V0ExecutorManifestRequest):
            if self.ctx.manifests[self.miner_hotkey] is None:
                self.ctx.manifests[self.miner_hotkey] = msg.manifest
                self.ctx.manifest_events[self.miner_hotkey].set()
            else:
                logger.warning("%s duplicate message: %s", self.miner_name, msg.message_type)
            return

        job_uuid = getattr(msg, "job_uuid", None)
        if job_uuid is not None:
            job = self.ctx.jobs.get(job_uuid)
            if job is not None:
                job.handle_message(msg)
            else:
                logger.info("%s unexpected/old job: %s", self.miner_name, job_uuid)
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

    async def connect(self) -> None:
        await super().connect()
        await self.transport.send(self.generate_authentication_message().model_dump_json())

    async def send_check(self, data: str | bytes) -> None:
        await self.send(data, error_event_callback=self._handle_send_error_event)

    async def _handle_send_error_event(self, msg: str):
        self.ctx.system_event(
            type=SystemEvent.EventType.MINER_SYNTHETIC_JOB_FAILURE,
            subtype=SystemEvent.EventSubType.MINER_SEND_ERROR,
            description=str(msg),
            miner_hotkey=self.miner_hotkey,
            func="send_check",
        )


@dataclass
class ExceptionInfo:
    exception: BaseException
    miner_hotkey: str
    job_uuid: str
    stage: str


@dataclass
class Job:
    ctx: "BatchContext"

    uuid: str
    # full name for easier debugging: "{miner_hotkey}({ip}:{port}) job {uuid}"
    name: str
    miner_hotkey: str
    executor_class: ExecutorClass
    job_generator: BaseSyntheticJobGenerator
    volume_contents: str

    # responses

    exception: BaseException | None = None
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

    # !!! time_took can be negative since it's just
    #     `job_response_time - job_before_sent_time`
    #     and miner could have already sent a failed
    #     response before we sent the job details
    time_took: timedelta | None = None
    correct: bool | None = None  # returned correct answer (even if outside time limit)
    success: bool = False  # returned correct answer within time limit
    comment: str = "failed"
    score: float = 0
    # dancing bonus
    score_manifest_multiplier: float | None = None
    average_job_send_time_bonus: timedelta | None = None

    def handle_message(self, msg: BaseRequest) -> None:
        # !!! it is very important to not allow a newer message of a
        #     certain kind to override a previously received message
        #     of the same kind. miners could play games with that.
        #     please don't change without discussion
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

            # we don't care if we receive multiple specs messages,
            # doesn't matter which one we keep, miner controls it
            case V0MachineSpecsRequest():
                self.machine_specs = msg

        if duplicate:
            logger.warning("%s duplicate message: %s", self.name, msg.message_type)

    def system_event(
        self,
        *,
        type: SystemEvent.EventType,
        subtype: SystemEvent.EventSubType,
        description: str,
        func: str | None = None,
        data: dict[str, str] | None = None,
    ) -> SystemEvent | None:
        return self.ctx.system_event(
            type=type,
            subtype=subtype,
            description=description,
            data=data,
            job_uuid=self.uuid,
            miner_hotkey=self.miner_hotkey,
            func=func,
        )

    def emit_telemetry_event(self) -> SystemEvent | None:
        data = dict(
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
            time_took=_timedelta_dump(self.time_took),
            success=self.success,
            correct=self.correct,
            comment=self.comment,
            score=self.score,
            score_manifest_multiplier=self.score_manifest_multiplier,
            average_job_send_time_bonus=_timedelta_dump(self.average_job_send_time_bonus),
        )
        return self.ctx.system_event(
            type=SystemEvent.EventType.VALIDATOR_TELEMETRY,
            subtype=SystemEvent.EventSubType.SYNTHETIC_JOB,
            description="job telemetry",
            data=data,
        )


@dataclass
class BatchContext:
    # an already existing SyntheticJobBatch model can be optionally passed in
    batch_id: int | None

    uuid: str
    own_keypair: bittensor.Keypair

    # randomized, but order preserving list of miner.hotkeys
    # used to go from indices returned by asyncio.gather() back to miner.hotkey
    hotkeys: list[str]

    # all dictionaries have miner.hotkey as key
    axons: dict[str, bittensor.AxonInfo]
    # full name for easier debugging: "{miner_hotkey}({ip}:{port})"
    names: dict[str, str]
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

    # system events, periodically flushed to database, which is why
    # we need a separate event_count field to track how many we
    # created during a batch run
    events: list[SystemEvent]
    event_count: int

    stage_start_time: dict[str, datetime]
    average_job_send_time: timedelta | None = None

    # for tests
    _loop: asyncio.AbstractEventLoop | None = None

    def system_event(
        self,
        *,
        type: SystemEvent.EventType,
        subtype: SystemEvent.EventSubType,
        description: str,
        data: dict[str, Any] | None = None,
        job_uuid: str | None = None,
        miner_hotkey: str | None = None,
        func: str | None = None,
    ) -> SystemEvent | None:
        if data is None:
            data = {}

        assert "batch_uuid" not in data
        data["batch_uuid"] = self.uuid

        if job_uuid is not None:
            assert "job_uuid" not in data
            data["job_uuid"] = job_uuid

        if miner_hotkey is not None:
            assert "miner_hotkey" not in data
            data["miner_hotkey"] = miner_hotkey

        if func is not None:
            assert "func" not in data
            data["func"] = func

        try:
            event = SystemEvent(
                type=type,
                subtype=subtype,
                long_description=description,
                data=data,
            )
            self.events.append(event)
            self.event_count += 1
            return event
        except Exception as exc:
            logger.error("Failed to emit system event: %r", exc)
            return None

    def emit_telemetry_event(self) -> SystemEvent | None:
        messages_count: dict[str, int] = defaultdict(int)
        for job in self.jobs.values():
            for msg in (
                job.accept_response,
                job.executor_response,
                job.job_response,
                job.machine_specs,
            ):
                if msg is not None:
                    messages_count[msg.message_type.value] += 1
        # convert to regular dict for nice logging
        messages_count = dict(messages_count)

        job_count = dict(
            total=len(self.jobs),
            failed=sum(1 for job in self.jobs.values() if not job.success),
            successful=sum(1 for job in self.jobs.values() if job.success),
            correct=sum(1 for job in self.jobs.values() if job.correct),
            # don't count None as incorrect
            incorrect=sum(1 for job in self.jobs.values() if job.correct is False),
        )

        counts = dict(
            miners=len(self.miners),
            manifests=sum(1 for manifest in self.manifests.values() if manifest is not None),
            messages=messages_count,
            jobs=job_count,
            system_events=self.event_count,
        )

        manifests = {}
        for miner_hotkey, manifest in self.manifests.items():
            if manifest is not None:
                executors = {
                    executor_class_manifest.executor_class: executor_class_manifest.count
                    for executor_class_manifest in manifest.executor_classes
                }
            else:
                executors = None
            manifests[miner_hotkey] = executors

        data = dict(
            validator_hotkey=self.own_keypair.ss58_address,
            stage_start_time={
                stage: _datetime_dump(dt) for stage, dt in self.stage_start_time.items()
            },
            average_job_send_time=_timedelta_dump(self.average_job_send_time),
            counts=counts,
            manifests=manifests,
        )
        return self.system_event(
            type=SystemEvent.EventType.VALIDATOR_TELEMETRY,
            subtype=SystemEvent.EventSubType.SYNTHETIC_BATCH,
            description="batch telemetry",
            data=data,
        )


def _datetime_dump(dt: datetime | None) -> str | None:
    if dt is None:
        return None
    return dt.isoformat()


def _timedelta_dump(delta: timedelta | None) -> float | None:
    if delta is None:
        return None
    return delta.total_seconds()


def _model_dump(model: BaseModel | None) -> dict | None:
    if model is None:
        return None
    return model.model_dump(mode="json")


def _handle_exceptions(ctx: BatchContext, exceptions: list[ExceptionInfo]) -> None:
    for exc_info in exceptions:
        name = ctx.jobs[exc_info.job_uuid].name
        text = f"{exc_info.stage}: {exc_info.exception!r}"
        logger.warning("%s %s", name, text)

        if isinstance(exc_info.exception, BaseException):
            subtype = SystemEvent.EventSubType.JOB_EXECUTION_TIMEOUT
        else:
            subtype = SystemEvent.EventSubType.GENERIC_ERROR

        ctx.system_event(
            type=SystemEvent.EventType.MINER_SYNTHETIC_JOB_FAILURE,
            subtype=subtype,
            description=text,
            miner_hotkey=exc_info.miner_hotkey,
            job_uuid=exc_info.job_uuid,
            func=exc_info.stage,
        )


def _init_context(
    axons: dict[str, bittensor.AxonInfo],
    serving_miners: list[Miner],
    batch_id: int | None = None,
    create_miner_client: Callable | None = None,
) -> BatchContext:
    start_time = datetime.now(tz=UTC)

    own_wallet = settings.BITTENSOR_WALLET()
    own_keypair = own_wallet.get_hotkey()
    create_miner_client = create_miner_client or MinerClient

    ctx = BatchContext(
        batch_id=batch_id,
        uuid=str(uuid.uuid4()),
        own_keypair=own_keypair,
        hotkeys=[],
        axons={},
        names={},
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
        event_count=0,
        stage_start_time={"_init_context": start_time},
        _loop=asyncio.get_running_loop(),
    )

    for miner in serving_miners:
        hotkey = miner.hotkey
        axon = axons[hotkey]
        ctx.hotkeys.append(hotkey)
        ctx.axons[hotkey] = axon
        ctx.names[hotkey] = f"{hotkey}({axon.ip}:{axon.port})"
        ctx.miners[hotkey] = miner
        ctx.clients[hotkey] = create_miner_client(ctx=ctx, miner_hotkey=hotkey)
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
    assert job.job_before_sent_time is not None

    if not job.success:
        assert job.score == 0

    if job.time_took is not None:
        time_took_sec = job.time_took.total_seconds()
    else:
        time_took_sec = 0

    payload = JobFinishedReceiptPayload(
        job_uuid=job.uuid,
        miner_hotkey=job.miner_hotkey,
        validator_hotkey=ctx.own_keypair.ss58_address,
        time_started=job.job_before_sent_time,
        time_took_us=int(time_took_sec * 1_000_000),
        score_str=f"{job.score:.6g}",
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
            await client.connect()
        except TransportConnectionError as exc:
            name = ctx.names[miner_hotkey]
            logger.warning("%s connection error: %r", name, exc)
            ctx.system_event(
                type=SystemEvent.EventType.MINER_SYNTHETIC_JOB_FAILURE,
                subtype=SystemEvent.EventSubType.MINER_CONNECTION_ERROR,
                description=repr(exc),
                miner_hotkey=miner_hotkey,
                func="connect",
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


async def _close_client(ctx: BatchContext, miner_hotkey: str) -> None:
    client = ctx.clients[miner_hotkey]

    async with asyncio.timeout(_CLOSE_TIMEOUT):
        await client.close()


async def _generate_jobs(ctx: BatchContext) -> None:
    start_time = time.time()
    generated_job_count = 0

    for hotkey, executors in ctx.executors.items():
        miner_name = ctx.names[hotkey]
        for executor_class, count in executors.items():
            job_generators = []
            for _ in range(count):
                job_generator = await current.synthetic_job_generator_factory.create(executor_class)
                await job_generator.ainit()
                job_uuid = str(job_generator.uuid())
                ctx.jobs[job_uuid] = Job(
                    ctx=ctx,
                    uuid=job_uuid,
                    name=f"{miner_name} job {job_uuid}",
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
    spin_up_time = max(spin_up_time, _MIN_SPIN_UP_TIME)
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
        await client.send_check(request_json)
        job.accept_after_sent_time = datetime.now(tz=UTC)

        await job.accept_response_event.wait()
        if isinstance(job.accept_response, V0AcceptJobRequest):
            await job.executor_response_event.wait()

    # send the receipt from outside the timeout
    if isinstance(job.executor_response, V0ExecutorReadyRequest):
        _generate_job_started_receipt(ctx, job)
        assert job.job_started_receipt is not None
        try:
            receipt_json = job.job_started_receipt.model_dump_json()
            async with asyncio.timeout(_SEND_RECEIPT_TIMEOUT):
                await client.send_check(receipt_json)
        except (Exception, asyncio.CancelledError) as exc:
            logger.warning("%s failed to send job started receipt: %r", job.name, exc)
            job.system_event(
                type=SystemEvent.EventType.RECEIPT_FAILURE,
                subtype=SystemEvent.EventSubType.RECEIPT_SEND_ERROR,
                description=repr(exc),
                func="_send_initial_job_request",
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
        await client.send_check(request_json)
        job.job_after_sent_time = datetime.now(tz=UTC)

        await job.job_response_event.wait()


async def _send_job_finished_receipts(ctx: BatchContext) -> None:
    for job in ctx.jobs.values():
        # generate job finished receipts for all jobs
        # which returned a response, even if they failed
        if job.job_response is not None:
            client = ctx.clients[job.miner_hotkey]
            try:
                _generate_job_finished_receipt(ctx, job)
                assert job.job_finished_receipt is not None

                receipt_json = job.job_finished_receipt.model_dump_json()
                async with asyncio.timeout(_SEND_RECEIPT_TIMEOUT):
                    await client.send_check(receipt_json)

            except (Exception, asyncio.CancelledError) as exc:
                logger.warning("%s failed to send job finished receipt: %r", job.name, exc)
                job.system_event(
                    type=SystemEvent.EventType.RECEIPT_FAILURE,
                    subtype=SystemEvent.EventSubType.RECEIPT_SEND_ERROR,
                    description=repr(exc),
                    func="_send_job_finished_receipts",
                )


def _emit_decline_or_failure_events(ctx: BatchContext) -> None:
    for job in ctx.jobs.values():
        if isinstance(job.accept_response, V0DeclineJobRequest) or isinstance(
            job.executor_response, V0ExecutorFailedRequest
        ):
            logger.warning("%s refused", job.name)
            job.system_event(
                type=SystemEvent.EventType.MINER_SYNTHETIC_JOB_FAILURE,
                subtype=SystemEvent.EventSubType.JOB_NOT_STARTED,
                description="refused",
            )
        if isinstance(job.job_response, V0JobFailedRequest):
            returncode = job.job_response.docker_process_exit_status
            text = f"failed: {returncode=}"
            logger.warning("%s %s", job.name, text)
            job.system_event(
                type=SystemEvent.EventType.MINER_SYNTHETIC_JOB_FAILURE,
                subtype=SystemEvent.EventSubType.FAILURE,
                description=text,
            )


def _emit_telemetry_events(ctx: BatchContext) -> None:
    batch_system_event = ctx.emit_telemetry_event()
    if batch_system_event is not None:
        counts = batch_system_event.data.get("counts")
        logger.info("Batch telemetry counts: %s", counts)

    for job in ctx.jobs.values():
        job.emit_telemetry_event()


async def _send_machine_specs(ctx: BatchContext) -> None:
    channel_layer = get_channel_layer()
    assert channel_layer is not None

    send_exc: BaseException | None = None
    send_exc_count = 0

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
                            "specs": job.machine_specs.specs.specs,
                        },
                    )
            except (Exception, asyncio.CancelledError) as exc:
                send_exc = exc
                send_exc_count += 1
                logger.warning("%s failed to send machine specs: %r", job.name, exc)

    if send_exc_count:
        msg = f"{send_exc_count} exceptions raised when trying to send machine specs. last one: {send_exc!r}"
        ctx.system_event(
            type=SystemEvent.EventType.VALIDATOR_CHANNEL_LAYER_ERROR,
            subtype=SystemEvent.EventSubType.SPECS_SEND_ERROR,
            description=msg,
            func="_send_machine_specs",
        )


async def _multi_get_miner_manifest(ctx: BatchContext) -> None:
    start_barrier = asyncio.Barrier(len(ctx.hotkeys))
    tasks = [
        asyncio.create_task(
            _get_miner_manifest(ctx, start_barrier, miner_hotkey),
            name=f"{miner_hotkey}._get_miner_manifest",
        )
        for miner_hotkey in ctx.hotkeys
    ]

    results = await asyncio.gather(*tasks, return_exceptions=True)

    for i, result in enumerate(results):
        if isinstance(result, BaseException):
            hotkey = ctx.hotkeys[i]
            name = ctx.names[hotkey]
            logger.warning("%s failed to get manifest: %r", name, result)

            if isinstance(result, TimeoutError | asyncio.CancelledError):
                subtype = SystemEvent.EventSubType.MANIFEST_TIMEOUT
            else:
                subtype = SystemEvent.EventSubType.MANIFEST_ERROR

            ctx.system_event(
                type=SystemEvent.EventType.MINER_SYNTHETIC_JOB_FAILURE,
                subtype=subtype,
                description=repr(result),
                miner_hotkey=hotkey,
                func="_get_miner_manifest",
            )
        else:
            assert result is None


async def _multi_close_client(ctx: BatchContext) -> None:
    tasks = [
        asyncio.create_task(
            _close_client(ctx, miner_hotkey),
            name=f"{miner_hotkey}._close_client",
        )
        for miner_hotkey in ctx.hotkeys
    ]

    results = await asyncio.gather(*tasks, return_exceptions=True)

    for i, result in enumerate(results):
        if isinstance(result, BaseException):
            hotkey = ctx.hotkeys[i]
            name = ctx.names[hotkey]
            logger.warning("%s failed to close client: %r", name, result)
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
            name=f"{job_uuid}._send_initial_job_request",
        )
        for job_uuid in ctx.job_uuids
    ]

    results = await asyncio.gather(*tasks, return_exceptions=True)

    exceptions: list[ExceptionInfo] = []
    for i, result in enumerate(results):
        if isinstance(result, BaseException):
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
        # occasionally we can get a job response (V0JobFailedRequest | V0JobFinishedRequest)
        # before sending the actual job request (V0JobRequest), for example because
        # the executor decide to abort the job before the details were sent
        and job.job_response is None
    ]
    logger.info("Sending job requests for %d ready jobs", len(executor_ready_job_uuids))
    start_barrier = asyncio.Barrier(len(executor_ready_job_uuids))
    tasks = [
        asyncio.create_task(
            _send_job_request(ctx, start_barrier, job_uuid),
            name=f"{job_uuid}._send_job_request",
        )
        for job_uuid in executor_ready_job_uuids
    ]

    results = await asyncio.gather(*tasks, return_exceptions=True)

    exceptions: list[ExceptionInfo] = []
    for i, result in enumerate(results):
        if isinstance(result, BaseException):
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


def _compute_average_send_time(ctx: BatchContext) -> None:
    durations_sec: list[float] = []

    for job in ctx.jobs.values():
        if job.job_after_sent_time is None:
            continue
        assert job.job_before_sent_time is not None
        duration = job.job_after_sent_time - job.job_before_sent_time
        duration_sec = duration.total_seconds()
        durations_sec.append(duration_sec)

    average_duration_sec = statistics.mean(durations_sec)
    assert average_duration_sec >= 0
    ctx.average_job_send_time = timedelta(seconds=average_duration_sec)
    logger.info("Average job send time: %.6f seconds", average_duration_sec)


async def _score_job(ctx: BatchContext, job: Job) -> None:
    job.score = 0
    job.score_manifest_multiplier = None
    job.average_job_send_time_bonus = None
    job.success = False
    job.comment = "failed"

    if job.job_response is None:
        job.comment = "timed out"
        logger.info("%s %s", job.name, job.comment)
        return

    if isinstance(job.job_response, V0JobFailedRequest):
        returncode = job.job_response.docker_process_exit_status
        job.comment = f"failed: {returncode=}"
        logger.info("%s %s", job.name, job.comment)
        return

    assert isinstance(job.job_response, V0JobFinishedRequest)
    assert job.job_response_time is not None

    # !!! time_took can be negative if miner sends responses out of order
    if job.job_before_sent_time is not None:
        job.time_took = job.job_response_time - job.job_before_sent_time
    else:
        job.time_took = None

    if job.time_took is None or job.time_took.total_seconds() <= 0:
        job.comment = "out of order job response"
        logger.info("%s %s", job.name, job.comment)
        job.system_event(
            type=SystemEvent.EventType.MINER_SYNTHETIC_JOB_FAILURE,
            subtype=SystemEvent.EventSubType.JOB_EXECUTION_TIMEOUT,
            description=job.comment,
        )
        return

    if _GIVE_AVERAGE_JOB_SEND_TIME_BONUS:
        # subtract the average time to send a job request. this will normalize
        # the timing between validators with different upload speeds.
        # if the time becomes negative, set it to 1 sec
        assert ctx.average_job_send_time is not None
        job.average_job_send_time_bonus = ctx.average_job_send_time
        job.time_took -= ctx.average_job_send_time
        if job.time_took.total_seconds() <= 0:
            job.time_took = timedelta(seconds=1)

    time_took_sec = job.time_took.total_seconds()

    # TODO separate correctness check from scoring in job generator
    job.correct, comment, score = job.job_generator.verify(
        job.job_response,
        time_took_sec,
    )

    if time_took_sec > job.job_generator.timeout_seconds():
        job.comment = f"took too long: {time_took_sec=:.2f}"
        logger.info("%s %s", job.name, job.comment)
        job.system_event(
            type=SystemEvent.EventType.MINER_SYNTHETIC_JOB_FAILURE,
            subtype=SystemEvent.EventSubType.JOB_EXECUTION_TIMEOUT,
            description=job.comment,
        )
        return

    job.success = job.correct
    job.comment = comment
    job.score = score

    if job.success:
        job.system_event(
            type=SystemEvent.EventType.MINER_SYNTHETIC_JOB_SUCCESS,
            subtype=SystemEvent.EventSubType.SUCCESS,
            description=job.comment,
        )
    else:
        job.system_event(
            type=SystemEvent.EventType.MINER_SYNTHETIC_JOB_FAILURE,
            subtype=SystemEvent.EventSubType.FAILURE,
            description=job.comment,
        )

    logger.info(
        "%s finished with %s in %.2f seconds with score %.6g: %s",
        job.name,
        "success" if job.success else "failure",
        time_took_sec,
        job.score,
        job.comment,
    )


async def _score_jobs(ctx: BatchContext) -> None:
    for job in ctx.jobs.values():
        try:
            await _score_job(ctx, job)
        except (Exception, asyncio.CancelledError) as exc:
            logger.warning("%s failed to score: %r", job.name, exc)
            job.system_event(
                type=SystemEvent.EventType.MINER_SYNTHETIC_JOB_FAILURE,
                subtype=SystemEvent.EventSubType.MINER_SCORING_ERROR,
                description=repr(exc),
                func="_score_jobs",
            )

    # compute for each hotkey how many executors finished successfully
    for job in ctx.jobs.values():
        if job.success:
            ctx.online_executor_count[job.miner_hotkey] += 1

    # apply manifest bonus
    # do not combine with the previous loop, we use online_executor_count
    for job in ctx.jobs.values():
        if job.success:
            try:
                job.score_manifest_multiplier = await get_manifest_multiplier(
                    ctx.previous_online_executor_count[job.miner_hotkey],
                    ctx.online_executor_count[job.miner_hotkey],
                )
            except (Exception, asyncio.CancelledError) as exc:
                logger.warning("%s failed to score: %r", job.name, exc)
                job.system_event(
                    type=SystemEvent.EventType.MINER_SYNTHETIC_JOB_FAILURE,
                    subtype=SystemEvent.EventSubType.MINER_SCORING_ERROR,
                    description=repr(exc),
                    func="_score_jobs",
                )
            if job.score_manifest_multiplier is not None:
                job.score *= job.score_manifest_multiplier


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
def _db_persist_system_events(ctx: BatchContext) -> None:
    if not ctx.events:
        return

    logger.info("Persisting %d system events", len(ctx.events))
    SystemEvent.objects.bulk_create(ctx.events)

    # we call this function multiple times during a batch,
    # clear the list to avoid duplicate events
    ctx.events.clear()


# sync_to_async is needed since we use the sync Django ORM
@sync_to_async
def _db_persist(ctx: BatchContext) -> None:
    start_time = time.time()

    # persist the batch and the jobs in the same transaction, to
    # prevent a situation where because of a crash only some of
    # the jobs are saved, which would generate incorrect weights
    with transaction.atomic():
        if ctx.batch_id is not None:
            batch = SyntheticJobBatch.objects.get(id=ctx.batch_id)
        else:
            batch = SyntheticJobBatch(
                started_at=ctx.stage_start_time["_init_context"],
            )
        # accepting_results_until is not used anywhere, it doesn't
        # matter that we pick a somewhat arbitrary time for it
        now = datetime.now(tz=UTC)
        batch.accepting_results_until = ctx.stage_start_time.get("_multi_send_job_request", now)
        batch.save()

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


async def execute_synthetic_batch_run(
    axons: dict[str, bittensor.AxonInfo],
    serving_miners: list[Miner],
    batch_id: int | None = None,
    create_miner_client: Callable | None = None,
) -> None:
    if not axons or not serving_miners:
        logger.warning("No miners provided")
        return
    logger.info("Executing synthetic jobs batch for %d miners", len(serving_miners))

    # randomize the order of miners each batch to avoid systemic bias
    random.shuffle(serving_miners)

    logger.info("STAGE: _init_context")
    ctx = _init_context(axons, serving_miners, batch_id, create_miner_client)

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

                # don't persist system events before this point, we want to minimize
                # any extra interactions which could slow down job processing before
                # we get the responses from the miners
                await _db_persist_system_events(ctx)

                logger.info("STAGE: _compute_average_send_time")
                ctx.stage_start_time["_compute_average_send_time"] = datetime.now(tz=UTC)
                _compute_average_send_time(ctx)

                logger.info("STAGE: _score_jobs")
                ctx.stage_start_time["_score_jobs"] = datetime.now(tz=UTC)
                await _score_jobs(ctx)

                await _db_persist_system_events(ctx)

                logger.info("STAGE: _send_job_finished_receipts")
                ctx.stage_start_time["_send_job_finished_receipts"] = datetime.now(tz=UTC)
                await _send_job_finished_receipts(ctx)

            else:
                logger.warning("No jobs accepted")

            await _db_persist_system_events(ctx)

            logger.info("STAGE: _emit_decline_or_failure_events")
            ctx.stage_start_time["_emit_decline_or_failure_events"] = datetime.now(tz=UTC)
            _emit_decline_or_failure_events(ctx)

        else:
            logger.warning("No executors available")

    except (Exception, asyncio.CancelledError) as exc:
        logger.error("Synthetic jobs batch failure: %r", exc)
        ctx.system_event(
            type=SystemEvent.EventType.VALIDATOR_FAILURE,
            subtype=SystemEvent.EventSubType.GENERIC_ERROR,
            description=repr(exc),
            func="execute_synthetic_batch_run",
        )

    await _db_persist_system_events(ctx)

    try:
        logger.info("STAGE: _multi_close_client")
        ctx.stage_start_time["_multi_close_client"] = datetime.now(tz=UTC)
        await _multi_close_client(ctx)
    except (Exception, asyncio.CancelledError) as exc:
        logger.error("Synthetic jobs batch failure: %r", exc)
        ctx.system_event(
            type=SystemEvent.EventType.VALIDATOR_FAILURE,
            subtype=SystemEvent.EventSubType.GENERIC_ERROR,
            description=repr(exc),
            func="_multi_close_client",
        )

    try:
        logger.info("STAGE: _emit_telemetry_events")
        ctx.stage_start_time["_emit_telemetry_events"] = datetime.now(tz=UTC)
        _emit_telemetry_events(ctx)
    except (Exception, asyncio.CancelledError) as exc:
        logger.error("Synthetic jobs batch failure: %r", exc)
        ctx.system_event(
            type=SystemEvent.EventType.VALIDATOR_FAILURE,
            subtype=SystemEvent.EventSubType.GENERIC_ERROR,
            description=repr(exc),
            func="_emit_telemetry_events",
        )

    await _db_persist_system_events(ctx)

    logger.info("STAGE: _db_persist")
    ctx.stage_start_time["_db_persist"] = datetime.now(tz=UTC)
    await _db_persist(ctx)

    # send the machine specs after the batch is done, it can fail or take a long time
    try:
        logger.info("STAGE: _send_machine_specs")
        await _send_machine_specs(ctx)
    except (Exception, asyncio.CancelledError) as exc:
        logger.error("Synthetic jobs batch failure: %r", exc)

    await _db_persist_system_events(ctx)

    logger.info("BATCH DONE")
