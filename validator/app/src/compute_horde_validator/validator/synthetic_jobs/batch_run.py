import asyncio
import logging
import random
import time
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta

import bittensor
from asgiref.sync import async_to_sync
from compute_horde.base_requests import BaseRequest
from compute_horde.executor_class import DEFAULT_EXECUTOR_CLASS, EXECUTOR_CLASS, ExecutorClass
from compute_horde.miner_client.base import (
    AbstractMinerClient,
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
    V0AuthenticateRequest,
    V0InitialJobRequest,
    V0JobRequest,
    VolumeType,
)
from compute_horde.utils import MachineSpecs
from django.conf import settings

from compute_horde_validator.validator.models import (
    Miner,
    SyntheticJobBatch,
)
from compute_horde_validator.validator.synthetic_jobs.generator import current
from compute_horde_validator.validator.synthetic_jobs.generator.base import (
    BaseSyntheticJobGenerator,
)

logger = logging.getLogger(__name__)

_JOB_TIMEOUT = 60
_CLOSE_TIMEOUT = 1
_BATCH_TIMEOUT = 5 * 60
_GET_MANIFEST_TIMEOUT = 30


class MinerClient(AbstractMinerClient):
    def __init__(
        self,
        ctx: "BatchContext",
        own_keypair: bittensor.Keypair,
        miner_hotkey: str,
        miner_address: str,
        miner_port: int,
    ):
        miner_name = f"{miner_hotkey}({miner_address}:{miner_port})"
        super().__init__(miner_name)

        self.ctx = ctx
        self.own_hotkey = own_keypair.ss58_address
        self.own_keypair = own_keypair
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
        logger.info(f"----------> HANDLE {msg.model_dump_json()}")

        if isinstance(msg, GenericError):
            error = f"Received error message from miner {self.miner_name}: {msg.model_dump_json()}"
            logger.warning(error)
            if msg.details is not None and msg.details.startswith(
                ("Unknown validator", "Inactive validator")
            ):
                logger.error(f"Closing connection to miner {self.miner_name}")
                # miner doesn't recognize our authority, close the connection to avoid retries
                await self.close()
            # await save_job_execution_event(
            #     subtype=SystemEvent.EventSubType.GENERIC_ERROR, long_description=error
            # )
            return

        if isinstance(msg, UnauthorizedError):
            logger.error(f"Unauthorized in {self.miner_name}: {msg.code}, details: {msg.details}")
            # await save_job_execution_event(
            #     subtype=SystemEvent.EventSubType.UNAUTHORIZED, long_description=msg
            # )
            return

        if isinstance(msg, V0ExecutorManifestRequest):
            try:
                self.ctx.manifests[self.miner_hotkey] = msg.manifest
                self.ctx.manifest_events[self.miner_hotkey].set()
            except asyncio.InvalidStateError:
                logger.warning(f"Received unexpected {msg} from {self.miner_name}")
            return

        job_uuid = getattr(msg, "job_uuid", None)
        if job_uuid is not None:
            job = self.ctx.jobs.get(job_uuid)
            if job is not None:
                job.handle_message(msg, self.miner_name)
            else:
                logger.info(f"Received info about another job {msg} from {self.miner_name}")
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
class Job:
    miner_hotkey: str
    executor_class: ExecutorClass
    job_generator: BaseSyntheticJobGenerator
    volume_contents: str

    # responses

    accept_response: V0AcceptJobRequest | V0DeclineJobRequest | None = None
    accept_response_time: float | None = None
    accept_response_event: asyncio.Event = field(default_factory=asyncio.Event)

    executor_response: V0ExecutorFailedRequest | V0ExecutorReadyRequest | None = None
    executor_response_time: float | None = None
    executor_response_event: asyncio.Event = field(default_factory=asyncio.Event)

    payload_sent_time_before: float | None = None
    payload_sent_time_after: float | None = None

    outcome: V0JobFailedRequest | V0JobFinishedRequest | None = None
    outcome_time: float | None = None
    outcome_event: asyncio.Event = field(default_factory=asyncio.Event)

    machine_specs: MachineSpecs | None = None

    def handle_message(self, msg: BaseRequest, miner_name: str) -> None:
        unexpected = False
        match msg:
            case V0AcceptJobRequest() | V0DeclineJobRequest():
                if self.accept_response is None:
                    self.accept_response = msg
                    self.accept_response_time = time.time()
                    self.accept_response_event.set()
                else:
                    unexpected = True

            case V0ExecutorReadyRequest() | V0ExecutorFailedRequest():
                if self.executor_response is None:
                    self.executor_response = msg
                    self.executor_response_time = time.time()
                    self.executor_response_event.set()
                else:
                    unexpected = True

            case V0JobFinishedRequest() | V0JobFailedRequest():
                if self.outcome is None:
                    self.outcome = msg
                    self.outcome_time = time.time()
                    self.outcome_event.set()
                else:
                    unexpected = True

            case V0MachineSpecsRequest():
                self.machine_specs = msg.specs

        if unexpected:
            logger.warning("Received unexpected %s from %s", msg, miner_name)


@dataclass
class BatchContext:
    # randomized, but order preserving list of miner.hotkeys
    # used to go from indices returned by asyncio.gather() back to miner.hotkey
    hotkeys: list[str]

    # all dictionaries have miner.hotkey as key
    axons: dict[str, bittensor.AxonInfo]
    miners: dict[str, Miner]
    clients: dict[str, MinerClient]
    executors: dict[str, defaultdict[ExecutorClass, int]]
    job_generators: dict[str, dict[ExecutorClass, list[BaseSyntheticJobGenerator]]]

    manifests: dict[str, ExecutorManifest | None]
    manifest_events: dict[str, asyncio.Event]

    # randomized, but order preserving list of job.uuid
    # used to go from indices returned by asyncio.gather() back to job.uuid
    job_uuids: list[str]

    # job.uuid as key
    jobs: dict[str, Job]


def _init_context(
    axons: dict[str, bittensor.AxonInfo], serving_miners: list[Miner]
) -> BatchContext:
    own_wallet = settings.BITTENSOR_WALLET()

    ctx = BatchContext(
        hotkeys=[],
        axons={},
        miners={},
        clients={},
        executors={},
        job_generators={},
        manifests={},
        manifest_events={},
        job_uuids=[],
        jobs={},
    )

    for miner in serving_miners:
        hotkey = miner.hotkey
        axon = axons[hotkey]
        ctx.hotkeys.append(hotkey)
        ctx.axons[hotkey] = axon
        ctx.miners[hotkey] = miner
        ctx.clients[hotkey] = MinerClient(
            ctx=ctx,
            own_keypair=own_wallet.get_hotkey(),
            miner_hotkey=hotkey,
            miner_address=axon.ip,
            miner_port=axon.port,
        )
        ctx.executors[hotkey] = defaultdict(int)
        ctx.job_generators[hotkey] = {}
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


async def _get_miner_manifest(
    ctx: BatchContext, start_barrier: asyncio.Barrier, miner_hotkey: str
) -> None:
    await start_barrier.wait()

    async with asyncio.timeout(_GET_MANIFEST_TIMEOUT):
        client = ctx.clients[miner_hotkey]

        await client.await_connect()

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
    logger.info("Generating jobs")

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

    async with asyncio.timeout(max_spin_up_time):
        job = ctx.jobs[job_uuid]

        spin_up_time = EXECUTOR_CLASS[job.executor_class].spin_up_time
        assert spin_up_time is not None
        stagger_wait_interval = max_spin_up_time - spin_up_time
        assert stagger_wait_interval >= 0
        if stagger_wait_interval > 0:
            await asyncio.sleep(max_spin_up_time - spin_up_time)

        client = ctx.clients[job.miner_hotkey]
        await client.send_model(
            V0InitialJobRequest(
                job_uuid=str(job_uuid),
                executor_class=job.executor_class,
                base_docker_image_name=job.job_generator.base_docker_image_name(),
                timeout_seconds=job.job_generator.timeout_seconds(),
                volume_type=VolumeType.inline,
            )
        )

        await job.accept_response_event.wait()
        if isinstance(job.accept_response, V0AcceptJobRequest):
            await job.executor_response_event.wait()


async def _send_job_request(
    ctx: BatchContext, start_barrier: asyncio.Barrier, job_uuid: str
) -> None:
    await start_barrier.wait()

    async with asyncio.timeout(_JOB_TIMEOUT):
        job = ctx.jobs[job_uuid]
        client = ctx.clients[job.miner_hotkey]

        job_request = V0JobRequest(
            job_uuid=job_uuid,
            executor_class=job.executor_class,
            docker_image_name=job.job_generator.docker_image_name(),
            docker_run_options_preset=job.job_generator.docker_run_options_preset(),
            docker_run_cmd=job.job_generator.docker_run_cmd(),
            raw_script=job.job_generator.raw_script(),
            volume={
                "volume_type": VolumeType.inline.value,
                "contents": job.volume_contents,
            },
            output_upload=None,
        )

        job.payload_sent_time_before = time.time()
        await client.send_model(job_request)
        job.payload_sent_time_after = time.time()

        await job.outcome_event.wait()


async def _close_clients(ctx: BatchContext) -> None:
    for hotkey, client in ctx.clients.items():
        try:
            async with asyncio.timeout(_CLOSE_TIMEOUT):
                await client.close()
        except Exception as exc:
            logger.error("%s failed to close client: %r", hotkey, exc)


async def _multi_get_miner_manifest(ctx: BatchContext) -> None:
    logger.info("Gathering manifests from %d miners", len(ctx.hotkeys))
    start_barrier = asyncio.Barrier(len(ctx.hotkeys))
    tasks = [
        asyncio.create_task(
            _get_miner_manifest(ctx, start_barrier, miner_hotkey), name=f"{miner_hotkey}_manifest"
        )
        for miner_hotkey in ctx.hotkeys
    ]
    results = await asyncio.gather(*tasks, return_exceptions=True)
    for i, result in enumerate(results):
        if isinstance(result, BaseException):
            hotkey = ctx.hotkeys[i]
            logger.error("%s failed to get manifest: %r", hotkey, result)
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
            name=f"{job_uuid}_initial",
        )
        for job_uuid in ctx.job_uuids
    ]
    results = await asyncio.gather(*tasks, return_exceptions=True)
    for i, result in enumerate(results):
        if isinstance(result, BaseException):
            job_uuid = ctx.job_uuids[i]
            job = ctx.jobs[job_uuid]
            logger.error("%s initial job request %s failed: %r", job.miner_hotkey, job_uuid, result)
        else:
            assert result is None


async def _multi_send_job_request(ctx: BatchContext) -> None:
    executor_ready_job_uuids = [
        job_uuid
        for job_uuid, job in ctx.jobs.items()
        if isinstance(job.executor_response, V0ExecutorReadyRequest)
    ]
    logger.info("Sending job requests for %d ready jobs", len(executor_ready_job_uuids))
    start_barrier = asyncio.Barrier(len(executor_ready_job_uuids))
    tasks = [
        asyncio.create_task(
            _send_job_request(ctx, start_barrier, job_uuid),
            name=f"{job_uuid}_job",
        )
        for job_uuid in executor_ready_job_uuids
    ]
    results = await asyncio.gather(*tasks, return_exceptions=True)
    for i, result in enumerate(results):
        if isinstance(result, BaseException):
            job_uuid = executor_ready_job_uuids[i]
            job = ctx.jobs[job_uuid]
            logger.error("%s job request %s failed: %r", job.miner_hotkey, job_uuid, result)
        else:
            assert result is None


async def _score_jobs(ctx: BatchContext) -> None:
    for job_uuid, job in ctx.jobs.items():
        if isinstance(job.outcome, V0JobFinishedRequest):
            assert job.payload_sent_time_before is not None
            assert job.outcome_time is not None
            job_time_took = job.outcome_time - job.payload_sent_time_before
            success, comment, score = job.job_generator.verify(job.outcome, job_time_took)
            logger.info(
                "%s %s in %.2f seconds with score %f: %s",
                job_uuid,
                "succeeded" if success else "failed",
                job_time_took,
                score,
                comment,
            )


@async_to_sync
async def execute_synthetic_batch_run(
    axons: dict[str, bittensor.AxonInfo], serving_miners: list[Miner]
) -> None:
    if not axons or not serving_miners:
        logger.error("No miners provided")
        return
    logger.info("Executing synthetic jobs batch for %d miners", len(serving_miners))

    # randomize the order of miners each batch to avoid systemic bias
    random.shuffle(serving_miners)

    ctx = _init_context(axons, serving_miners)

    await _multi_get_miner_manifest(ctx)

    total_executor_count = _get_total_executor_count(ctx)
    if total_executor_count == 0:
        logger.error("No executors available")
        return

    await _generate_jobs(ctx)

    # randomize the order of jobs each batch to avoid systemic bias
    random.shuffle(ctx.job_uuids)

    await _multi_send_initial_job_request(ctx)

    if not any(isinstance(job.accept_response, V0AcceptJobRequest) for job in ctx.jobs.values()):
        logger.error("No jobs accepted")
        return

    await _multi_send_job_request(ctx)

    await _score_jobs(ctx)

    await _close_clients(ctx)

    batch_start_time = datetime.now(tz=UTC)
    batch_timeout_time = batch_start_time + timedelta(seconds=_BATCH_TIMEOUT)
    await SyntheticJobBatch.objects.acreate(
        started_at=batch_start_time,
        accepting_results_until=batch_timeout_time,
    )

    # miners_previous_online_executors = await get_previous_online_executors(miners, batch)
