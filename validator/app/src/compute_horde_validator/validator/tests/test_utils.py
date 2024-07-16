import asyncio
import logging
import threading
import time
import uuid
from unittest.mock import MagicMock, patch

import bittensor
import pytest
from asgiref.sync import sync_to_async
from compute_horde.executor_class import DEFAULT_EXECUTOR_CLASS, ExecutorClass
from compute_horde.miner_client.base import BaseRequest
from compute_horde.mv_protocol.miner_requests import (
    ExecutorClassManifest,
    ExecutorManifest,
    V0DeclineJobRequest,
    V0ExecutorFailedRequest,
    V0ExecutorReadyRequest,
    V0JobFailedRequest,
    V0JobFinishedRequest,
)
from compute_horde.mv_protocol.validator_requests import (
    V0JobFinishedReceiptRequest,
)
from constance.test.unittest import override_config
from django.utils.timezone import now

from compute_horde_validator.validator.models import (
    Miner,
    MinerManifest,
    SyntheticJob,
    SyntheticJobBatch,
    SystemEvent,
)
from compute_horde_validator.validator.synthetic_jobs.generator.base import (
    BaseSyntheticJobGenerator,
    BaseSyntheticJobGeneratorFactory,
)
from compute_horde_validator.validator.synthetic_jobs.utils import (
    MinerClient,
    create_and_run_sythethic_job_batch,
    execute_miner_synthetic_jobs,
)

from .helpers import (
    check_system_events,
    get_miner_client,
)

MOCK_SCORE = 0.8
MANIFEST_INCENTIVE_MULTIPLIER = 1.05
MANIFEST_DANCE_RATIO_THRESHOLD = 1.4
MANIFEST_INCENTIVE_APPLIED_SCORE = MOCK_SCORE * MANIFEST_INCENTIVE_MULTIPLIER
NOT_SCORED = 0.0

logger = logging.getLogger(__name__)


class MockSyntheticJobGenerator(BaseSyntheticJobGenerator):
    async def ainit(self):
        return

    def timeout_seconds(self) -> int:
        return 1

    def base_docker_image_name(self) -> str:
        return "mock"

    def docker_image_name(self) -> str:
        return "mock"

    def docker_run_options_preset(self) -> str:
        return "mock"

    def docker_run_cmd(self) -> list[str] | None:
        return ["mock"]

    async def volume_contents(self) -> str:
        return "mock"

    def verify(self, msg: V0JobFinishedRequest, time_took: float) -> tuple[bool, str, float]:
        return True, "mock", MOCK_SCORE

    def job_description(self) -> str:
        return "mock"


class TimeToookScoreMockSyntheticJobGenerator(MockSyntheticJobGenerator):
    def verify(self, msg: V0JobFinishedRequest, time_took: float) -> tuple[bool, str, float]:
        return True, "mock", 1 / time_took


async def await_for_not_none(fun, timeout=5):
    start = time.time()
    while time.time() - start < timeout:
        value = fun()
        if value is not None:
            return value
        await asyncio.sleep(0.1)
    raise TimeoutError("timeout!")


def wait_for_not_none(fun, timeout=5):
    start = time.time()
    while time.time() - start < timeout:
        value = fun()
        if value is not None:
            return value
        time.sleep(0.1)
    raise TimeoutError("timeout!")


async def await_for_true(fun, timeout=5):
    start = time.time()
    while time.time() - start < timeout:
        if fun():
            return
        await asyncio.sleep(0.1)
    raise TimeoutError("timeout!")


def wait_for_true(fun, timeout=5):
    start = time.time()
    while time.time() - start < timeout:
        if fun():
            return
        time.sleep(0.1)
    raise TimeoutError("timeout!")


async def miner_synthetic_jobs_scheme(
    mocked_synthetic_miner_client,
    manifest_callback,
    expected_jobs,
    interaction_callback,
    prev_online_executor_count=None,
):
    miner, _ = await Miner.objects.aget_or_create(hotkey="miner_hotkey")

    batch = await SyntheticJobBatch.objects.acreate(
        started_at="2021-09-01 00:00:00",
        accepting_results_until="2021-09-01 00:00:00",
    )

    miner_axon_info = bittensor.AxonInfo(
        version=4,
        ip="ignore",
        ip_type=4,
        port=9999,
        hotkey="miner_hotkey",
        coldkey="miner_hotkey",
    )

    # create synthetic jobs task for miner
    task = asyncio.create_task(
        execute_miner_synthetic_jobs(
            batch.pk,
            miner.pk,
            "miner_hotkey",
            miner_axon_info,
            prev_online_executor_count,
        )
    )
    try:
        # wait for creation of mocked MinerClient to get instance
        miner_client = await await_for_not_none(lambda: mocked_synthetic_miner_client.instance)

        # set manifest using manifest_callback
        manifest_callback(miner_client)

        # wait for task to consume manifest future result and setup jobs
        await await_for_true(lambda: len(miner_client.job_states) == expected_jobs)

        # simulate miner interaction using interaction_callback
        interaction_callback(miner_client)
    finally:
        # wait for synthetic jobs task is finished
        await asyncio.wait_for(task, 5)


def syntethic_batch_scheme_single_miner(
    settings,
    mocked_synthetic_miner_client,
    manifest_callback,
    expected_jobs,
    interaction_callback,
    miner_hotkey="miner_hotkey",
):
    settings.DEBUG_MINER_KEY = miner_hotkey
    settings.DEBUG_MINER_ADDRESS = "ignore"
    settings.DEBUG_MINER_PORT = 9999

    async def as_coro(fun, *args, **kwargs):
        fun(*args, *kwargs)

    thread = threading.Thread(
        target=create_and_run_sythethic_job_batch, args=(1, "test"), daemon=True
    )
    thread.start()

    try:
        miner_client = wait_for_not_none(lambda: mocked_synthetic_miner_client.instance)

        # we change async state, eg. Futures - so run as coroutine threadsafe using loop from miner_manifest Future
        loop = miner_client.miner_manifest.get_loop()

        asyncio.run_coroutine_threadsafe(as_coro(manifest_callback, miner_client), loop)

        wait_for_true(lambda: len(miner_client.job_states) == expected_jobs)

        asyncio.run_coroutine_threadsafe(as_coro(interaction_callback, miner_client), loop)
    finally:
        # contrary to the async version this does not cancel task, so it might be left running
        # and influence next tests
        thread.join(timeout=5)


class MockSyntheticJobGeneratorFactory(BaseSyntheticJobGeneratorFactory):
    async def create(self, executor_class: ExecutorClass) -> BaseSyntheticJobGenerator:
        return MockSyntheticJobGenerator()


mock_synthetic_job_generator_factory = MagicMock(name="MockSyntheticJobGeneratorFactory")


@patch("compute_horde_validator.validator.synthetic_jobs.utils.JOB_LENGTH", 5)
@patch("compute_horde_validator.validator.synthetic_jobs.utils.TIMEOUT_LEEWAY", 1)
@patch("compute_horde_validator.validator.synthetic_jobs.utils.TIMEOUT_MARGIN", 1)
@patch("compute_horde_validator.validator.synthetic_jobs.utils.TIMEOUT_BARRIER", 3)
@patch(
    "compute_horde_validator.validator.synthetic_jobs.generator.current.synthetic_job_generator_factory",
    MockSyntheticJobGeneratorFactory(),
)
@pytest.mark.asyncio
@pytest.mark.django_db(databases=["default", "default_alias"], transaction=True)
@pytest.mark.parametrize(
    "futures_result,expected_job_status,expected_score,expected_system_event",
    [
        (
            (None, None),
            SyntheticJob.Status.FAILED,
            NOT_SCORED,
            (
                SystemEvent.EventType.MINER_SYNTHETIC_JOB_FAILURE,
                SystemEvent.EventSubType.JOB_NOT_STARTED,
                1,
            ),
        ),
        (
            (V0DeclineJobRequest, None),
            SyntheticJob.Status.FAILED,
            NOT_SCORED,
            (
                SystemEvent.EventType.MINER_SYNTHETIC_JOB_FAILURE,
                SystemEvent.EventSubType.JOB_NOT_STARTED,
                1,
            ),
        ),
        (
            (V0ExecutorReadyRequest, None),
            SyntheticJob.Status.FAILED,
            NOT_SCORED,
            (
                SystemEvent.EventType.MINER_SYNTHETIC_JOB_FAILURE,
                SystemEvent.EventSubType.JOB_EXECUTION_TIMEOUT,
                1,
            ),
        ),
        (
            (V0ExecutorReadyRequest, V0JobFailedRequest),
            SyntheticJob.Status.FAILED,
            NOT_SCORED,
            (
                SystemEvent.EventType.MINER_SYNTHETIC_JOB_FAILURE,
                SystemEvent.EventSubType.FAILURE,
                1,
            ),
        ),
        (
            (V0ExecutorReadyRequest, V0JobFinishedRequest),
            SyntheticJob.Status.COMPLETED,
            MOCK_SCORE,
            (
                SystemEvent.EventType.MINER_SYNTHETIC_JOB_SUCCESS,
                SystemEvent.EventSubType.SUCCESS,
                1,
            ),
        ),
    ],
)
async def test_execute_synthetic_job(
    futures_result,
    expected_job_status,
    expected_score,
    expected_system_event,
    override_weights_version_v1,
    mocked_synthetic_miner_client,
):
    manifest = ExecutorManifest(
        executor_classes=[ExecutorClassManifest(executor_class=DEFAULT_EXECUTOR_CLASS, count=1)]
    )
    job_uuid = None

    def manifest_callback(miner_client):
        miner_client.miner_manifest.set_result(manifest)

    def interaction_callback(miner_client):
        nonlocal job_uuid
        job_uuid = list(miner_client.job_states)[0]
        f0, f1 = futures_result
        if f0:
            miner_client.job_states[job_uuid].miner_ready_or_declining_future.set_result(
                f0(job_uuid=job_uuid)
            )
        if f1:
            miner_client.job_states[job_uuid].miner_finished_or_failed_future.set_result(
                f1(
                    job_uuid=job_uuid,
                    docker_process_stdout="",
                    docker_process_stderr="",
                )
            )

    await miner_synthetic_jobs_scheme(
        mocked_synthetic_miner_client, manifest_callback, 1, interaction_callback
    )

    job = await SyntheticJob.objects.aget(job_uuid=job_uuid)

    assert job.score == expected_score
    assert job.status == expected_job_status

    if expected_system_event:
        await sync_to_async(check_system_events)(*expected_system_event)
    else:
        assert await SystemEvent.objects.aget() == 0


@pytest.mark.asyncio
@pytest.mark.django_db(databases=["default", "default_alias"], transaction=True)
@pytest.mark.parametrize(
    "msg",
    [
        V0ExecutorReadyRequest(job_uuid=str(uuid.uuid4())),
        V0ExecutorFailedRequest(job_uuid=str(uuid.uuid4())),
        V0ExecutorReadyRequest(job_uuid=str(uuid.uuid4())),
    ],
)
async def test_miner_client__handle_message__set_ready_or_declining_future(msg: BaseRequest):
    miner_client = get_miner_client(MinerClient, msg.job_uuid)
    assert not miner_client.get_job_state(msg.job_uuid).miner_ready_or_declining_future.done()
    await miner_client.handle_message(msg)
    assert await miner_client.get_job_state(msg.job_uuid).miner_ready_or_declining_future == msg


@pytest.mark.asyncio
@pytest.mark.django_db(databases=["default", "default_alias"], transaction=True)
@pytest.mark.parametrize(
    "msg",
    [
        V0JobFailedRequest(
            job_uuid=str(uuid.uuid4()),
            docker_process_exit_status=1,
            docker_process_stdout="stdout",
            docker_process_stderr="stderr",
        ),
        V0JobFinishedRequest(
            job_uuid=str(uuid.uuid4()),
            docker_process_exit_status=1,
            docker_process_stdout="stdout",
            docker_process_stderr="stderr",
        ),
    ],
)
async def test_miner_client__handle_message__set_other_msg(msg: BaseRequest):
    miner_client = get_miner_client(MinerClient, msg.job_uuid)
    assert not miner_client.get_job_state(msg.job_uuid).miner_finished_or_failed_future.done()
    await miner_client.handle_message(msg)
    assert await miner_client.get_job_state(msg.job_uuid).miner_finished_or_failed_future == msg


async def create_mock_job_batches(miner):
    return [
        await SyntheticJobBatch.objects.acreate(
            started_at=f"2021-01-01 00:0{i}:00",
            accepting_results_until=f"2021-01-01 00:0{i+1}:00",
        )
        for i in range(5)
    ]


class TimeToookScoreMockSyntheticJobGeneratorFactory(BaseSyntheticJobGeneratorFactory):
    async def create(self, executor_class: ExecutorClass) -> BaseSyntheticJobGenerator:
        return TimeToookScoreMockSyntheticJobGenerator()


time_took_mock_synthetic_job_generator_factory = MagicMock(
    name="TimeToookScoreMockSyntheticJobGeneratorFactory"
)


@patch("compute_horde_validator.validator.synthetic_jobs.utils.JOB_LENGTH", 5)
@patch("compute_horde_validator.validator.synthetic_jobs.utils.TIMEOUT_LEEWAY", 1)
@patch("compute_horde_validator.validator.synthetic_jobs.utils.TIMEOUT_MARGIN", 1)
@patch("compute_horde_validator.validator.synthetic_jobs.utils.TIMEOUT_BARRIER", 3)
@patch(
    "compute_horde_validator.validator.synthetic_jobs.generator.current.synthetic_job_generator_factory",
    TimeToookScoreMockSyntheticJobGeneratorFactory(),
)
@pytest.mark.asyncio
@pytest.mark.django_db(databases=["default", "default_alias"], transaction=True)
@override_config(
    DYNAMIC_MANIFEST_SCORE_MULTIPLIER=MANIFEST_INCENTIVE_MULTIPLIER,
    DYNAMIC_MANIFEST_DANCE_RATIO_THRESHOLD=MANIFEST_DANCE_RATIO_THRESHOLD,
)
@pytest.mark.parametrize(
    "curr_online_executor_count,prev_online_executor_count,expected_multiplier",
    [
        # None -> 3
        (3, None, MANIFEST_INCENTIVE_MULTIPLIER),
        # 0 -> 3 - e.g. all executors failed to start cause docker images were not cached
        (3, 0, MANIFEST_INCENTIVE_MULTIPLIER),
        # 10 -> below ratio
        (10, int(10 * MANIFEST_DANCE_RATIO_THRESHOLD) - 1, 1),
        # below ratio -> 10
        (int(10 * MANIFEST_DANCE_RATIO_THRESHOLD) - 1, 10, 1),
        # 10 -> above ratio
        (
            int(10 * MANIFEST_DANCE_RATIO_THRESHOLD) + 1,
            10,
            MANIFEST_INCENTIVE_MULTIPLIER,
        ),
        # above ratio -> 10
        (
            10,
            int(10 * MANIFEST_DANCE_RATIO_THRESHOLD) + 1,
            MANIFEST_INCENTIVE_MULTIPLIER,
        ),
    ],
)
async def test_manifest_dance_incentives(
    curr_online_executor_count,
    prev_online_executor_count,
    expected_multiplier,
    mocked_synthetic_miner_client,
    override_weights_version_v2,
):
    manifest = ExecutorManifest(
        executor_classes=[
            ExecutorClassManifest(
                executor_class=DEFAULT_EXECUTOR_CLASS, count=curr_online_executor_count
            )
        ]
    )
    job_uuids = []

    def manifest_callback(miner_client):
        miner_client.miner_manifest.set_result(manifest)

    def interaction_callback(miner_client):
        for job_uuid in miner_client.job_states:
            job_uuids.append(job_uuid)
            miner_client.job_states[job_uuid].miner_ready_or_declining_future.set_result(
                V0ExecutorReadyRequest(job_uuid=job_uuid)
            )
            miner_client.job_states[job_uuid].miner_finished_or_failed_future.set_result(
                V0JobFinishedRequest(
                    job_uuid=job_uuid,
                    docker_process_stdout="",
                    docker_process_stderr="",
                )
            )

    await miner_synthetic_jobs_scheme(
        mocked_synthetic_miner_client,
        manifest_callback,
        curr_online_executor_count,
        interaction_callback,
        prev_online_executor_count=prev_online_executor_count,
    )

    miner_client = mocked_synthetic_miner_client.instance
    async for job in SyntheticJob.objects.filter(job_uuid__in=job_uuids):
        receipt = miner_client._query_sent_models(
            lambda m: m.payload.job_uuid, V0JobFinishedReceiptRequest
        )[0]
        time_took = receipt.payload.time_took_us / 1_000_000
        assert abs(job.score * time_took - expected_multiplier) < 0.0001


@patch("compute_horde_validator.validator.synthetic_jobs.utils.JOB_LENGTH", 10)
@patch("compute_horde_validator.validator.synthetic_jobs.utils.TIMEOUT_LEEWAY", 1)
@patch("compute_horde_validator.validator.synthetic_jobs.utils.TIMEOUT_MARGIN", 1)
@patch("compute_horde_validator.validator.synthetic_jobs.utils.TIMEOUT_BARRIER", 3)
@patch(
    "compute_horde_validator.validator.synthetic_jobs.generator.current.synthetic_job_generator_factory",
    TimeToookScoreMockSyntheticJobGeneratorFactory(),
)
@pytest.mark.django_db(databases=["default", "default_alias"], transaction=True)
@pytest.mark.parametrize(
    "weights_version_override,expected_multiplier,current_online_executors,previous_online_executors",
    [
        # no effect on v1
        ("override_weights_version_v1", 1, 1, None),
        # basic test for v2
        ("override_weights_version_v2", MANIFEST_INCENTIVE_MULTIPLIER, 1, None),
        # just basic test for previous executors on single current executor
        ("override_weights_version_v2", MANIFEST_INCENTIVE_MULTIPLIER, 1, 100),
    ],
)
def test_create_and_run_sythethic_job_batch(
    weights_version_override,
    settings,
    mocked_synthetic_miner_client,
    request,
    expected_multiplier,
    current_online_executors,
    previous_online_executors,
):
    request.getfixturevalue(weights_version_override)
    miner_hotkey = "miner_hotkey"
    miner = Miner.objects.get_or_create(hotkey=miner_hotkey)[0]

    if previous_online_executors:
        batch = SyntheticJobBatch.objects.create(accepting_results_until=now(), scored=True)
        MinerManifest.objects.create(
            miner=miner,
            batch=batch,
            executor_count=previous_online_executors,
            online_executor_count=previous_online_executors,
        )

    manifest = ExecutorManifest(
        executor_classes=[
            ExecutorClassManifest(
                executor_class=DEFAULT_EXECUTOR_CLASS,
                count=current_online_executors,
            )
        ]
    )

    job_uuids = []

    def manifest_callback(miner_client):
        miner_client.miner_manifest.set_result(manifest)

    def interaction_callback(miner_client):
        for job_uuid in miner_client.job_states:
            job_uuids.append(job_uuid)
            miner_client.job_states[job_uuid].miner_ready_or_declining_future.set_result(
                V0ExecutorReadyRequest(job_uuid=job_uuid)
            )
            miner_client.job_states[job_uuid].miner_finished_or_failed_future.set_result(
                V0JobFinishedRequest(
                    job_uuid=job_uuid,
                    docker_process_stdout="",
                    docker_process_stderr="",
                )
            )

    syntethic_batch_scheme_single_miner(
        settings,
        mocked_synthetic_miner_client,
        manifest_callback,
        current_online_executors,
        interaction_callback,
        miner_hotkey="miner_hotkey",
    )

    miner_client = mocked_synthetic_miner_client.instance
    for job in SyntheticJob.objects.filter(job_uuid__in=job_uuids):
        receipt = miner_client._query_sent_models(
            lambda m: m.payload.job_uuid, V0JobFinishedReceiptRequest
        )[0]
        time_took = receipt.payload.time_took_us / 1_000_000
        assert abs(job.score * time_took - expected_multiplier) < 0.0001
