import uuid
from unittest.mock import MagicMock, patch

import pytest
from asgiref.sync import sync_to_async
from compute_horde.miner_client.base import BaseRequest
from compute_horde.mv_protocol.miner_requests import (
    V0DeclineJobRequest,
    V0ExecutorFailedRequest,
    V0ExecutorReadyRequest,
    V0JobFailedRequest,
    V0JobFinishedRequest,
)
from django.conf import settings

from compute_horde_validator.validator.models import (
    Miner,
    MinerManifest,
    SyntheticJob,
    SyntheticJobBatch,
    SystemEvent,
)
from compute_horde_validator.validator.synthetic_jobs.generator.base import (
    AbstractSyntheticJobGenerator,
)
from compute_horde_validator.validator.synthetic_jobs.utils import (
    MinerClient,
    apply_manifest_incentive,
    execute_synthetic_job,
)

from .helpers import check_system_events
from .test_miner_driver import MockMinerClient, get_miner_client

MOCK_SCORE = 0.8
MANIFEST_INCENTIVE_APPLIED_SCORE = MOCK_SCORE * settings.MANIFEST_INCENTIVE_MULTIPLIER
NOT_SCORED = 0.0


class MockSyntheticJobGenerator(AbstractSyntheticJobGenerator):
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


mock_synthetic_job_generator = MagicMock(name="MockSyntheticJobGenerator")


@patch("compute_horde_validator.validator.synthetic_jobs.utils.JOB_LENGTH", 0.1)
@patch("compute_horde_validator.validator.synthetic_jobs.utils.TIMEOUT_LEEWAY", 0.1)
@patch("compute_horde_validator.validator.synthetic_jobs.utils.TIMEOUT_MARGIN", 0.1)
@patch(
    "compute_horde_validator.validator.synthetic_jobs.generator.current.SyntheticJobGenerator",
    MockSyntheticJobGenerator,
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
            MANIFEST_INCENTIVE_APPLIED_SCORE,
            (
                SystemEvent.EventType.MINER_SYNTHETIC_JOB_SUCCESS,
                SystemEvent.EventSubType.SUCCESS,
                1,
            ),
        ),
    ],
)
async def test_execute_synthetic_job(
    futures_result, expected_job_status, expected_score, expected_system_event
):
    miner, _ = await Miner.objects.aget_or_create(hotkey="miner_hotkey")

    batch = await SyntheticJobBatch.objects.acreate(
        started_at="2021-09-01 00:00:00",
        accepting_results_until="2021-09-01 00:00:00",
    )

    job = await SyntheticJob.objects.acreate(
        batch_id=batch.pk,
        miner_id=miner.pk,
        miner_address="ignore",
        miner_address_ip_version=4,
        miner_port=9999,
        status=SyntheticJob.Status.PENDING,
    )
    job_uuid = str(job.job_uuid)
    miner_client = get_miner_client(MockMinerClient, job_uuid)
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

    await execute_synthetic_job(miner_client, job.pk)
    job = await SyntheticJob.objects.aget(pk=job.pk)
    assert job.score == expected_score
    assert job.status == expected_job_status

    if expected_system_event:
        await sync_to_async(check_system_events)(*expected_system_event)
    else:
        assert await SystemEvent.objects.get() == 0


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


@pytest.mark.asyncio
@pytest.mark.django_db(databases=["default", "default_alias"], transaction=True)
async def test_manifest_incentive__no_recent_manifests():
    miner, _ = await Miner.objects.aget_or_create(hotkey="miner_hotkey")
    batches = await create_mock_job_batches(miner)

    await MinerManifest.objects.acreate(miner=miner, batch=batches[-1], executor_count=10)

    new_score = await apply_manifest_incentive(
        miner_hotkey=miner.hotkey, batch_id=batches[-1].pk, score=MOCK_SCORE
    )

    assert new_score == MANIFEST_INCENTIVE_APPLIED_SCORE


@pytest.mark.asyncio
@pytest.mark.django_db(databases=["default", "default_alias"], transaction=True)
async def test_manifest_incentive__num_executors_increased():
    miner, _ = await Miner.objects.aget_or_create(hotkey="miner_hotkey")
    batches = await create_mock_job_batches(miner)

    for batch in batches[:-1]:
        await MinerManifest.objects.acreate(miner=miner, batch=batch, executor_count=8)
    await MinerManifest.objects.acreate(miner=miner, batch=batches[-1], executor_count=16)

    new_score = await apply_manifest_incentive(
        miner_hotkey=miner.hotkey, batch_id=batches[-1].pk, score=MOCK_SCORE
    )

    assert new_score == MANIFEST_INCENTIVE_APPLIED_SCORE


@pytest.mark.asyncio
@pytest.mark.django_db(databases=["default", "default_alias"], transaction=True)
async def test_manifest_incentive__not_applying():
    miner, _ = await Miner.objects.aget_or_create(hotkey="miner_hotkey")
    batches = await create_mock_job_batches(miner)

    for batch in batches:
        await MinerManifest.objects.acreate(miner=miner, batch=batch, executor_count=8)

    new_score = await apply_manifest_incentive(
        miner_hotkey=miner.hotkey, batch_id=batches[-1].pk, score=MOCK_SCORE
    )

    assert new_score == MOCK_SCORE


@pytest.mark.asyncio
@pytest.mark.django_db(databases=["default", "default_alias"], transaction=True)
async def test_manifest_incentive__recently_registered():
    miner, _ = await Miner.objects.aget_or_create(hotkey="miner_hotkey")
    batches = await create_mock_job_batches(miner)

    for batch in batches[-2:]:
        await MinerManifest.objects.acreate(miner=miner, batch=batch, executor_count=8)

    new_score = await apply_manifest_incentive(
        miner_hotkey=miner.hotkey, batch_id=batches[-1].pk, score=MOCK_SCORE
    )

    assert new_score == MANIFEST_INCENTIVE_APPLIED_SCORE


@pytest.mark.asyncio
@pytest.mark.django_db(databases=["default", "default_alias"], transaction=True)
async def test_manifest_incentive__few_executors():
    miner, _ = await Miner.objects.aget_or_create(hotkey="miner_hotkey")
    batches = await create_mock_job_batches(miner)

    for batch in batches:
        await MinerManifest.objects.acreate(miner=miner, batch=batch, executor_count=3)

    new_score = await apply_manifest_incentive(
        miner_hotkey=miner.hotkey, batch_id=batches[-1].pk, score=MOCK_SCORE
    )

    assert new_score == MANIFEST_INCENTIVE_APPLIED_SCORE
