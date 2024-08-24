import asyncio
import uuid
from unittest.mock import patch

import bittensor
import pytest
import pytest_asyncio
from asgiref.sync import sync_to_async
from compute_horde.executor_class import DEFAULT_EXECUTOR_CLASS
from compute_horde.miner_client.base import AbstractTransport
from compute_horde.mv_protocol import miner_requests
from django.conf import settings
from pytest_mock import MockerFixture

from compute_horde_validator.validator.miner_client import MinerClient
from compute_horde_validator.validator.models import (
    Miner,
    SyntheticJob,
    SyntheticJobBatch,
    SystemEvent,
)
from compute_horde_validator.validator.synthetic_jobs.utils import (
    execute_miner_synthetic_jobs,
)
from compute_horde_validator.validator.tests.mock_generator import (
    MOCK_SCORE,
    NOT_SCORED,
    MockSyntheticJobGeneratorFactory,
)
from compute_horde_validator.validator.tests.transport import MinerSimulationTransport

from .helpers import (
    check_system_events,
)


@pytest.fixture
def miner_hotkey():
    return "miner_hotkey"


@pytest.fixture
def validator_hotkey():
    return "validator_hotkey"


@pytest.fixture
def miner_axon_info(miner_hotkey: str):
    return bittensor.AxonInfo(
        version=4,
        ip="ignore",
        ip_type=4,
        port=9999,
        hotkey=miner_hotkey,
        coldkey=miner_hotkey,
    )


@pytest_asyncio.fixture
async def miner(miner_hotkey: str):
    return await Miner.objects.acreate(hotkey=miner_hotkey)


@pytest_asyncio.fixture
async def batch():
    return await SyntheticJobBatch.objects.acreate(
        started_at="2021-09-01 00:00:00",
        accepting_results_until="2021-09-01 00:00:00",
    )


@pytest.fixture
def keypair():
    return settings.BITTENSOR_WALLET().get_hotkey()


@pytest.fixture
def job_uuid():
    return uuid.uuid4()


@pytest.fixture
def manifest_message():
    return miner_requests.V0ExecutorManifestRequest(
        manifest=miner_requests.ExecutorManifest(
            executor_classes=[
                miner_requests.ExecutorClassManifest(executor_class=DEFAULT_EXECUTOR_CLASS, count=1)
            ]
        )
    ).model_dump_json()


@pytest.fixture
def executor_ready_message(job_uuid: uuid.UUID):
    return miner_requests.V0ExecutorReadyRequest(job_uuid=str(job_uuid)).model_dump_json()


@pytest.fixture
def accept_job_message(job_uuid: uuid.UUID):
    return miner_requests.V0AcceptJobRequest(job_uuid=str(job_uuid)).model_dump_json()


@pytest.fixture
def decline_job_message(job_uuid: uuid.UUID):
    return miner_requests.V0DeclineJobRequest(job_uuid=str(job_uuid)).model_dump_json()


@pytest.fixture
def docker_process_stdout():
    return "stdout"


@pytest.fixture
def docker_process_stderr():
    return "stderr"


@pytest.fixture
def job_finish_message(job_uuid: uuid.UUID, docker_process_stdout: str, docker_process_stderr: str):
    return miner_requests.V0JobFinishedRequest(
        job_uuid=str(job_uuid),
        docker_process_stdout=docker_process_stdout,
        docker_process_stderr=docker_process_stderr,
    ).model_dump_json()


@pytest.fixture
def job_failed_message(job_uuid: uuid.UUID, docker_process_stdout: str, docker_process_stderr: str):
    return miner_requests.V0JobFailedRequest(
        job_uuid=str(job_uuid),
        docker_process_exit_status=1,
        docker_process_stdout=docker_process_stdout,
        docker_process_stderr=docker_process_stderr,
    ).model_dump_json()


@pytest_asyncio.fixture
async def transport(miner_hotkey: str):
    return MinerSimulationTransport(miner_hotkey)


@pytest_asyncio.fixture
async def miner_client(
    miner_hotkey: str, validator_hotkey: str, keypair, job_uuid: str, transport: AbstractTransport
):
    return MinerClient(
        miner_address="ignore",
        my_hotkey=validator_hotkey,
        miner_hotkey=miner_hotkey,
        miner_port=9999,
        job_uuid=job_uuid,
        batch_id=None,
        keypair=keypair,
        transport=transport,
    )


@pytest.fixture(autouse=True)
def _patch_generator_factory(mocker: MockerFixture):
    mocker.patch(
        "compute_horde_validator.validator.synthetic_jobs.generator.current.synthetic_job_generator_factory",
        MockSyntheticJobGeneratorFactory(),
    )


@pytest.mark.asyncio
@pytest.mark.django_db(transaction=True)
async def test_execute_miner_synthetic_jobs_success(
    miner: Miner,
    batch: SyntheticJobBatch,
    miner_hotkey: str,
    miner_axon_info: bittensor.AxonInfo,
    miner_client: MinerClient,
    manifest_message: str,
    executor_ready_message: str,
    accept_job_message: str,
    job_finish_message: str,
    transport: MinerSimulationTransport,
    job_uuid: uuid.UUID,
):
    await transport.add_message(manifest_message, send_before=1)
    await transport.add_message(executor_ready_message, send_before=1)
    await transport.add_message(accept_job_message, send_before=2)
    await transport.add_message(job_finish_message, send_before=0)

    await asyncio.wait_for(
        execute_miner_synthetic_jobs(
            batch.pk,
            miner.pk,
            miner_hotkey,
            miner_axon_info,
            None,
            miner_client,
            generate_job_uuid=lambda: job_uuid,
        ),
        timeout=1,
    )

    await check_synthetic_job(job_uuid, miner, SyntheticJob.Status.COMPLETED, MOCK_SCORE)
    await sync_to_async(check_system_events)(
        SystemEvent.EventType.MINER_SYNTHETIC_JOB_SUCCESS, SystemEvent.EventSubType.SUCCESS
    )


@pytest.mark.asyncio
@pytest.mark.django_db(transaction=True)
@patch("compute_horde_validator.validator.synthetic_jobs.utils.TIMEOUT_LEEWAY", 0.05)
@patch("compute_horde_validator.validator.synthetic_jobs.utils.TIMEOUT_MARGIN", 0.05)
async def test_execute_miner_synthetic_jobs_success_timeout(
    miner: Miner,
    batch: SyntheticJobBatch,
    miner_hotkey: str,
    miner_axon_info: bittensor.AxonInfo,
    miner_client: MinerClient,
    manifest_message: str,
    executor_ready_message: str,
    accept_job_message: str,
    job_finish_message: str,
    transport: MinerSimulationTransport,
    job_uuid: uuid.UUID,
):
    await transport.add_message(manifest_message, send_before=1)
    await transport.add_message(executor_ready_message, send_before=1)
    await transport.add_message(accept_job_message, send_before=2)
    await transport.add_message(job_finish_message, send_before=0, sleep_before=2)

    await asyncio.wait_for(
        execute_miner_synthetic_jobs(
            batch.pk,
            miner.pk,
            miner_hotkey,
            miner_axon_info,
            None,
            miner_client,
            generate_job_uuid=lambda: job_uuid,
        ),
        timeout=2,
    )

    await check_synthetic_job(job_uuid, miner, SyntheticJob.Status.FAILED, NOT_SCORED)
    await sync_to_async(check_system_events)(
        SystemEvent.EventType.MINER_SYNTHETIC_JOB_FAILURE,
        SystemEvent.EventSubType.JOB_EXECUTION_TIMEOUT,
    )


@pytest.mark.asyncio
@pytest.mark.django_db(transaction=True)
async def test_execute_miner_synthetic_jobs_job_failed(
    miner: Miner,
    batch: SyntheticJobBatch,
    miner_hotkey: str,
    miner_axon_info: bittensor.AxonInfo,
    miner_client: MinerClient,
    manifest_message: str,
    executor_ready_message: str,
    accept_job_message: str,
    job_failed_message: str,
    transport: MinerSimulationTransport,
    job_uuid: uuid.UUID,
):
    await transport.add_message(manifest_message, send_before=1)
    await transport.add_message(executor_ready_message, send_before=1)
    await transport.add_message(accept_job_message, send_before=2)
    await transport.add_message(job_failed_message, send_before=0)

    await asyncio.wait_for(
        execute_miner_synthetic_jobs(
            batch.pk,
            miner.pk,
            miner_hotkey,
            miner_axon_info,
            None,
            miner_client,
            generate_job_uuid=lambda: job_uuid,
        ),
        timeout=1,
    )

    await check_synthetic_job(job_uuid, miner, SyntheticJob.Status.FAILED, NOT_SCORED)
    await sync_to_async(check_system_events)(
        SystemEvent.EventType.MINER_SYNTHETIC_JOB_FAILURE, SystemEvent.EventSubType.FAILURE
    )


@pytest.mark.asyncio
@pytest.mark.django_db(transaction=True)
async def test_execute_miner_synthetic_jobs_job_declined(
    miner: Miner,
    batch: SyntheticJobBatch,
    miner_hotkey: str,
    miner_axon_info: bittensor.AxonInfo,
    miner_client: MinerClient,
    manifest_message: str,
    decline_job_message: str,
    transport: MinerSimulationTransport,
    job_uuid: uuid.UUID,
):
    await transport.add_message(manifest_message, send_before=1)
    await transport.add_message(decline_job_message, send_before=1)

    await asyncio.wait_for(
        execute_miner_synthetic_jobs(
            batch.pk,
            miner.pk,
            miner_hotkey,
            miner_axon_info,
            None,
            miner_client,
            generate_job_uuid=lambda: job_uuid,
        ),
        timeout=1,
    )

    await check_synthetic_job(job_uuid, miner, SyntheticJob.Status.FAILED, NOT_SCORED)
    await sync_to_async(check_system_events)(
        SystemEvent.EventType.MINER_SYNTHETIC_JOB_FAILURE, SystemEvent.EventSubType.JOB_NOT_STARTED
    )


@pytest.mark.asyncio
@pytest.mark.django_db(transaction=True)
@patch("compute_horde_validator.validator.synthetic_jobs.utils.MANIFEST_TIMEOUT", 0.2)
async def test_execute_miner_synthetic_jobs_no_manifest(
    miner: Miner,
    batch: SyntheticJobBatch,
    miner_hotkey: str,
    miner_axon_info: bittensor.AxonInfo,
    miner_client: MinerClient,
    job_uuid: uuid.UUID,
):
    await asyncio.wait_for(
        execute_miner_synthetic_jobs(
            batch.pk,
            miner.pk,
            miner_hotkey,
            miner_axon_info,
            None,
            miner_client,
            generate_job_uuid=lambda: job_uuid,
        ),
        timeout=1,
    )

    assert not await SyntheticJob.objects.aexists()
    await sync_to_async(check_system_events)(
        SystemEvent.EventType.MINER_SYNTHETIC_JOB_FAILURE, SystemEvent.EventSubType.MANIFEST_ERROR
    )


async def check_synthetic_job(job_uuid: uuid.UUID, miner: Miner, status: str, score: float):
    job = await SyntheticJob.objects.aget()
    assert job.job_uuid == job_uuid
    assert job.miner_id == miner.pk
    assert job.status == status
    assert job.score == score
