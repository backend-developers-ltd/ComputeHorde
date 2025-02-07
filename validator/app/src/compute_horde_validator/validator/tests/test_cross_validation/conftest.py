import uuid

import pytest
import pytest_asyncio
from compute_horde.executor_class import DEFAULT_EXECUTOR_CLASS
from compute_horde.mv_protocol import miner_requests

from compute_horde_validator.validator.cross_validation.utils import (
    TrustedMinerClient,
)
from compute_horde_validator.validator.tests.transport import SimulationTransport


@pytest_asyncio.fixture
async def transport():
    return SimulationTransport("miner_hotkey")


@pytest.fixture
def job_uuid():
    return uuid.uuid4()


@pytest.fixture
def create_miner_client(transport: SimulationTransport):
    def _create(*args, **kwargs):
        kwargs["transport"] = transport
        return TrustedMinerClient(*args, **kwargs)

    return _create


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
def job_finish_message(job_uuid: uuid.UUID):
    return miner_requests.V0JobFinishedRequest(
        job_uuid=str(job_uuid),
        docker_process_stdout="",
        docker_process_stderr="",
    ).model_dump_json()


@pytest.fixture
def job_failed_message(job_uuid: uuid.UUID):
    return miner_requests.V0JobFailedRequest(
        job_uuid=str(job_uuid),
        docker_process_stdout="",
        docker_process_stderr="",
    ).model_dump_json()
