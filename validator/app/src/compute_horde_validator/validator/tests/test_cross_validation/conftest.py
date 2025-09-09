import uuid

import pytest
import pytest_asyncio
from compute_horde.protocol_messages import (
    V0AcceptJobRequest,
    V0ExecutionDoneRequest,
    V0ExecutorReadyRequest,
    V0JobFailedRequest,
    V0JobFinishedRequest,
    V0VolumesReadyRequest,
)

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
def executor_ready_message(job_uuid: uuid.UUID):
    return V0ExecutorReadyRequest(job_uuid=str(job_uuid)).model_dump_json()


@pytest.fixture
def volumes_ready_message(job_uuid: uuid.UUID):
    return V0VolumesReadyRequest(job_uuid=str(job_uuid)).model_dump_json()


@pytest.fixture
def execution_done_message(job_uuid: uuid.UUID):
    return V0ExecutionDoneRequest(job_uuid=str(job_uuid)).model_dump_json()


@pytest.fixture
def accept_job_message(job_uuid: uuid.UUID):
    return V0AcceptJobRequest(job_uuid=str(job_uuid)).model_dump_json()


@pytest.fixture
def job_finish_message(job_uuid: uuid.UUID):
    return V0JobFinishedRequest(
        job_uuid=str(job_uuid),
        docker_process_stdout="",
        docker_process_stderr="",
        artifacts={},
        upload_results={
            "output.zip": '{"headers": {"Content-Length": "123", "ETag": "abc123"}, "body": "response body content"}'
        },
    ).model_dump_json()


@pytest.fixture
def job_failed_message(job_uuid: uuid.UUID):
    return V0JobFailedRequest(
        job_uuid=str(job_uuid),
        docker_process_stdout="",
        docker_process_stderr="",
    ).model_dump_json()
