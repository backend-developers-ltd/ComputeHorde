import uuid
from collections.abc import Callable

import pytest
import pytest_asyncio
from compute_horde.base.output_upload import MultiUpload
from compute_horde.executor_class import DEFAULT_EXECUTOR_CLASS
from compute_horde.miner_client.organic import OrganicJobError, OrganicMinerClient
from compute_horde.mv_protocol import miner_requests
from compute_horde.mv_protocol.validator_requests import BaseValidatorRequest

from compute_horde_validator.validator.cross_validation.prompt_generation import generate_prompts
from compute_horde_validator.validator.models import PromptSeries
from compute_horde_validator.validator.tests.transport import MinerSimulationTransport

pytestmark = [pytest.mark.asyncio, pytest.mark.django_db(transaction=True)]


@pytest_asyncio.fixture
async def transport():
    return MinerSimulationTransport("miner_hotkey")


@pytest.fixture
def job_uuid():
    return uuid.uuid4()


@pytest.fixture
def create_miner_client(transport: MinerSimulationTransport):
    def _create(*args, **kwargs):
        kwargs["transport"] = transport
        return OrganicMinerClient(*args, **kwargs)

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


@pytest.mark.override_config(
    DYNAMIC_PROMPTS_BATCHES_IN_A_SINGLE_GO=3,
    DYNAMIC_NUMBER_OF_PROMPTS_IN_BATCH=99,
)
async def test_generate_prompts(
    transport: MinerSimulationTransport,
    create_miner_client: Callable,
    manifest_message: str,
    executor_ready_message: str,
    accept_job_message: str,
    job_finish_message: str,
    job_uuid: uuid.UUID,
):
    await transport.add_message(manifest_message, send_before=1)
    await transport.add_message(accept_job_message, send_before=1)
    await transport.add_message(executor_ready_message, send_before=0)
    await transport.add_message(job_finish_message, send_before=2)

    await generate_prompts(
        create_miner_client=create_miner_client, job_uuid=job_uuid, wait_timeout=2
    )

    series_uuids = []
    async for series in PromptSeries.objects.all():
        _uuid = str(series.series_uuid)

        assert _uuid in series.s3_url
        # make sure we save the public url
        assert "Signature" not in series.s3_url

        series_uuids.append(_uuid)

    assert len(series_uuids) == 3

    job_request = BaseValidatorRequest.parse(transport.sent[-2])
    assert job_request.job_uuid == str(job_uuid)
    assert isinstance(job_request.output_upload, MultiUpload)

    uuids_from_uploads = []
    for upload in job_request.output_upload.uploads:
        assert upload.relative_path.startswith("prompts_")

        _uuid = upload.relative_path.split("_")[1].split(".")[0]

        assert "Signature" in upload.url
        assert _uuid in upload.url
        uuids_from_uploads.append(_uuid)

    assert set(uuids_from_uploads) == set(series_uuids)

    assert "99" in job_request.docker_run_cmd
    assert ",".join(series_uuids) in job_request.docker_run_cmd


@pytest.mark.override_config(
    DYNAMIC_PROMPTS_BATCHES_IN_A_SINGLE_GO=3,
    DYNAMIC_NUMBER_OF_PROMPTS_IN_BATCH=99,
)
async def test_generate_prompts_job_failed(
    transport: MinerSimulationTransport,
    create_miner_client: Callable,
    manifest_message: str,
    executor_ready_message: str,
    accept_job_message: str,
    job_failed_message: str,
    job_uuid: uuid.UUID,
):
    await transport.add_message(manifest_message, send_before=1)
    await transport.add_message(accept_job_message, send_before=1)
    await transport.add_message(executor_ready_message, send_before=0)
    await transport.add_message(job_failed_message, send_before=2)

    with pytest.raises(OrganicJobError):
        await generate_prompts(
            create_miner_client=create_miner_client, job_uuid=job_uuid, wait_timeout=2
        )

    assert not await PromptSeries.objects.aexists()


@pytest.mark.override_config(
    DYNAMIC_PROMPTS_BATCHES_IN_A_SINGLE_GO=3,
    DYNAMIC_NUMBER_OF_PROMPTS_IN_BATCH=99,
)
async def test_generate_prompts_timeout(
    transport: MinerSimulationTransport,
    create_miner_client: Callable,
    manifest_message: str,
    job_uuid: uuid.UUID,
):
    await transport.add_message(manifest_message, send_before=1)

    with pytest.raises(OrganicJobError):
        await generate_prompts(
            create_miner_client=create_miner_client, job_uuid=job_uuid, wait_timeout=0.5
        )

    assert not await PromptSeries.objects.aexists()
