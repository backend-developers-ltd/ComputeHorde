import uuid

import pytest
import pytest_asyncio
from compute_horde.executor_class import DEFAULT_EXECUTOR_CLASS, DEFAULT_LLM_EXECUTOR_CLASS
from compute_horde.miner_client.base import AbstractTransport
from compute_horde.protocol_messages import (
    V0AcceptJobRequest,
    V0DeclineJobRequest,
    V0ExecutorReadyRequest,
    V0JobFailedRequest,
    V0JobFinishedRequest,
)
from django.utils.timezone import now
from pytest_mock import MockerFixture

from compute_horde_validator.validator.models import (
    Miner,
    Prompt,
    PromptSample,
    PromptSeries,
    SolveWorkload,
)
from compute_horde_validator.validator.synthetic_jobs.batch_run import BatchContext, MinerClient
from compute_horde_validator.validator.synthetic_jobs.generator.llm_prompts import (
    LlmPromptsSyntheticJobGenerator,
)
from compute_horde_validator.validator.tests.transport import SimulationTransport

from .mock_generator import MockSyntheticJobGeneratorFactory


@pytest.fixture
def miner_hotkey():
    return "miner_hotkey"


@pytest_asyncio.fixture
async def miner(miner_hotkey: str):
    return await Miner.objects.acreate(
        hotkey=miner_hotkey,
        address="ignore",
        ip_version=4,
        port=9999,
    )


@pytest_asyncio.fixture
async def transport(miner_hotkey: str):
    return SimulationTransport(miner_hotkey)


@pytest.fixture
def create_simulation_miner_client(transport: AbstractTransport):
    def _create(ctx: BatchContext, miner_hotkey: str):
        return MinerClient(ctx=ctx, miner_hotkey=miner_hotkey, transport=transport)

    return _create


@pytest.fixture
def job_uuid():
    return uuid.uuid4()


@pytest.fixture
def job_uuids(job_uuid: uuid.UUID):
    return [job_uuid]


@pytest.fixture
def job_generator_factory(job_uuids: list[uuid.UUID]):
    return MockSyntheticJobGeneratorFactory(uuids=job_uuids)


@pytest.fixture(autouse=True)
def _patch_get_streaming_job_executor_classes(mocker: MockerFixture):
    mocker.patch(
        "compute_horde_validator.validator.synthetic_jobs.batch_run.get_streaming_job_executor_classes",
        return_value={},
    )


@pytest.fixture(autouse=True)
def _patch_generator_factory(
    mocker: MockerFixture, job_generator_factory: MockSyntheticJobGeneratorFactory
):
    mocker.patch(
        "compute_horde_validator.validator.synthetic_jobs.generator.current.synthetic_job_generator_factory",
        job_generator_factory,
    )


@pytest.fixture
def manifest_message():
    return {DEFAULT_EXECUTOR_CLASS: 1}


@pytest.fixture
def streaming_manifest_message():
    return {DEFAULT_LLM_EXECUTOR_CLASS: 1}


@pytest.fixture
def executor_ready_message(job_uuid: uuid.UUID):
    return V0ExecutorReadyRequest(job_uuid=str(job_uuid)).model_dump_json()


@pytest.fixture
def accept_job_message(job_uuid: uuid.UUID):
    return V0AcceptJobRequest(job_uuid=str(job_uuid)).model_dump_json()


@pytest.fixture
def decline_job_message(job_uuid: uuid.UUID):
    return V0DeclineJobRequest(job_uuid=str(job_uuid)).model_dump_json()


@pytest.fixture
def docker_process_stdout():
    return "stdout"


@pytest.fixture
def docker_process_stderr():
    return "stderr"


@pytest.fixture
def job_finish_message(job_uuid: uuid.UUID, docker_process_stdout: str, docker_process_stderr: str):
    return V0JobFinishedRequest(
        job_uuid=str(job_uuid),
        docker_process_stdout=docker_process_stdout,
        docker_process_stderr=docker_process_stderr,
        artifacts={},
    ).model_dump_json()


@pytest.fixture
def job_failed_message(job_uuid: uuid.UUID, docker_process_stdout: str, docker_process_stderr: str):
    return V0JobFailedRequest(
        job_uuid=str(job_uuid),
        docker_process_exit_status=1,
        docker_process_stdout=docker_process_stdout,
        docker_process_stderr=docker_process_stderr,
    ).model_dump_json()


@pytest_asyncio.fixture
async def prompt_series():
    return await PromptSeries.objects.acreate(
        series_uuid=uuid.uuid4(),
        s3_url="http://localhost:9999/prompt-series-download-url",
        generator_version=0,
    )


@pytest_asyncio.fixture
async def solve_workload():
    return await SolveWorkload.objects.acreate(
        workload_uuid=uuid.uuid4(),
        seed=42,
        s3_url="http://localhost:9999/solve-workload-download-url",
        finished_at=now(),
    )


@pytest_asyncio.fixture
async def prompt_sample(prompt_series, solve_workload):
    return await PromptSample.objects.acreate(
        series=prompt_series,
        workload=solve_workload,
        synthetic_job=None,
    )


@pytest_asyncio.fixture
async def prompts(prompt_sample):
    return await Prompt.objects.abulk_create(
        [
            Prompt(
                sample=prompt_sample,
                content=str(i),
                answer=str(i),
            )
            for i in range(10)
        ]
    )


@pytest_asyncio.fixture
async def llm_prompts_job_generator(
    prompt_series: PromptSeries,
    solve_workload: SolveWorkload,
    prompt_sample: PromptSample,
    prompts: list[Prompt],
) -> LlmPromptsSyntheticJobGenerator:
    job_generator = LlmPromptsSyntheticJobGenerator(
        prompt_sample=prompt_sample,
        expected_prompts=prompts,
        s3_url=prompt_series.s3_url,
        seed=solve_workload.seed,
    )
    await job_generator.ainit(miner_hotkey="test-dummy-hotkey")
    return job_generator
