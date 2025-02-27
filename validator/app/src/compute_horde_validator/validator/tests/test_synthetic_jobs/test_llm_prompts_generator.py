import pytest
from compute_horde.mv_protocol.miner_requests import V0JobFinishedRequest
from compute_horde_core.output_upload import MultiUpload, SingleFilePutUpload
from compute_horde_core.volume import MultiVolume, SingleFileVolume
from pytest_httpx import HTTPXMock

from compute_horde_validator.validator.synthetic_jobs.generator.llm_prompts import (
    LlmPromptsSyntheticJobGenerator,
)

_JOB_FINISHED_REQUEST = V0JobFinishedRequest(
    job_uuid="CF8753B2-C86C-45A3-A01F-84295C3BAD8F",
    docker_process_stdout="",
    docker_process_stderr="",
    artifacts={},
)


@pytest.mark.override_config(
    DYNAMIC_SYNTHETIC_STREAMING_JOB_READY_TIMEOUT="",
)
@pytest.mark.asyncio
@pytest.mark.django_db(databases=["default", "default_alias"], transaction=True)
async def test_llm_prompts_generator_basic(
    httpx_mock: HTTPXMock,
    llm_prompts_job_generator: LlmPromptsSyntheticJobGenerator,
):
    httpx_mock.add_response(json={str(i): str(i) for i in range(240)})

    volume = await llm_prompts_job_generator.volume()
    assert isinstance(volume, MultiVolume)
    assert len(volume.volumes) == 1
    assert isinstance(volume.volumes[0], SingleFileVolume)

    output_upload = await llm_prompts_job_generator.output_upload()
    assert isinstance(output_upload, MultiUpload)
    assert len(output_upload.uploads) == 1
    assert isinstance(output_upload.uploads[0], SingleFilePutUpload)

    # before downloading answers
    correct, _, score = llm_prompts_job_generator.verify(_JOB_FINISHED_REQUEST, 0)
    assert not correct
    assert score == 0.0

    await llm_prompts_job_generator.download_answers()
    correct, _, score = llm_prompts_job_generator.verify(_JOB_FINISHED_REQUEST, 0)
    assert correct
    assert score == 1.0


@pytest.mark.asyncio
@pytest.mark.django_db(databases=["default", "default_alias"], transaction=True)
async def test_llm_prompts_generator_missing_prompts(
    httpx_mock: HTTPXMock,
    llm_prompts_job_generator: LlmPromptsSyntheticJobGenerator,
):
    httpx_mock.add_response(json={str(i): str(i) for i in range(9, 249)})

    await llm_prompts_job_generator.download_answers()
    correct, _, score = llm_prompts_job_generator.verify(_JOB_FINISHED_REQUEST, 0)
    assert not correct
    assert score == 0.0


@pytest.mark.asyncio
@pytest.mark.django_db(databases=["default", "default_alias"], transaction=True)
async def test_llm_prompts_generator_wrong_answers(
    httpx_mock: HTTPXMock,
    llm_prompts_job_generator: LlmPromptsSyntheticJobGenerator,
):
    httpx_mock.add_response(json={str(i): "wrong" for i in range(240)})

    await llm_prompts_job_generator.download_answers()
    correct, _, score = llm_prompts_job_generator.verify(_JOB_FINISHED_REQUEST, 0)
    assert not correct
    assert score == 0.0
