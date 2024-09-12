import pytest
from compute_horde.base.output_upload import MultiUpload, SingleFilePutUpload
from compute_horde.base.volume import MultiVolume, SingleFileVolume
from compute_horde.mv_protocol.miner_requests import V0JobFinishedRequest
from pytest_httpx import HTTPXMock

from compute_horde_validator.validator.models import (
    PromptSample,
)
from compute_horde_validator.validator.synthetic_jobs.generator.llama_prompts import (
    LlamaPromptsSyntheticJobGenerator,
)

_JOB_FINISHED_REQUEST = V0JobFinishedRequest(
    job_uuid="CF8753B2-C86C-45A3-A01F-84295C3BAD8F",
    docker_process_stdout="",
    docker_process_stderr="",
)


@pytest.mark.asyncio
@pytest.mark.django_db(databases=["default", "default_alias"], transaction=True)
async def test_llama_prompts_generator_basic(
    httpx_mock: HTTPXMock,
    prompt_sample_prefetched: PromptSample,
):
    httpx_mock.add_response(json={str(i): str(i) for i in range(240)})

    job_generator = LlamaPromptsSyntheticJobGenerator(prompt_sample_prefetched)
    await job_generator.ainit()

    volume = await job_generator.volume()
    assert isinstance(volume, MultiVolume)
    assert len(volume.volumes) == 1
    assert isinstance(volume.volumes[0], SingleFileVolume)

    output_upload = await job_generator.output_upload()
    assert isinstance(output_upload, MultiUpload)
    assert len(output_upload.uploads) == 1
    assert isinstance(output_upload.uploads[0], SingleFilePutUpload)

    # before downloading answers
    correct, _, score = job_generator.verify(_JOB_FINISHED_REQUEST, 0)
    assert not correct
    assert score == 0.0

    await job_generator._download_answers()
    correct, _, score = job_generator.verify(_JOB_FINISHED_REQUEST, 0)
    assert correct
    assert score == 1.0


@pytest.mark.asyncio
@pytest.mark.django_db(databases=["default", "default_alias"], transaction=True)
async def test_llama_prompts_generator_missing_prompts(
    httpx_mock: HTTPXMock,
    prompt_sample_prefetched: PromptSample,
):
    httpx_mock.add_response(json={str(i): str(i) for i in range(9, 249)})

    job_generator = LlamaPromptsSyntheticJobGenerator(prompt_sample_prefetched)
    await job_generator.ainit()

    await job_generator._download_answers()
    correct, _, score = job_generator.verify(_JOB_FINISHED_REQUEST, 0)
    assert not correct
    assert score == 0.0


@pytest.mark.asyncio
@pytest.mark.django_db(databases=["default", "default_alias"], transaction=True)
async def test_llama_prompts_generator_wrong_answers(
    httpx_mock: HTTPXMock,
    prompt_sample_prefetched: PromptSample,
):
    httpx_mock.add_response(json={str(i): "wrong" for i in range(240)})

    job_generator = LlamaPromptsSyntheticJobGenerator(prompt_sample_prefetched)
    await job_generator.ainit()

    await job_generator._download_answers()
    correct, _, score = job_generator.verify(_JOB_FINISHED_REQUEST, 0)
    assert not correct
    assert score == 0.0
