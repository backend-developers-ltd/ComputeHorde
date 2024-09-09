import uuid

import pytest
from compute_horde.base.output_upload import SingleFilePutUpload
from compute_horde.base.volume import SingleFileVolume
from django.utils.timezone import now
from pytest_httpx import HTTPXMock

from compute_horde_validator.validator.models import (
    Prompt,
    PromptSample,
    PromptSeries,
    SolveWorkload,
)
from compute_horde_validator.validator.synthetic_jobs.generator.llama_prompts import (
    LlamaPromptsSyntheticJobGenerator,
)


async def _prepare_models():
    prompt_series = await PromptSeries.objects.acreate(
        series_uuid=uuid.uuid4(),
        s3_url="prompt-series-download-url",
    )
    solve_workload = await SolveWorkload.objects.acreate(
        workload_uuid=uuid.uuid4(),
        seed=42,
        s3_url="",
        finished_at=now(),
    )
    prompt_sample = await PromptSample.objects.acreate(
        series=prompt_series,
        workload=solve_workload,
        synthetic_job=None,
    )
    await Prompt.objects.abulk_create(
        [
            Prompt(
                sample=prompt_sample,
                content=str(i),
                answer=str(i),
            )
            for i in range(10)
        ]
    )

    # reload prompt sample with related objects
    prompt_samples = (
        PromptSample.objects.select_related("series", "workload")
        .prefetch_related("prompts")
        .filter(
            synthetic_job__isnull=True,
            workload__finished_at__isnull=False,
        )[:1]
    )
    prompt_sample = [ps async for ps in prompt_samples][0]
    return prompt_sample


@pytest.mark.asyncio
@pytest.mark.django_db(databases=["default", "default_alias"], transaction=True)
async def test_llama_prompts_generator_basic(httpx_mock: HTTPXMock):
    prompt_sample = await _prepare_models()
    httpx_mock.add_response(json=[{"prompt": str(i), "answer": str(i)} for i in range(240)])

    job_generator = LlamaPromptsSyntheticJobGenerator(prompt_sample)
    await job_generator.ainit()

    volume = await job_generator.volume()
    assert isinstance(volume, SingleFileVolume)

    output_upload = await job_generator.output_upload()
    assert isinstance(output_upload, SingleFilePutUpload)

    with pytest.raises(RuntimeError):
        assert job_generator.verify(None, 0) == (True, "", 1)

    await job_generator._download_answers()
    correct, _, score = job_generator.verify(None, 0)
    assert correct
    assert score == 1.0


@pytest.mark.asyncio
@pytest.mark.django_db(databases=["default", "default_alias"], transaction=True)
async def test_llama_prompts_generator_missing_prompts(httpx_mock: HTTPXMock):
    prompt_sample = await _prepare_models()
    httpx_mock.add_response(json=[{"prompt": str(i), "answer": str(i)} for i in range(9, 249)])

    job_generator = LlamaPromptsSyntheticJobGenerator(prompt_sample)
    await job_generator.ainit()

    await job_generator._download_answers()
    correct, _, score = job_generator.verify(None, 0)
    assert not correct
    assert score == 0.0


@pytest.mark.asyncio
@pytest.mark.django_db(databases=["default", "default_alias"], transaction=True)
async def test_llama_prompts_generator_wrong_answers(httpx_mock: HTTPXMock):
    prompt_sample = await _prepare_models()
    httpx_mock.add_response(json=[{"prompt": str(i), "answer": "wrong"} for i in range(240)])

    job_generator = LlamaPromptsSyntheticJobGenerator(prompt_sample)
    await job_generator.ainit()

    await job_generator._download_answers()
    correct, _, score = job_generator.verify(None, 0)
    assert not correct
    assert score == 0.0
