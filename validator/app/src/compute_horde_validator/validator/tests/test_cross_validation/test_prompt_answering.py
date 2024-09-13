import json
import uuid
from collections.abc import Callable
from unittest.mock import patch

import pytest
from compute_horde.miner_client.organic import OrganicJobError

from compute_horde_validator.validator.models import (
    Prompt,
    PromptSample,
    PromptSeries,
    SolveWorkload,
)
from compute_horde_validator.validator.tasks import answer_prompts
from compute_horde_validator.validator.tests.transport import MinerSimulationTransport

pytestmark = [
    pytest.mark.asyncio,
    pytest.mark.django_db(transaction=True),
    pytest.mark.override_config(
        DYNAMIC_MAX_PROMPT_BATCHES=5,
        DYNAMIC_PROMPTS_BATCHES_IN_A_SINGLE_GO=3,
        DYNAMIC_NUMBER_OF_PROMPTS_IN_BATCH=99,
    ),
]


async def db_setup():
    workload = await SolveWorkload.objects.acreate(seed=0, s3_url="s3://test")
    prompt_series = await PromptSeries.objects.acreate(
        s3_url="s3://test",
        generator_version=1,
    )
    prompt_sample = await PromptSample.objects.acreate(
        series=prompt_series,
        workload=workload,
    )
    prompts = await Prompt.objects.abulk_create(
        [
            Prompt(sample=prompt_sample, content="prompt1"),
            Prompt(sample=prompt_sample, content="prompt2"),
            Prompt(sample=prompt_sample, content="prompt3"),
        ]
    )
    return prompts, workload


async def mock_download_json(*args, **kwargs):
    return json.dumps({f"prompt{i}": f"answer{i}" for i in range(1, 4)})


async def mock_throw_error(*args, **kwargs):
    raise Exception("Download failed")


@patch(
    "compute_horde_validator.validator.synthetic_jobs.generator.llama_prompts.download_json",
    mock_download_json,
)
async def test_answer_prompts(
    settings,
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

    prompts, workload = await db_setup()

    await answer_prompts(create_miner_client=create_miner_client, job_uuid=job_uuid, wait_timeout=2)

    await workload.arefresh_from_db()
    assert workload.finished_at is not None

    for i, prompt in enumerate(prompts):
        await prompt.arefresh_from_db()
        assert prompt.answer == f"answer{i + 1}"


async def test_answer_prompts_job_failed(
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

    prompts, workload = await db_setup()

    with pytest.raises(OrganicJobError):
        await answer_prompts(
            create_miner_client=create_miner_client, job_uuid=job_uuid, wait_timeout=2
        )

    await workload.arefresh_from_db()
    assert workload.finished_at is None

    for prompt in prompts:
        await prompt.arefresh_from_db()
        assert prompt.answer is None


@patch(
    "compute_horde_validator.validator.synthetic_jobs.generator.llama_prompts.download_json",
    mock_throw_error,
)
async def test_answer_prompts_download_failed(
    settings,
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

    prompts, workload = await db_setup()

    await answer_prompts(create_miner_client=create_miner_client, job_uuid=job_uuid, wait_timeout=2)

    await workload.arefresh_from_db()
    assert workload.finished_at is None

    for prompt in prompts:
        await prompt.arefresh_from_db()
        assert prompt.answer is None
