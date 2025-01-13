import asyncio
import random
from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock

import httpx
import pytest
from compute_horde.mv_protocol.miner_requests import V0JobFinishedRequest
from pytest_mock import MockerFixture

from compute_horde_validator.validator.synthetic_jobs.batch_run import (
    _LLM_ANSWERS_DOWNLOAD_MAX_ATTEMPTS,
    _LLM_ANSWERS_DOWNLOAD_RETRY_MIN_BACKOFF,
    LLM_EXECUTOR_CLASS,
    BatchContext,
    LlmAnswerDownloadTask,
    LlmAnswerDownloadTaskFailed,
    _download_llm_prompts_answers,
    _download_llm_prompts_answers_worker,
)
from compute_horde_validator.validator.synthetic_jobs.generator.llm_prompts import (
    LlmPromptsSyntheticJobGenerator,
)


def _create_mock_job(name):
    mock_job = MagicMock()
    mock_job.executor_class = LLM_EXECUTOR_CLASS
    mock_job.job_generator = AsyncMock(spec=LlmPromptsSyntheticJobGenerator)
    mock_job.job_response = MagicMock(spec=V0JobFinishedRequest)
    mock_job.name = name
    mock_job.miner_hotkey = "test_hotkey"
    return mock_job


@pytest.mark.asyncio
async def test_download_worker_successful_download():
    """
    Test that a single task is processed successfully.
    """
    queue = asyncio.Queue()
    mock_job = _create_mock_job("mock_job")
    task = LlmAnswerDownloadTask(mock_job)
    queue.put_nowait(task)

    async with httpx.AsyncClient() as client:
        failures = await _download_llm_prompts_answers_worker(queue, client)

    assert len(failures) == 0
    assert mock_job.job_generator.download_answers.call_count == 1
    assert queue.empty()


@pytest.mark.asyncio
async def test_download_worker_http_retry():
    """
    Test that HTTP errors trigger retry mechanism.
    """
    mock_job = _create_mock_job("mock_job")

    mock_job.job_generator.download_answers.side_effect = [
        httpx.HTTPError("First attempt failed"),  # Fail on first attempt
        None,  # Succeed on second attempt
    ]

    queue = asyncio.Queue()
    task = LlmAnswerDownloadTask(mock_job)
    queue.put_nowait(task)

    async with httpx.AsyncClient() as client:
        failures = await _download_llm_prompts_answers_worker(queue, client)

    assert len(failures) == 0
    assert mock_job.job_generator.download_answers.call_count == 2
    assert task.attempt == 1
    assert task.last_tried is not None
    assert queue.empty()


@pytest.mark.asyncio
async def test_download_worker_max_attempts_exceeded(mocker: MockerFixture):
    """
    Test that tasks exceed maximum retry attempts.
    """
    mocker.patch("asyncio.sleep")  # makes the test faster, should not affect this test

    mock_job = _create_mock_job("mock_job")
    mock_job.job_generator.download_answers.side_effect = httpx.HTTPError("Persistent failure")

    queue = asyncio.Queue()
    task = LlmAnswerDownloadTask(mock_job)
    queue.put_nowait(task)

    async with httpx.AsyncClient() as client:
        failures = await _download_llm_prompts_answers_worker(queue, client)

    assert len(failures) == 1
    assert isinstance(failures[0], LlmAnswerDownloadTaskFailed)
    assert failures[0].task == task
    assert mock_job.job_generator.download_answers.call_count == _LLM_ANSWERS_DOWNLOAD_MAX_ATTEMPTS


@pytest.mark.asyncio
async def test_download_worker_backoff_delay():
    """
    Test that backoff delay is applied between retry attempts.
    """
    mock_job = _create_mock_job("mock_job")
    mock_job.job_generator.download_answers.side_effect = httpx.HTTPError("Persistent failure")

    queue = asyncio.Queue()
    task = LlmAnswerDownloadTask(mock_job)
    queue.put_nowait(task)

    start_time = datetime.now(tz=UTC)
    async with httpx.AsyncClient() as client:
        await _download_llm_prompts_answers_worker(queue, client)
    end_time = datetime.now(tz=UTC)
    wait_time = (end_time - start_time).total_seconds()

    assert wait_time > _LLM_ANSWERS_DOWNLOAD_RETRY_MIN_BACKOFF


@pytest.mark.asyncio
@pytest.mark.parametrize("max_workers", [1, 10, 100])
async def test_download_llm_prompts_answers_integration(mocker: MockerFixture, max_workers: int):
    """
    Integration test for the full download process.
    """
    mocker.patch("asyncio.sleep")  # makes the test faster, should not affect this test
    mocker.patch(
        "compute_horde_validator.validator.synthetic_jobs.batch_run._LLM_ANSWERS_DOWNLOAD_MAX_WORKERS",
        max_workers,
    )

    mock_context = MagicMock(spec=BatchContext)

    successful_jobs = []
    for i in range(7):
        name = f"success_{i}"
        mock_job = _create_mock_job(name)
        successful_jobs.append((name, mock_job))

    successful_retried_jobs = []
    for i in range(11):
        name = f"retried_{i}"
        retried_job = _create_mock_job(name)
        retried_job.job_generator.download_answers.side_effect = [
            httpx.HTTPError("First attempt failed"),
            httpx.HTTPError("Second attempt failed"),
            None,  # Succeed on third attempt
        ]
        successful_retried_jobs.append((name, retried_job))

    failed_jobs = []
    for i in range(13):
        name = f"failed_{i}"
        mock_job = _create_mock_job(name)
        mock_job.job_generator.download_answers.side_effect = httpx.HTTPError("Failed")
        failed_jobs.append((name, mock_job))

    jobs = successful_jobs + successful_retried_jobs + failed_jobs
    random.shuffle(jobs)
    mock_context.jobs = dict(jobs)

    # Run the download process
    await _download_llm_prompts_answers(mock_context)

    # Assert expectations
    for _, job in successful_jobs:
        assert job.job_generator.download_answers.call_count == 1
    for _, job in successful_retried_jobs:
        assert job.job_generator.download_answers.call_count == 3  # we set 2 failures for these
    for _, job in failed_jobs:
        assert job.job_generator.download_answers.call_count == _LLM_ANSWERS_DOWNLOAD_MAX_ATTEMPTS

    assert mock_context.system_event.call_count == len(failed_jobs)
