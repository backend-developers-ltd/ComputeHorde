from unittest.mock import AsyncMock, Mock

import pytest

from compute_horde_sdk._internal.fallback.client import FallbackClient
from compute_horde_sdk._internal.fallback.job import (
    FallbackJob,
    FallbackJobResult,
    FallbackJobSpec,
    FallbackJobStatus,
    FallbackJobTimeoutError,
)
from compute_horde_sdk._internal.models import HTTPOutputVolume, InlineInputVolume
from compute_horde_sdk._internal.sdk import ComputeHordeJobSpec, ExecutorClass

FAKE_UUID = "deadbeef-face-4fed-abad-cafeaddeddad"


@pytest.mark.parametrize(
    "job_spec_args, expected_run, expected_image_id, expected_accelerators",
    [
        (
            {
                "executor_class": ExecutorClass.spin_up_4min__gpu_24gb,
                "job_namespace": "SN123.0",
                "docker_image": "image/example:latest",
                "download_time_limit_sec": 1,
                "execution_time_limit_sec": 1,
                "streaming_start_time_limit_sec": 1,
                "upload_time_limit_sec": 1,
            },
            "",
            "docker:image/example:latest",
            None,
        ),
        (
            {
                "executor_class": ExecutorClass.always_on__llm__a6000,
                "job_namespace": "SN123.0",
                "args": ["python", "app.py"],
                "docker_image": "image/example:latest",
                "env": {"KEY": "VALUE"},
                "artifacts_dir": "/results",
                "input_volumes": {
                    "/volume/dummy": InlineInputVolume(contents="dummy"),
                },
                "output_volumes": {
                    "/output/dummy": HTTPOutputVolume(http_method="POST", url="https://example.com/dummy"),
                },
                "download_time_limit_sec": 1,
                "execution_time_limit_sec": 1,
                "streaming_start_time_limit_sec": 1,
                "upload_time_limit_sec": 1,
            },
            "python app.py",
            "docker:image/example:latest",
            {"RTXA6000": 1},
        ),
    ],
)
def test_fallback_job_spec_from_job_spec(job_spec_args, expected_run, expected_image_id, expected_accelerators):
    job_spec = ComputeHordeJobSpec(**job_spec_args)
    fallback_job = FallbackJobSpec.from_job_spec(job_spec)

    assert fallback_job.run == expected_run
    assert fallback_job.envs == job_spec_args.get("env", {})
    assert fallback_job.artifacts_dir == job_spec_args.get("artifacts_dir", None)
    assert fallback_job.input_volumes == job_spec_args.get("input_volumes", None)
    assert fallback_job.output_volumes == job_spec_args.get("output_volumes", None)
    assert fallback_job.image_id == expected_image_id
    assert fallback_job.accelerators == expected_accelerators


@pytest.mark.usefixtures("async_sleep_mock")
class TestFallbackJob:
    @pytest.fixture
    def mock_client(self):
        return AsyncMock(spec=FallbackClient)

    @pytest.fixture
    def mock_new_job(self):
        new_status = Mock(spec=FallbackJobStatus)
        new_status.is_in_progress = Mock(return_value=False)
        new_job = Mock(spec=FallbackJob)
        new_job.status = FallbackJobStatus.COMPLETED
        new_job.result = "new_result"
        return new_job

    @pytest.mark.asyncio
    async def test_wait_completed_job(self, mock_client):
        mock_client.get_job.side_effect = (
            FallbackJob(mock_client, uuid=FAKE_UUID, status=FallbackJobStatus.ACCEPTED),
            FallbackJob(mock_client, uuid=FAKE_UUID, status=FallbackJobStatus.COMPLETED),
        )
        job = FallbackJob(mock_client, uuid=FAKE_UUID, status=FallbackJobStatus.SENT)

        await job.wait(timeout=5.0)
        assert not job.status.is_in_progress()

    @pytest.mark.asyncio
    async def test_wait_timeout_error(self, mock_client):
        mock_client.get_job.return_value = FallbackJob(mock_client, uuid=FAKE_UUID, status=FallbackJobStatus.ACCEPTED)
        job = FallbackJob(mock_client, uuid=FAKE_UUID, status=FallbackJobStatus.SENT)

        with pytest.raises(FallbackJobTimeoutError):
            await job.wait(timeout=1.0)

    @pytest.mark.asyncio
    async def test_refresh_updates_status_and_result(self, mock_client):
        mock_client.get_job.return_value = FallbackJob(
            mock_client,
            uuid=FAKE_UUID,
            status=FallbackJobStatus.FAILED,
            result=FallbackJobResult(stdout="test", artifacts={"test": b"test"}),
        )
        job = FallbackJob(mock_client, uuid=FAKE_UUID, status=FallbackJobStatus.SENT)

        await job.refresh()

        assert job.status == FallbackJobStatus.FAILED
        assert job.result == FallbackJobResult(stdout="test", artifacts={"test": b"test"})
