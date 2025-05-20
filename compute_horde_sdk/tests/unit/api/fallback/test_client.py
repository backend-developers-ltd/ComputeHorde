from unittest.mock import MagicMock, patch

import pytest

from compute_horde_sdk._internal.fallback.client import FallbackClient
from compute_horde_sdk._internal.fallback.exceptions import FallbackNotFoundError
from compute_horde_sdk._internal.fallback.job import FallbackJob, FallbackJobSpec, FallbackJobStatus

FAKE_CLOUD = "runpod"
FAKE_UUID = "deadbeef-face-4fed-abad-cafeaddeddad"
FAKE_UUID2 = "deadbeef-face-4fed-abad-cafeaddeddae"


@pytest.mark.usefixtures("async_sleep_mock")
class TestClient:
    @pytest.fixture
    def mock_sky_job(self):
        sky_job = MagicMock()
        sky_job.id = 1
        sky_job.job_uuid = FAKE_UUID
        sky_job.status.return_value = None
        return sky_job

    @pytest.fixture
    def mock_sky_job2(self):
        sky_job = MagicMock()
        sky_job.id = 2
        sky_job.job_uuid = FAKE_UUID2
        sky_job.status.return_value = None
        return sky_job

    @pytest.fixture
    def job_spec(self):
        return FallbackJobSpec(run="echo test")

    @pytest.mark.asyncio
    async def test_create_job(self, job_spec, mock_sky_job):
        with patch("compute_horde_sdk._internal.fallback.client.sky.SkyJob", return_value=mock_sky_job):
            client = FallbackClient(cloud=FAKE_CLOUD)
            job = await client.create_job(job_spec)
            assert isinstance(job, FallbackJob)
            mock_sky_job.submit.assert_called_once()

    @pytest.mark.asyncio
    async def test_create_job_fallback_not_found(self):
        job_spec = FallbackJobSpec(run="echo test")
        with patch("compute_horde_sdk._internal.fallback.client.sky.SkyJob", side_effect=ModuleNotFoundError):
            with pytest.raises(ModuleNotFoundError):
                client = FallbackClient(cloud=FAKE_CLOUD)
                await client.create_job(job_spec)

    @pytest.mark.asyncio
    async def test_run_until_complete(self, job_spec, mock_sky_job):
        with (
            patch("compute_horde_sdk._internal.fallback.client.sky.SkyJob", return_value=mock_sky_job),
            patch.object(
                FallbackClient, "_get_status", side_effect=(FallbackJobStatus.ACCEPTED, FallbackJobStatus.COMPLETED)
            ),
        ):
            client = FallbackClient(cloud=FAKE_CLOUD)
            job = await client.run_until_complete(job_spec)
            assert job.status.is_successful()

    @pytest.mark.asyncio
    async def test_get_job(self, job_spec, mock_sky_job):
        with patch("compute_horde_sdk._internal.fallback.client.sky.SkyJob", return_value=mock_sky_job):
            client = FallbackClient(cloud=FAKE_CLOUD)
            job = await client.create_job(job_spec)
            job = await client.get_job(job.uuid)
            assert isinstance(job, FallbackJob)

    @pytest.mark.asyncio
    async def test_get_job_not_found(self, job_spec, mock_sky_job):
        with patch("compute_horde_sdk._internal.fallback.client.sky.SkyJob", return_value=mock_sky_job):
            client = FallbackClient(cloud=FAKE_CLOUD)
            await client.create_job(job_spec)
            with pytest.raises(FallbackNotFoundError):
                await client.get_job(FAKE_UUID2)

    @pytest.mark.asyncio
    async def test_iter_jobs(self, job_spec, mock_sky_job, mock_sky_job2):
        client = FallbackClient(cloud=FAKE_CLOUD)
        with patch(
            "compute_horde_sdk._internal.fallback.client.sky.SkyJob",
            return_value=mock_sky_job,
        ):
            await client.create_job(job_spec)
        with patch(
            "compute_horde_sdk._internal.fallback.client.sky.SkyJob",
            return_value=mock_sky_job2,
        ):
            await client.create_job(job_spec)

        jobs = [job async for job in client.iter_jobs()]
        assert len(jobs) == 2
        assert all(isinstance(job, FallbackJob) for job in jobs)
