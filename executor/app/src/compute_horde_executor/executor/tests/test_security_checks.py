from contextlib import asynccontextmanager
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from asgiref.sync import async_to_sync
from compute_horde.job_errors import HordeError
from compute_horde.protocol_consts import HordeFailureReason

from compute_horde_executor.executor import job_driver
from compute_horde_executor.executor.job_driver import JobDriver


class FakeContainer:
    def __init__(self, status_code: int, stdout: str, stderr: str = ""):
        self.status_code = status_code
        self.logs = {True: [stdout], False: [stderr]}

    async def wait(self):
        return {"StatusCode": self.status_code}

    async def log(self, stdout: bool, stderr: bool) -> list[str]:
        return self.logs[stdout]


def run_check(check_name: str, container: FakeContainer) -> HordeError:
    @asynccontextmanager
    async def docker_container_wrapper(*args, **kwargs):
        yield container

    driver = JobDriver(
        runner=MagicMock(clean=AsyncMock()), miner_client=MagicMock(), startup_time_limit=5
    )
    with (
        patch.object(job_driver, "docker_container_wrapper", docker_container_wrapper),
        pytest.raises(HordeError) as exc_info,
    ):
        async_to_sync(getattr(driver, check_name))()
    return exc_info.value


def test_cve_2022_0492_check_nonzero_exit():
    error = run_check(
        "run_cve_2022_0492_check_or_fail", FakeContainer(1, "escaped\n", "some error\n")
    )
    assert (error.reason, error.context) == (
        HordeFailureReason.SECURITY_CHECK_FAILED,
        {"return_code": 1, "stdout": "escaped\n", "stderr": "some error\n"},
    )


def test_cve_2022_0492_check_unexpected_output():
    error = run_check("run_cve_2022_0492_check_or_fail", FakeContainer(0, "escaped\n"))
    assert (error.reason, error.message, error.context) == (
        HordeFailureReason.SECURITY_CHECK_FAILED,
        'CVE-2022-0492 check failed: "Contained: cannot escape via CVE-2022-0492" not in stdout.',
        {"stdout": "escaped\n", "stderr": ""},
    )


def test_nvidia_toolkit_check_outdated_version():
    stdout = "NVIDIA Container Runtime Hook version 1.16.1\n"
    error = run_check("run_nvidia_toolkit_version_check_or_fail", FakeContainer(0, stdout))
    assert (error.reason, error.context) == (
        HordeFailureReason.SECURITY_CHECK_FAILED,
        {"return_code": 0, "stdout": stdout, "stderr": ""},
    )
