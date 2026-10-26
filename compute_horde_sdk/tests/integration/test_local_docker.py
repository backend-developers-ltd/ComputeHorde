"""Real Docker tests; enable with CH_SDK_DOCKER_TESTS=1."""

import asyncio
import os
import subprocess
from contextlib import closing
from dataclasses import replace
from uuid import uuid4

import pytest
from asgiref.sync import sync_to_async
from docker import APIClient, from_env
from docker.errors import NotFound

from compute_horde_sdk.v2 import (
    ComputeHordeJobSpec,
    LocalBindVolume,
    LocalDockerClient,
    LocalJobCancelledError,
    LocalJobLaunchError,
    LocalJobStatus,
    LocalJobTimeoutError,
)

pytestmark = [
    pytest.mark.asyncio,
    pytest.mark.skipif(os.environ.get("CH_SDK_DOCKER_TESTS") != "1", reason="Opt-in local Docker smoke tests"),
]


@sync_to_async(thread_sensitive=False)
def inspect_container(identity):
    with closing(from_env()) as client:
        try:
            return client.api.inspect_container(identity)
        except NotFound:
            return None


@pytest.fixture
def missing_image():
    image = "busybox:1.37"
    result = subprocess.run(["docker", "image", "inspect", image], capture_output=True)
    if result.returncode == 0:
        pytest.skip("The automatic-pull smoke test requires busybox:1.37 to be absent")
    try:
        yield image
    finally:
        subprocess.run(["docker", "image", "rm", image], capture_output=True)


async def test_pull_missing_image_then_reuse(missing_image, monkeypatch):
    pulls = []
    pull = APIClient.pull

    def record_pull(self, image, **kwargs):
        pulls.append({"image": image, **kwargs})
        return pull(self, image, **kwargs)

    monkeypatch.setattr(APIClient, "pull", record_pull)
    client = LocalDockerClient()
    for _ in range(2):
        job = await client.create_job(
            ComputeHordeJobSpec(docker_image=missing_image, args=["printf", "pulled"], platform="linux/amd64")
        )
        try:
            result = await job.wait(timeout=60)
            assert (result.returncode, result.stdout, result.stderr) == (0, b"pulled", b"")
        finally:
            await job.stop()
    assert pulls == [{"image": missing_image, "platform": "linux/amd64", "stream": True, "decode": True}]


@pytest.fixture
def entrypoint_image(tmp_path):
    image = f"ch-sdk-test-{uuid4().hex}:latest"
    (tmp_path / "Dockerfile").write_text('FROM busybox:1.37\nENTRYPOINT ["printf", "%s\\n"]\n')
    subprocess.run(["docker", "build", "-t", image, str(tmp_path)], check=True, capture_output=True)
    try:
        yield image
    finally:
        subprocess.run(["docker", "image", "rm", image], check=True, capture_output=True)


async def test_entrypoint_and_argument_boundaries(entrypoint_image):
    job = await LocalDockerClient().create_job(
        ComputeHordeJobSpec(
            docker_image=entrypoint_image,
            args=["a b", "$(printf INJECTION)", "", '"quoted"'],
            text=True,
        )
    )
    result = await job.wait(timeout=15)
    assert (result.returncode, result.stdout, result.stderr) == (0, 'a b\n$(printf INJECTION)\n\n"quoted"\n', "")
    assert await inspect_container(job.container_id) is None


async def test_binds_limits_and_full_stdout(tmp_path):
    reference = tmp_path / "ref.fa"
    reference.write_bytes(b">ref\nACGT\n")
    output = tmp_path / "artifact.vcf"
    script = (
        'test "$(id -u):$(id -g)" = "$EXPECTED_USER"; '
        "cat /ref.fa > /work/copy.fa; "
        "if touch /readonly/forbidden 2>/dev/null; then exit 88; fi; "
        'printf "diagnostic" >&2; '
        "head -c 3000000 /dev/zero; "
        "sleep 1; exit 7"
    )
    spec = ComputeHordeJobSpec(
        docker_image="busybox:1.37",
        args=["sh", "-ec", script],
        env={"EXPECTED_USER": f"{os.getuid()}:{os.getgid()}"},
        volumes=[
            LocalBindVolume(source=reference, destination="/ref.fa"),
            LocalBindVolume(source=tmp_path, destination="/readonly"),
            LocalBindVolume(source=tmp_path, destination="/work", read_only=False),
        ],
        cpus=0.5,
        memory="64m",
        user=f"{os.getuid()}:{os.getgid()}",
        platform="linux/amd64",
    )
    job = await LocalDockerClient().create_job(spec, stdout=output)
    for _ in range(300):
        if job.status == LocalJobStatus.RUNNING:
            break
        if job.result is not None:
            await job.wait()
        await asyncio.sleep(0.01)
    info = await inspect_container(job.container_id)
    assert (info["HostConfig"]["NanoCpus"], info["HostConfig"]["Memory"], info["Config"]["User"]) == (
        500000000,
        67108864,
        f"{os.getuid()}:{os.getgid()}",
    )
    result = await job.wait(timeout=15)
    actual = await sync_to_async(output.read_bytes)()
    copied = await sync_to_async((tmp_path / "copy.fa").read_bytes)()
    assert (result.returncode, result.stdout, result.stderr, actual, copied) == (
        7,
        None,
        b"diagnostic",
        b"\x00" * 3000000,
        b">ref\nACGT\n",
    )
    assert await inspect_container(job.container_id) is None


async def test_timeouts_repeated_stop_and_missing_image():
    client = LocalDockerClient()
    spec = ComputeHordeJobSpec(docker_image="busybox:1.37", args=["sleep", "60"], execution_time_limit_sec=0.1)
    job = await client.create_job(spec)
    with pytest.raises(LocalJobTimeoutError):
        await job.wait(timeout=15)
    await job.stop()
    await job.stop()
    assert await inspect_container(job.container_id) is None
    failed = await client.create_job(replace(spec, docker_image="ch-sdk-intentionally-absent:missing"))
    with pytest.raises(LocalJobLaunchError):
        await failed.wait(timeout=15)


async def test_concurrent_round_cleanup():
    client = LocalDockerClient()
    jobs = [
        await client.create_job(ComputeHordeJobSpec(docker_image="busybox:1.37", args=["sleep", "60"]))
        for _ in range(3)
    ]
    while any(job.status != LocalJobStatus.RUNNING for job in jobs):
        for job in jobs:
            if job.result is not None:
                await job.wait()
        await asyncio.sleep(0.01)
    await client.stop_jobs(jobs, timeout=20)
    for job in jobs:
        with pytest.raises(LocalJobCancelledError):
            await job.wait()
    assert await asyncio.gather(*(inspect_container(job.container_id) for job in jobs)) == [
        None,
        None,
        None,
    ]
