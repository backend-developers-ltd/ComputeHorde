import asyncio
import io
import threading
import time
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import Mock, call

import pytest
from asgiref.sync import sync_to_async
from docker.errors import APIError, NotFound

from compute_horde_sdk._internal.fallback.client import FallbackClient
from compute_horde_sdk._internal.fallback.job import FallbackJobSpec
from compute_horde_sdk._internal.local.client import JOB_LABEL
from compute_horde_sdk.v1 import ComputeHordeClient
from compute_horde_sdk.v2 import (
    ComputeHordeJobSpec,
    LocalBindVolume,
    LocalCleanupError,
    LocalCleanupTimeoutError,
    LocalDockerClient,
    LocalDockerError,
    LocalJobCancelledError,
    LocalJobExecutionError,
    LocalJobLaunchError,
    LocalJobResult,
    LocalJobStatus,
    LocalJobTimeoutError,
)


def closing_stream(messages):
    yield from messages


class Gate:
    def __init__(self):
        self.event = threading.Event()

    def set(self):
        self.event.set()

    def clear(self):
        self.event.clear()

    async def wait(self):
        await sync_to_async(self.event.wait, thread_sensitive=False)()

    def block(self):
        assert self.event.wait(timeout=5), "Test gate timed out"


class FakeDocker:
    def __init__(self):
        self.calls = []
        self.create_entered = Gate()
        self.image_entered = Gate()
        self.start_entered = Gate()
        self.attach_entered = Gate()
        for name in ("image_gate", "create_gate", "attach_gate", "start_gate", "exit_gate", "remove_gate"):
            gate = Gate()
            gate.set()
            setattr(self, name, gate)
        self.returncode = 0
        self.chunks = [(1, b"output\x00\xff"), (2, b"error\n")]
        self.failure = None
        self.image_failure = None
        self.remove_failure = None
        self.removed = False
        self.uuid = None
        self.close = Mock()
        self.create_host_config = Mock(return_value={})
        self.pull = Mock()

    def inspect_image(self, image):
        self.image_entered.set()
        self.image_gate.block()
        if self.image_failure:
            raise self.image_failure
        return {"Os": "linux", "Architecture": "amd64"}

    def create_container(self, **kwargs):
        self.calls.append("create")
        self.uuid = kwargs["labels"][JOB_LABEL]
        self.create_entered.set()
        self.create_gate.block()
        if self.failure:
            raise self.failure
        return {"Id": "container-id"}

    def start(self, container_id):
        self.calls.append("start")
        self.start_entered.set()
        self.start_gate.block()

    def logs(self):
        for stream, chunk in self.chunks:
            yield (chunk, None) if stream == 1 else (None, chunk)
        self.exit_gate.block()

    def attach(self, container_id, **kwargs):
        self.attach_entered.set()
        self.attach_gate.block()
        return FakeStream(self.logs(), self.exit_gate)

    def wait(self, container_id):
        self.exit_gate.block()
        return {"StatusCode": self.returncode}

    def inspect_container(self, identity):
        if self.removed:
            raise NotFound("missing")
        return {
            "Id": "container-id",
            "Config": {"Labels": {JOB_LABEL: self.uuid}},
            "State": {"Running": False, "ExitCode": self.returncode},
        }

    def remove_container(self, container_id, **kwargs):
        self.calls.append("remove")
        self.remove_gate.block()
        if self.remove_failure:
            raise self.remove_failure
        self.removed = True
        self.exit_gate.set()


class FakeStream:
    def __init__(self, chunks, gate):
        self.chunks = iter(chunks)
        self.gate = gate

    def __next__(self):
        return next(self.chunks)

    def close(self):
        self.gate.set()


@pytest.fixture
def engine(monkeypatch):
    fake = FakeDocker()
    monkeypatch.setattr("compute_horde_sdk._internal.local.client.from_env", lambda: SimpleNamespace(api=fake))
    return fake


def spec(**kwargs):
    return ComputeHordeJobSpec(docker_image="example:latest", **kwargs)


def test_volumes(tmp_path):
    source = tmp_path / 'a,"file'
    source.touch()
    volumes = [
        LocalBindVolume(source=tmp_path, destination="/data"),
        LocalBindVolume(source=source, destination="/ref.fa"),
        LocalBindVolume(source=tmp_path, destination="/output", read_only=False),
    ]
    assert [volume.to_docker_mount() for volume in volumes] == [
        {"Type": "bind", "Source": str(tmp_path), "Target": "/data", "ReadOnly": True},
        {"Type": "bind", "Source": str(source), "Target": "/ref.fa", "ReadOnly": True},
        {"Type": "bind", "Source": str(tmp_path), "Target": "/output", "ReadOnly": False},
    ]
    with pytest.raises(ValueError, match="existing regular"):
        LocalBindVolume(source=tmp_path / "missing", destination="/data")
    with pytest.raises(ValueError, match="absolute"):
        LocalBindVolume(source=tmp_path, destination="relative")


@pytest.mark.asyncio
async def test_docker_mapping(engine, tmp_path):
    args = ["sh", "-lc", 'printf "%s" "$VAR"; echo "a b"', "", "x;$(no)"]
    job_spec = spec(
        args=args,
        env={"VAR": "a b=c"},
        cpus=1.5,
        memory="2g",
        user="1001:1002",
        platform="linux/amd64",
        volumes=[LocalBindVolume(source=tmp_path, destination="/somewhere", read_only=False)],
    )
    engine.create_container = Mock(wraps=engine.create_container)
    job = await LocalDockerClient().create_job(job_spec)
    await job.wait()
    engine.create_host_config.assert_called_once_with(
        mounts=[{"Type": "bind", "Source": str(tmp_path), "Target": "/somewhere", "ReadOnly": False}],
        nano_cpus=1500000000,
        mem_limit="2g",
        log_config={"Type": "none", "Config": {}},
    )
    engine.create_container.assert_called_once_with(
        image="example:latest",
        command=args,
        environment={"VAR": "a b=c"},
        labels={JOB_LABEL: job.uuid},
        host_config={},
        name=job.container_name,
        user="1001:1002",
        platform="linux/amd64",
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("returncode", [0, 1, 125, 137])
async def test_exit_results(engine, returncode):
    engine.returncode = returncode
    job = await LocalDockerClient().create_job(spec())
    result = await job.wait()
    status = LocalJobStatus.COMPLETED if returncode == 0 else LocalJobStatus.FAILED
    assert (job.container_id, job.status, result, engine.calls) == (
        "container-id",
        status,
        LocalJobResult(("example:latest",), returncode, b"output\x00\xff", b"error\n", status),
        ["create", "start", "remove"],
    )
    assert vars(result.to_completed_process()) == {
        "args": ("example:latest",),
        "returncode": returncode,
        "stdout": b"output\x00\xff",
        "stderr": b"error\n",
    }
    await job.stop()
    await job.stop()
    assert engine.calls == ["create", "start", "remove"]


@pytest.mark.asyncio
@pytest.mark.parametrize("target", ["path", "binary", "text"])
async def test_stdout_delivery(engine, tmp_path, target):
    payload = "ACGT\t€\r\n" * 200000
    raw = payload.encode()
    engine.chunks = [(1, raw[:6]), (2, b"diagnostic"), (1, raw[6:8]), (1, raw[8:])]
    sink = tmp_path / "out.vcf" if target == "path" else io.StringIO() if target == "text" else io.BytesIO()
    job = await LocalDockerClient().create_job(spec(text=target == "text"), stdout=sink)
    result = await job.wait()
    actual = sink.read_bytes() if target == "path" else sink.getvalue()
    assert (actual, result.stdout, result.stderr, bytes(job._stdout)) == (
        payload if target == "text" else raw,
        None,
        "diagnostic" if target == "text" else b"diagnostic",
        b"",
    )
    if target != "path":
        assert not sink.closed


@pytest.mark.asyncio
@pytest.mark.parametrize("stage", ["prepared", "scheduled", "create", "attach", "start", "running"])
async def test_stop_start_races(engine, stage):
    engine.exit_gate.clear()
    client = LocalDockerClient()
    job = client.prepare_job(spec(container_name="test-round-job"))
    assert (job.container_name, job.container_id, engine.calls) == ("test-round-job", None, [])
    if stage == "create":
        engine.create_gate.clear()
    if stage == "start":
        engine.start_gate.clear()
    if stage == "attach":
        engine.attach_gate.clear()
    if stage != "prepared":
        job.start()
    if stage == "create":
        await engine.create_entered.wait()
    elif stage == "attach":
        await engine.attach_entered.wait()
    elif stage in {"start", "running"}:
        await engine.start_entered.wait()
    job.request_stop()
    stop = asyncio.create_task(job.stop())
    await asyncio.sleep(0)
    engine.create_gate.set()
    engine.attach_gate.set()
    engine.start_gate.set()
    await stop
    job.start()
    with pytest.raises(LocalJobCancelledError):
        await job.wait()
    expected = (
        []
        if stage in {"prepared", "scheduled"}
        else ["create", "remove"]
        if stage in {"create", "attach"}
        else ["create", "start", "remove"]
    )
    assert (job.status, engine.calls) == (LocalJobStatus.CANCELLED, expected)
    await job.stop()
    assert engine.calls == expected


@pytest.mark.asyncio
@pytest.mark.parametrize("execution_timeout", [True, False])
async def test_timeout_removes(engine, execution_timeout):
    engine.exit_gate.clear()
    job = await LocalDockerClient().create_job(spec(execution_time_limit_sec=0.01 if execution_timeout else None))
    with pytest.raises(LocalJobTimeoutError) as error:
        await job.wait(timeout=None if execution_timeout else 0.01)
    assert (engine.removed, job.status, error.value.result) == (True, LocalJobStatus.TIMED_OUT, job.result)


@pytest.mark.asyncio
async def test_waiter_cancellation_does_not_stop_or_block_another_task(engine):
    engine.exit_gate.clear()
    job = await LocalDockerClient().create_job(spec())
    waiter = asyncio.create_task(job.wait())
    await engine.start_entered.wait()
    waiter.cancel()
    with pytest.raises(asyncio.CancelledError):
        await waiter
    await job.stop()
    assert engine.removed


@pytest.mark.asyncio
async def test_stop_deadline_during_create_and_late_cleanup(engine):
    engine.create_gate.clear()
    job = await LocalDockerClient().create_job(spec())
    await engine.create_entered.wait()
    with pytest.raises(LocalCleanupTimeoutError):
        await job.stop(timeout=0.01)
    assert engine.calls == ["create"]
    engine.create_gate.set()
    await job.stop()
    assert engine.calls == ["create", "remove"]


@pytest.mark.asyncio
async def test_launch_error_and_ambiguous_create_cleanup(engine):
    engine.failure = OSError("lost create response")
    job = await LocalDockerClient().create_job(spec())
    with pytest.raises(LocalJobLaunchError, match="lost create response"):
        await job.wait()
    assert (engine.calls, job.status, job.result.returncode) == (
        ["create", "remove"],
        LocalJobStatus.LAUNCH_FAILED,
        None,
    )


@pytest.mark.asyncio
async def test_failed_removal_is_reported_and_retryable(engine):
    engine.remove_failure = OSError("daemon unavailable")
    job = await LocalDockerClient().create_job(spec())
    with pytest.raises(LocalCleanupError):
        await job.wait()
    with pytest.raises(LocalCleanupError):
        await job.stop()
    engine.remove_failure = None
    await job.stop()
    assert engine.removed


@pytest.mark.asyncio
async def test_lost_create_response_does_not_claim_cleanup_before_late_creation(engine):
    engine.failure = OSError("lost response")
    inspect = engine.inspect_container
    engine.inspect_container = Mock(side_effect=NotFound("missing"))
    job = await LocalDockerClient().create_job(spec())
    with pytest.raises(LocalCleanupError):
        await job.wait()
    with pytest.raises(LocalCleanupError):
        await job.stop()
    engine.inspect_container = inspect
    await job.stop()
    assert (engine.removed, engine.calls) == (True, ["create", "remove"])


@pytest.mark.asyncio
async def test_round_cleanup_has_one_budget(monkeypatch):
    engines = [FakeDocker() for _ in range(5)]
    remaining = iter(engines)
    monkeypatch.setattr(
        "compute_horde_sdk._internal.local.client.from_env", lambda: SimpleNamespace(api=next(remaining))
    )
    for engine in engines:
        engine.create_gate.clear()
    client = LocalDockerClient()
    jobs = [await client.create_job(spec()) for _ in engines]
    await asyncio.gather(*(engine.create_entered.wait() for engine in engines))
    start = time.monotonic()
    with pytest.raises(LocalCleanupError):
        await client.stop_jobs(jobs, timeout=0.05)
    assert time.monotonic() - start < 0.15
    for engine in engines:
        engine.create_gate.set()
    await client.stop_jobs(jobs)
    assert [engine.calls for engine in engines] == [["create", "remove"]] * 5


@pytest.mark.asyncio
async def test_remote_backends_reject_v2_before_side_effects():
    with pytest.raises(TypeError, match="LocalDockerClient"):
        await ComputeHordeClient.create_job(None, spec())
    with pytest.raises(TypeError, match="LocalDockerClient"):
        await FallbackClient.create_job(None, spec())
    with pytest.raises(TypeError, match="LocalDockerClient"):
        FallbackJobSpec.from_job_spec(spec())


def test_spec_snapshots_collections():
    args, env, volumes = ["a"], {"x": "y"}, []
    job_spec = spec(args=args, env=env, volumes=volumes)
    args.append("b")
    env["x"] = "z"
    assert (job_spec.args, dict(job_spec.env), job_spec.volumes) == (("a",), {"x": "y"}, ())
    with pytest.raises(ValueError):
        replace(job_spec, cpus=float("nan"))


@pytest.mark.asyncio
async def test_short_writes_and_write_failure(engine):
    class ShortWriter(io.BytesIO):
        def write(self, data):
            return super().write(data[:2])

    sink = ShortWriter()
    job = await LocalDockerClient().create_job(spec(), stdout=sink)
    await job.wait()
    assert sink.getvalue() == b"output\x00\xff"

    class BrokenWriter(io.BytesIO):
        def write(self, data):
            raise OSError("disk full")

    failed = await LocalDockerClient().create_job(spec(), stdout=BrokenWriter())
    with pytest.raises(LocalJobExecutionError, match="disk full"):
        await failed.wait()
    assert engine.removed


@pytest.mark.asyncio
async def test_name_collision_does_not_remove_unrelated_container(engine):
    engine.failure = APIError("name already exists")
    engine.inspect_container = Mock(return_value={"Id": "someone-else", "Config": {"Labels": {JOB_LABEL: "other-job"}}})
    job = await LocalDockerClient().create_job(spec(container_name="already-in-use"))
    with pytest.raises(LocalJobLaunchError):
        await job.wait()
    assert engine.calls == ["create"]


@pytest.mark.asyncio
async def test_missing_container_removal_and_removal_race(engine):
    engine.remove_container = Mock(
        side_effect=[APIError("removal in progress", response=SimpleNamespace(status_code=409)), NotFound("missing")]
    )
    job = await LocalDockerClient().create_job(spec())
    await job.wait()
    assert engine.remove_container.call_args_list == [call("container-id", force=True, v=True)] * 2


@pytest.mark.asyncio
async def test_stop_from_another_thread(engine):
    engine.exit_gate.clear()
    job = await LocalDockerClient().create_job(spec())
    await engine.start_entered.wait()
    loop = asyncio.get_running_loop()

    def stop_in_thread():
        asyncio.run_coroutine_threadsafe(job.stop(), loop).result(timeout=2)

    await sync_to_async(stop_in_thread, thread_sensitive=False)()
    assert engine.removed


@pytest.mark.asyncio
@pytest.mark.parametrize("platform", [None, "linux/amd64", "linux/arm64/v8"])
async def test_existing_image_is_not_pulled(engine, platform):
    parts = (platform or "linux/amd64").split("/")
    engine.inspect_image = Mock(
        return_value={"Os": parts[0], "Architecture": parts[1], "Variant": parts[2] if len(parts) > 2 else ""}
    )
    job = await LocalDockerClient().create_job(spec(platform=platform))
    await job.wait()
    engine.inspect_image.assert_called_once_with("example:latest")
    engine.pull.assert_not_called()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "reference", ["busybox", "registry.test:5000/team/image", "busybox:1.37", "registry.test/image@sha256:1234"]
)
async def test_missing_image_pull_preserves_reference_and_platform(engine, reference):
    engine.inspect_image = Mock(side_effect=NotFound("missing"))
    engine.pull.return_value = closing_stream([{"status": "Pull complete"}])
    job = await LocalDockerClient().create_job(replace(spec(platform="linux/amd64"), docker_image=reference))
    await job.wait()
    engine.pull.assert_called_once_with(reference, platform="linux/amd64", stream=True, decode=True)


@pytest.mark.asyncio
async def test_image_with_wrong_platform_is_pulled(engine):
    engine.inspect_image = Mock(return_value={"Os": "linux", "Architecture": "arm64"})
    engine.pull.return_value = closing_stream([{"status": "Pull complete"}])
    job = await LocalDockerClient().create_job(spec(platform="linux/amd64"))
    await job.wait()
    engine.pull.assert_called_once_with("example:latest", platform="linux/amd64", stream=True, decode=True)


@pytest.mark.asyncio
@pytest.mark.parametrize("message", [{"error": "pull denied"}, {"errorDetail": {"message": "pull denied"}}])
async def test_pull_stream_error_is_not_treated_as_success(engine, message):
    engine.inspect_image = Mock(side_effect=NotFound("missing"))
    engine.pull.return_value = closing_stream([{"status": "Downloading"}, message])
    job = await LocalDockerClient().create_job(spec())
    with pytest.raises(LocalJobLaunchError, match="pull denied"):
        await job.wait()
    assert engine.calls == []


@pytest.mark.asyncio
async def test_image_inspection_failure_is_not_treated_as_missing(engine):
    engine.inspect_image = Mock(side_effect=APIError("daemon error"))
    job = await LocalDockerClient().create_job(spec())
    with pytest.raises(LocalJobLaunchError, match="daemon error"):
        await job.wait()
    engine.pull.assert_not_called()


@pytest.mark.asyncio
@pytest.mark.parametrize("timeout", [False, True])
async def test_cancel_during_pull_prevents_container_creation(engine, timeout):
    engine.image_gate.clear()
    job = await LocalDockerClient().create_job(spec())
    await engine.image_entered.wait()
    if timeout:
        with pytest.raises(LocalJobTimeoutError):
            await job.wait(timeout=0.01)
    else:
        await job.stop(timeout=0.1)
        with pytest.raises(LocalJobCancelledError):
            await job.wait()
    engine.image_gate.set()
    await job._preparation
    await job.stop()
    assert (engine.calls, job.container_id) == ([], None)


@pytest.mark.asyncio
async def test_pull_failure_is_a_launch_failure(engine):
    engine.image_failure = LocalDockerError("pull denied")
    job = await LocalDockerClient().create_job(spec())
    with pytest.raises(LocalJobLaunchError, match="pull denied"):
        await job.wait()
    assert (engine.calls, job.status) == ([], LocalJobStatus.LAUNCH_FAILED)


@pytest.mark.asyncio
async def test_client_connection_and_resources(monkeypatch, engine):
    constructor = Mock(return_value=SimpleNamespace(api=engine))
    monkeypatch.setattr("compute_horde_sdk._internal.local.client.from_env", constructor)
    engine.create_container = Mock(wraps=engine.create_container)
    job = LocalDockerClient().prepare_job(spec())
    constructor.assert_not_called()
    job.start()
    await job.wait()
    constructor.assert_called_once_with()
    engine.create_container.assert_called_once_with(
        image="example:latest",
        command=None,
        environment={},
        labels={JOB_LABEL: job.uuid},
        host_config={},
        name=job.container_name,
        user=None,
        platform=None,
    )
    engine.close.assert_called_once_with()


@pytest.mark.asyncio
async def test_attach_failure_removes_container(engine):
    engine.attach = Mock(side_effect=APIError("attach failed"))
    job = await LocalDockerClient().create_job(spec())
    with pytest.raises(LocalJobLaunchError, match="attach failed"):
        await job.wait()
    assert (engine.calls, engine.removed) == (["create", "remove"], True)
    engine.close.assert_called_once_with()


@pytest.mark.asyncio
async def test_wait_error_removes_container(engine):
    engine.wait = Mock(return_value={"Error": {"Message": "wait failed"}})
    job = await LocalDockerClient().create_job(spec())
    with pytest.raises(LocalJobExecutionError, match="wait failed"):
        await job.wait()
    assert (engine.calls, engine.removed) == (["create", "start", "remove"], True)
    engine.close.assert_called_once_with()
