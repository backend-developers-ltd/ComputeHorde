from __future__ import annotations

import asyncio
import codecs
import time
from collections.abc import Iterable
from contextlib import closing
from pathlib import Path
from typing import BinaryIO, TextIO, cast
from uuid import uuid4

from asgiref.sync import sync_to_async
from docker import APIClient, from_env
from docker.errors import APIError, NotFound
from docker.types import CancellableStream, LogConfig

from .models import (
    ComputeHordeJobSpec,
    LocalCleanupError,
    LocalCleanupTimeoutError,
    LocalDockerError,
    LocalJobCancelledError,
    LocalJobExecutionError,
    LocalJobLaunchError,
    LocalJobResult,
    LocalJobStatus,
    LocalJobTimeoutError,
)

JOB_LABEL = "io.computehorde.sdk.job"

StdoutTarget = str | Path | BinaryIO | TextIO


class LocalDockerJob:
    """An event-loop-bound handle with identity available before Docker creation."""

    def __init__(self, spec: ComputeHordeJobSpec, cleanup_timeout: float, stdout: StdoutTarget | None):
        self.uuid = str(uuid4())
        self.container_name = spec.container_name or f"ch-{self.uuid}"
        self.container_id: str | None = None
        self.spec = spec
        self.status = LocalJobStatus.PREPARED
        self.result: LocalJobResult | None = None
        self._docker: APIClient | None = None
        self._creation_uncertain = False
        self._preparation: asyncio.Task[None] | None = None
        self._cleanup_timeout = cleanup_timeout
        self._stdout_target = stdout
        self._stdout = bytearray()
        self._stderr = bytearray()
        self._done = asyncio.Event()
        self._cancel = asyncio.Event()
        self._reason: LocalJobStatus | None = None
        self._task: asyncio.Task[None] | None = None
        self._error: LocalDockerError | None = None
        self._cleanup_error: LocalCleanupError | None = None
        self._creation_attempted = False
        self._removed = False
        self._remove_lock = asyncio.Lock()

    def start(self) -> None:
        """Schedule execution once. Call after registering this handle under the round lock."""
        if self._task is None:
            self._task = asyncio.get_running_loop().create_task(self._run(), name=f"local-docker-{self.uuid}")

    def request_stop(self) -> None:
        """Prevent future startup and request removal; synchronous, for use under a round lock."""
        if not self._done.is_set():
            self._reason = self._reason or LocalJobStatus.CANCELLED
            self._cancel.set()
            self.start()

    async def wait(self, timeout: float | None = None) -> LocalJobResult:
        """Wait without holding lifecycle locks. A timeout requests actual container removal."""
        if self._task is None:
            raise RuntimeError("Call start() before wait() on a prepared job")
        try:
            await asyncio.wait_for(self._done.wait(), timeout)
        except asyncio.TimeoutError:  # noqa: UP041 - Python 3.10 compatibility
            if not self._done.is_set():
                self._reason = self._reason or LocalJobStatus.TIMED_OUT
                await self.stop()
        if self._cleanup_error is not None:
            raise self._cleanup_error
        if self._error is not None:
            raise self._error
        assert self.result is not None
        return self.result

    async def stop(self, timeout: float | None = None) -> None:
        """Force-remove the container, including during startup; success confirms cleanup."""
        self.request_stop()
        budget = self._cleanup_timeout if timeout is None else timeout

        async def finish() -> None:
            await self._done.wait()
            if self._cleanup_error is not None:
                try:
                    await self._remove()
                except Exception as exc:
                    raise LocalCleanupError(f"Removal of {self.container_name} failed: {exc}", self.result) from exc
                self._cleanup_error = None

        try:
            await asyncio.wait_for(finish(), budget)
        except asyncio.TimeoutError as exc:  # noqa: UP041 - Python 3.10 compatibility
            raise LocalCleanupTimeoutError(
                f"Cleanup of {self.container_name} is still pending; retry stop()", self.result
            ) from exc

    async def _remove(self) -> None:
        async with self._remove_lock:
            if self._removed or not self._creation_attempted:
                return
            assert self._docker is not None
            if self.container_id is None:
                try:
                    info = await sync_to_async(self._docker.inspect_container, thread_sensitive=False)(
                        self.container_name
                    )
                except NotFound:
                    info = None
                if info is None or info["Config"].get("Labels", {}).get(JOB_LABEL) != self.uuid:
                    if self._creation_uncertain:
                        raise LocalCleanupError(
                            "Docker create response was lost; container absence is not yet confirmed"
                        )
                    self._removed = True
                    return
                self.container_id = info["Id"]
            while True:
                try:
                    await sync_to_async(self._docker.remove_container, thread_sensitive=False)(
                        self.container_id, force=True, v=True
                    )
                    break
                except NotFound:
                    break
                except APIError as exc:
                    if exc.status_code != 409:
                        raise
                    await asyncio.sleep(0.05)
            self._removed = True

    async def _collect(
        self,
        stream: CancellableStream[tuple[bytes | None, bytes | None]],
        sink: BinaryIO | TextIO | None,
        binary_sink: bool,
    ) -> int:
        assert self.container_id is not None
        assert self._docker is not None
        decoder = codecs.getincrementaldecoder(self.spec.encoding)(errors=self.spec.errors)
        while (chunks := await sync_to_async(lambda: next(stream, None), thread_sensitive=False)()) is not None:
            stdout, stderr = chunks
            if stderr:
                self._stderr.extend(stderr)
            if stdout:
                if sink is None:
                    self._stdout.extend(stdout)
                else:
                    await self._write(sink, stdout if binary_sink else decoder.decode(stdout))
        if sink is not None:
            if not binary_sink:
                await self._write(sink, decoder.decode(b"", final=True))
            await sync_to_async(sink.flush, thread_sensitive=False)()
        result = await sync_to_async(self._docker.wait, thread_sensitive=False)(self.container_id)
        if result.get("Error"):
            raise LocalDockerError(result["Error"]["Message"])
        return int(result["StatusCode"])

    @staticmethod
    async def _write(sink: BinaryIO | TextIO, chunk: bytes | str) -> None:
        # Let an in-flight write finish before closing a sink, even when collection is cancelled.
        task = asyncio.create_task(sync_to_async(LocalDockerJob._write_all, thread_sensitive=False)(sink, chunk))
        try:
            await asyncio.shield(task)
        except asyncio.CancelledError:
            await task
            raise

    @staticmethod
    def _write_all(sink: BinaryIO | TextIO, chunk: bytes | str) -> None:
        offset = 0
        while offset < len(chunk):
            if isinstance(chunk, bytes):
                written = cast(BinaryIO, sink).write(chunk[offset:])
            else:
                written = cast(TextIO, sink).write(chunk[offset:])
            if written is None or written <= 0:
                raise OSError("stdout destination did not accept output")
            offset += written

    def _prepare_image(self) -> None:
        self._docker = from_env().api
        try:
            image = self._docker.inspect_image(self.spec.docker_image)  # type: ignore[no-untyped-call]
        except NotFound:
            pass
        else:
            platform = [image.get("Os"), image.get("Architecture"), image.get("Variant", "")]
            requested = self.spec.platform.split("/") if self.spec.platform else []
            if not requested or requested == platform[: len(requested)]:
                return
        with closing(
            self._docker.pull(self.spec.docker_image, platform=self.spec.platform, stream=True, decode=True)
        ) as progress_stream:
            for progress in progress_stream:
                error = progress.get("error") or progress.get("errorDetail", {}).get("message")
                if error:
                    raise LocalDockerError(f"Pulling image {self.spec.docker_image!r} failed: {error}")
                if self._cancel.is_set():
                    return

    async def _ensure_image(self) -> None:
        async def prepare() -> None:
            try:
                await sync_to_async(self._prepare_image, thread_sensitive=False)()
            finally:
                if self._cancel.is_set() and self._docker is not None:
                    await sync_to_async(self._docker.close, thread_sensitive=False)()

        preparation = self._preparation = asyncio.create_task(prepare())
        cancellation = asyncio.create_task(self._cancel.wait())
        try:
            await asyncio.wait((preparation, cancellation), return_when=asyncio.FIRST_COMPLETED)
            if not self._cancel.is_set():
                await preparation
            else:
                preparation.add_done_callback(lambda task: None if task.cancelled() else task.exception())
        finally:
            cancellation.cancel()
            await asyncio.gather(cancellation, return_exceptions=True)

    def _create_container(self) -> str:
        assert self._docker is not None
        host_config = self._docker.create_host_config(
            mounts=[volume.to_docker_mount() for volume in self.spec.volumes],
            nano_cpus=int(self.spec.cpus * 1_000_000_000) if self.spec.cpus is not None else None,
            mem_limit=self.spec.memory,
            log_config=LogConfig(type="none"),
        )
        self._creation_attempted = True
        self._creation_uncertain = True
        try:
            result = self._docker.create_container(
                image=self.spec.docker_image,
                command=list(self.spec.args) or None,
                environment=dict(self.spec.env),
                labels={JOB_LABEL: self.uuid},
                host_config=host_config,
                name=self.container_name,
                user=self.spec.user,
                platform=self.spec.platform,
            )
        except APIError:
            self._creation_uncertain = False
            raise
        self._creation_uncertain = False
        return str(result["Id"])

    async def _run(self) -> None:
        sink: BinaryIO | TextIO | None = None
        owned_sink = isinstance(self._stdout_target, (str, Path))
        collector: asyncio.Task[int] | None = None
        cancellation: asyncio.Task[bool] | None = None
        returncode: int | None = None
        started = False
        stream: CancellableStream[tuple[bytes | None, bytes | None]] | None = None
        try:
            if self._cancel.is_set():
                return
            self.status = LocalJobStatus.STARTING
            if owned_sink:
                sink = await sync_to_async(open, thread_sensitive=False)(self._stdout_target, "wb")  # type: ignore[arg-type]
            else:
                sink = cast(BinaryIO | TextIO | None, self._stdout_target)
            if self._cancel.is_set():
                return
            await self._ensure_image()
            if self._cancel.is_set():
                return
            self.container_id = await sync_to_async(self._create_container, thread_sensitive=False)()
            if self._cancel.is_set():
                return
            assert self._docker is not None
            docker = self._docker
            container_id = self.container_id
            stream = await sync_to_async(
                lambda: docker.attach(container_id, stdout=True, stderr=True, stream=True, demux=True),
                thread_sensitive=False,
            )()
            if self._cancel.is_set():
                return
            await sync_to_async(self._docker.start, thread_sensitive=False)(self.container_id)
            started = True
            self.status = LocalJobStatus.RUNNING
            collector = asyncio.create_task(self._collect(stream, sink, owned_sink or not self.spec.text))
            cancellation = asyncio.create_task(self._cancel.wait())
            done, _ = await asyncio.wait(
                (collector, cancellation),
                timeout=self.spec.execution_time_limit_sec,
                return_when=asyncio.FIRST_COMPLETED,
            )
            if collector in done:
                returncode = collector.result()
                self.status = LocalJobStatus.COMPLETED if returncode == 0 else LocalJobStatus.FAILED
            elif cancellation not in done:
                self._reason = LocalJobStatus.TIMED_OUT
                self._cancel.set()
        except Exception as exc:
            error_type = LocalJobExecutionError if started else LocalJobLaunchError
            self._error = error_type(str(exc))
            self.status = LocalJobStatus.FAILED if started else LocalJobStatus.LAUNCH_FAILED
        finally:
            try:
                await asyncio.wait_for(self._remove(), self._cleanup_timeout)
            except Exception as exc:
                self._cleanup_error = LocalCleanupError(f"Removal of {self.container_name} failed: {exc}")
            if stream is not None:
                try:
                    await sync_to_async(stream.close, thread_sensitive=False)()
                except OSError:
                    pass
            if collector is not None:
                if not collector.done():
                    collector.cancel()
                await asyncio.gather(collector, return_exceptions=True)
            if cancellation is not None:
                cancellation.cancel()
                await asyncio.gather(cancellation, return_exceptions=True)
            try:
                if sink is not None:
                    try:
                        await sync_to_async(sink.flush, thread_sensitive=False)()
                    finally:
                        if owned_sink:
                            await sync_to_async(sink.close, thread_sensitive=False)()
                if self._docker is not None and (self._preparation is None or self._preparation.done()):
                    await sync_to_async(self._docker.close, thread_sensitive=False)()
                self._finish(returncode)
            except Exception as exc:
                self._error = LocalJobExecutionError(str(exc))
                self.status = LocalJobStatus.FAILED
            finally:
                self._done.set()

    def _finish(self, returncode: int | None) -> None:
        if self._reason is not None:
            self.status = self._reason
            error_type = LocalJobTimeoutError if self._reason == LocalJobStatus.TIMED_OUT else LocalJobCancelledError
            self._error = error_type(f"Job {self.uuid} {self.status.value}")

        def output(data: bytearray) -> bytes | str:
            value = bytes(data)
            return value.decode(self.spec.encoding, self.spec.errors) if self.spec.text else value

        self.result = LocalJobResult(
            args=(self.spec.docker_image, *self.spec.args),
            returncode=returncode,
            stdout=output(self._stdout) if self._stdout_target is None else None,
            stderr=output(self._stderr),
            status=self.status,
        )
        for error in (self._error, self._cleanup_error):
            if error is not None:
                error.result = self.result


class LocalDockerClient:
    """Run Linux containers using Docker connection configuration."""

    def __init__(self, cleanup_timeout: float = 30):
        if cleanup_timeout <= 0:
            raise ValueError("cleanup_timeout must be positive")
        self.cleanup_timeout = cleanup_timeout

    def prepare_job(self, job_spec: ComputeHordeJobSpec, *, stdout: StdoutTarget | None = None) -> LocalDockerJob:
        """Allocate a handle without contacting Docker. Register it, then call start()."""
        if not isinstance(job_spec, ComputeHordeJobSpec):
            raise TypeError("LocalDockerClient requires compute_horde_sdk.v2.ComputeHordeJobSpec")
        return LocalDockerJob(job_spec, self.cleanup_timeout, stdout)

    async def create_job(self, job_spec: ComputeHordeJobSpec, *, stdout: StdoutTarget | None = None) -> LocalDockerJob:
        """Submit and return identity immediately; use prepare_job for locked registration."""
        job = self.prepare_job(job_spec, stdout=stdout)
        job.start()
        return job

    async def stop_jobs(self, jobs: Iterable[LocalDockerJob], timeout: float = 20) -> None:
        """Stop a snapshot concurrently under one total deadline; never multiply the budget."""
        deadline = time.monotonic() + timeout
        snapshot = tuple(jobs)
        for job in snapshot:
            job.request_stop()
        results = await asyncio.gather(
            *(job.stop(timeout=max(0, deadline - time.monotonic())) for job in snapshot), return_exceptions=True
        )
        failures = {
            job.uuid: str(result) for job, result in zip(snapshot, results) if isinstance(result, BaseException)
        }
        if failures:
            raise LocalCleanupError(f"Cleanup not confirmed for jobs: {failures}")
