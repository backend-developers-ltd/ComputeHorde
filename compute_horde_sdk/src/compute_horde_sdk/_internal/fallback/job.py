import asyncio
import base64
import os
import tempfile
import time
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import timedelta
from typing import IO, TYPE_CHECKING, Any, Self

from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.rsa import RSAPrivateKey

from ..models import ComputeHordeJobResult, ComputeHordeJobStatus, InputVolume, OutputVolume
from ..sdk import ComputeHordeJobSpec
from .exceptions import FallbackJobTimeoutError

if TYPE_CHECKING:
    from .client import FallbackClient


JOB_REFRESH_INTERVAL = timedelta(seconds=3)


@dataclass
class FallbackJobSpec:
    """
    Specification of a fallback job to run on the given cloud.
    """

    run: str | None = None
    """A command to execute. It should be a single string, not a list of arguments."""

    envs: Mapping[str, str] | None = None
    """Environment variables to run the job with."""

    work_dir: str = "/"
    """Path to the working directory for the job. Defaults to ``/``."""

    artifacts_dir: str | None = None
    """
    Path of the directory that the job will write its results to.
    Contents of files found in this directory will be returned after the job completes
    as a part of the job result. It should be an absolute path (starting with ``/``).
    """

    input_volumes: Mapping[str, InputVolume] | None = None
    """
    The data to be made available to the job in Docker volumes.
    The keys should be absolute file/directory paths under which you want your data to be available.
    The values should be :class:`InputVolume` instances representing how to obtain the input data.
    For now, input volume paths must start with ``/volume/``.
    """

    output_volumes: Mapping[str, OutputVolume] | None = None
    """
    The data to be read from the Docker volumes after job completion
    and uploaded to the described destinations. Use this for outputs that are too big
    to be treated as ``artifacts``.
    The keys should be absolute file paths under which job output data will be available.
    The values should be :class:`OutputVolume` instances representing how to handle the output data.
    For now, output volume paths must start with ``/output/``.
    """

    cpus: int | float | str | None = None
    """Required number of CPUs for the job."""

    memory: int | float | str | None = None
    """Required amount of memory for the job."""

    accelerators: str | Mapping[str, int] | None = None
    """Required GPUs for the job."""

    disk_size: int | None = None
    """Required amount of memory for the job."""

    ports: int | str | Sequence[str] | None = None
    """Required amount of memory for the job."""

    instance_type: str | None = None
    """Instance type."""

    image_id: str | None = None
    """Image ID. For docker it must be in the form of ``docker:<image>``."""

    region: str | None = None
    """Region to run the job in."""

    zone: str | None = None
    """Zone to run the job in."""

    streaming: bool = False
    """Whether to enable streaming."""

    proxy_pass_port: int = 80
    """
    Port to proxy pass to for streaming jobs.

    This is the internal port on which the job's server runs inside the Docker container.
    The streaming server will proxy incoming requests to this port.
    """

    @classmethod
    def from_job_spec(cls, job_spec: ComputeHordeJobSpec, **kwargs: Any) -> Self:
        if job_spec.executor_class == job_spec.executor_class.__class__.always_on__llm__a6000:
            accelerators = {"RTXA6000": 1}
        else:
            accelerators = None

        return cls(
            run=" ".join(job_spec.args),
            envs=job_spec.env,
            artifacts_dir=job_spec.artifacts_dir,
            input_volumes=job_spec.input_volumes,
            output_volumes=job_spec.output_volumes,
            accelerators=accelerators,
            image_id=f"docker:{job_spec.docker_image}",
            streaming=job_spec.streaming,
            **kwargs,
        )


@dataclass
class FallbackJobResult(ComputeHordeJobResult):
    """
    Result of a fallback job.
    """


FallbackJobStatus = ComputeHordeJobStatus


class FallbackJob:
    """
    The class representing a job running on the SkyPilot cloud.
    Do not construct it directly, always use :class:`FallbackClient`.

    :ivar str uuid: The UUID of the job.
    :ivar FallbackJobStatus status: The status of the job.
    :ivar FallbackJobResult | None result: The result of the job, if it has completed.
    """

    def __init__(
        self,
        client: "FallbackClient",
        uuid: str,
        status: FallbackJobStatus,
        result: FallbackJobResult | None = None,
        client_cert: bytes | None = None,
        private_key: RSAPrivateKey | bytes | None = None,
        streaming_port: str | None = None,
        proxy_pass_port: int = 80,
    ):
        self._client = client
        self.uuid = uuid
        self.status = status
        self.result = result
        self.client_cert = client_cert
        self.private_key = private_key
        self.streaming_server_certificate: bytes | None = None
        self.streaming_port = streaming_port
        self.streaming_server_ip: str | None = None
        self.streaming_server_port: str | None = None
        self.proxy_pass_port = proxy_pass_port

    async def wait(self, timeout: float | None = None) -> None:
        """
        Wait for this job to complete or fail.

        :param timeout: Maximum number of seconds to wait for.
        :raises FallbackJobTimeoutError: If the job does not complete within ``timeout`` seconds.
        """
        start_time = time.monotonic()

        while self.status.is_in_progress():
            if timeout is not None and time.monotonic() - start_time > timeout:
                raise FallbackJobTimeoutError(
                    f"Job {self.uuid} did not complete within {timeout} seconds, last status: {self.status}"
                )
            await asyncio.sleep(JOB_REFRESH_INTERVAL.total_seconds())
            await self.refresh()

    async def wait_for_streaming(self, timeout: int = 120) -> None:
        """
        Wait for the streaming to start.
        """
        if not self.client_cert or not self.private_key:
            raise ValueError("Client certificate and private key are required for streaming.")
        start_time = time.monotonic()
        temp_client_cert: IO[bytes] | None = None
        temp_client_key: IO[bytes] | None = None
        temp_server_cert: IO[bytes] | None = None
        server_cert_path = None
        try:
            if self.client_cert:
                temp_client_cert = tempfile.NamedTemporaryFile(delete=False, suffix=".crt")
                if isinstance(self.client_cert, bytes):
                    temp_client_cert.write(self.client_cert)
                else:
                    temp_client_cert.write(self.client_cert.public_bytes(serialization.Encoding.PEM))
                temp_client_cert.close()
            if self.private_key:
                temp_client_key = tempfile.NamedTemporaryFile(delete=False, suffix=".key")
                if isinstance(self.private_key, bytes):
                    temp_client_key.write(self.private_key)
                else:
                    temp_client_key.write(
                        self.private_key.private_bytes(
                            encoding=serialization.Encoding.PEM,
                            format=serialization.PrivateFormat.TraditionalOpenSSL,
                            encryption_algorithm=serialization.NoEncryption(),
                        )
                    )
                temp_client_key.close()

            while True:
                if timeout is not None and time.monotonic() - start_time > timeout:
                    raise FallbackJobTimeoutError(f"Streaming did not start within {timeout} seconds.")
                await asyncio.sleep(JOB_REFRESH_INTERVAL.total_seconds())
                await self.refresh()
                if not self.streaming_server_certificate:
                    cert = await self._client.get_streaming_server_certificate(self.uuid)
                    if not cert:
                        continue
                    try:
                        if cert is not None:
                            self.streaming_server_certificate = base64.b64decode(cert)
                    except Exception:
                        continue
                    temp_server_cert = tempfile.NamedTemporaryFile(delete=False, suffix=".crt")
                    if self.streaming_server_certificate is not None:
                        temp_server_cert.write(self.streaming_server_certificate)
                    temp_server_cert.close()
                    server_cert_path = temp_server_cert.name
                if not self.streaming_server_ip:
                    ip = await self._client.get_streaming_server_head_ip(self.uuid)
                    if not ip:
                        continue
                    if ip is not None:
                        self.streaming_server_ip = ip
                if not self.streaming_server_port:
                    if not server_cert_path or not os.path.exists(server_cert_path):
                        raise ValueError("Server certificate file path is not set or does not exist.")
                    port = await self._client.find_streaming_port(
                        self.uuid,
                        temp_client_cert.name if temp_client_cert is not None else "",
                        temp_client_key.name if temp_client_key is not None else "",
                        server_cert_path if server_cert_path else "",
                    )
                    if port is not None:
                        self.streaming_server_port = str(port)
                if self.streaming_server_certificate and self.streaming_server_ip and self.streaming_server_port:
                    break
        finally:
            if temp_client_cert is not None:
                os.unlink(temp_client_cert.name)
            if temp_client_key is not None:
                os.unlink(temp_client_key.name)
            if temp_server_cert is not None:
                os.unlink(temp_server_cert.name)

    async def refresh(self) -> None:
        """
        Refresh the current status and result of the job.
        """

        new_job = await self._client.get_job(self.uuid)
        self.status = new_job.status
        self.result = new_job.result

    def __repr__(self) -> str:
        return f"<{self.__class__.__qualname__}: {self.uuid!r}>"
