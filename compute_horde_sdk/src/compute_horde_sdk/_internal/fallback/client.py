import asyncio
import logging
import pathlib
import tempfile
import textwrap
import time
from collections.abc import AsyncIterator, Awaitable, Callable, Coroutine
from typing import TYPE_CHECKING, Any, TypeAlias

import requests
from cryptography.hazmat.primitives.asymmetric.rsa import RSAPrivateKey

from compute_horde_core.certificate import generate_certificate, serialize_certificate
from compute_horde_sdk._internal.models import InlineInputVolume

from .adaptors.utils import lazy_import_adaptor
from .exceptions import FallbackNotFoundError
from .job import FallbackJob, FallbackJobResult, FallbackJobSpec, FallbackJobStatus
from .utils import PackageAnalyzer, change_dir

if TYPE_CHECKING:
    from .adaptors.sky import SkyCloud as SkyCloudType
    from .adaptors.sky import SkyJob as SkyJobType

logger = logging.getLogger(__name__)

sky = lazy_import_adaptor("sky")

JobAttemptCallbackType: TypeAlias = (
    Callable[["FallbackJob"], None]
    | Callable[["FallbackJob"], Awaitable[None]]
    | Callable[["FallbackJob"], Coroutine[Any, Any, None]]
)

_SETUP_TMPL = """
#!/bin/bash

set -euo pipefail

ARTIFACTS_DIR="{artifacts_dir}"
VOLUME_DIR="/volume"
OUTPUT_UPLOAD_DIR="/output"
[ -n "$ARTIFACTS_DIR" ] && rm -rf "$ARTIFACTS_DIR" && mkdir -p "$ARTIFACTS_DIR"
rm -rf "$VOLUME_DIR" && mkdir -p "$VOLUME_DIR"
rm -rf "$OUTPUT_UPLOAD_DIR" && mkdir -p "$OUTPUT_UPLOAD_DIR"
{command}
"""

_RUN_TMPL = """
#!/bin/bash

set -euo pipefail
shopt -s nullglob

volumes=(volume-*.json)
if (( ${{#volumes[@]}} )); then
  python3 -m compute_horde_core.volume ${{volumes[@]}} --dir /volume > ./ch-volume.log 2>&1
fi

pushd "{workdir}" > /dev/null
{streaming_setup}
{command}
popd > /dev/null

output_uploads=(output_upload-*.json)
if (( ${{#output_uploads[@]}} )); then
  python3 -m compute_horde_core.output_upload ${{output_uploads[@]}} --dir /output > ./ch-output_upload.log 2>&1
fi
"""

STREAMING_SETUP_BASH_TEMPLATE = """
rm -rf /streaming_server_data
rm -rf /etc/nginx/ssl
mkdir -p /streaming_server_data

streaming_setup() {
  if command -v apt-get >/dev/null 2>&1; then
    apt-get install -y nginx openssl
  elif command -v apk >/dev/null 2>&1; then
    apk add --no-cache nginx openssl
  elif command -v yum >/dev/null 2>&1; then
    yum install -y nginx openssl
  fi
  mkdir -p /etc/nginx/ssl
  cp /volume/client.crt /etc/nginx/ssl/client.crt

  # Get public IP
  PUBLIC_IP=$(curl -s https://api.ipify.org)
  echo "Public IP: $PUBLIC_IP"

  # Create OpenSSL config
  cat > /etc/nginx/ssl/openssl-san.cnf <<EOF
[req]
distinguished_name = req_distinguished_name
req_extensions = v3_req
prompt = no

[req_distinguished_name]
CN = $PUBLIC_IP

[v3_req]
subjectAltName = @alt_names

[alt_names]
IP.1 = $PUBLIC_IP
EOF

  openssl req -x509 -nodes -days 365 -newkey rsa:2048 \
    -keyout /etc/nginx/ssl/private_key.pem \
    -out /etc/nginx/ssl/certificate.pem \
    -config /etc/nginx/ssl/openssl-san.cnf \
    -extensions v3_req

  cp /etc/nginx/ssl/certificate.pem /streaming_server_data/certificate.pem

  cat > /etc/nginx/nginx.conf <<'EOF'
user  root;
events {}
http {
  server {
    listen %(streaming_port)s ssl;
    ssl_certificate /etc/nginx/ssl/certificate.pem;
    ssl_certificate_key /etc/nginx/ssl/private_key.pem;
    ssl_client_certificate /etc/nginx/ssl/client.crt;
    ssl_verify_client on;
    location / {
      proxy_pass http://localhost:%(proxy_pass_port)s;
    }
    location /health {
      access_log off;
      add_header 'Content-Type' 'application/json';
      return 200 '{"status":"Healthy"}';
    }
  }
}
EOF

  nginx -g 'daemon off;' &
}

streaming_setup > /streaming_server_data/streaming_setup.log 2>&1
"""


class FallbackClient:
    """
    A fallback client that provides the same API as ComputeHordeClient.
    """

    DEFAULT_MAX_JOB_RUN_ATTEMPTS = 3
    MAX_ARTIFACT_SIZE = 1_000_000

    def __init__(self, cloud: str, idle_minutes: int = 15, **kwargs: Any) -> None:
        try:
            self.cloud: SkyCloudType = sky.SkyCloud(cloud, **kwargs)
        except ModuleNotFoundError:
            raise ModuleNotFoundError(
                "The fallback extra has not been installed. "
                "Please install it with `pip install compute-horde-sdk[fallback]`."
            ) from None
        self.idle_minutes = idle_minutes

        self._jobs: dict[str, SkyJobType] = {}
        self.streaming_port: str | None = None

    async def create_job(self, job_spec: FallbackJobSpec) -> FallbackJob:
        """
        Run a fallback job in the SkyPilot cluster. This method does not retry a failed job.
        Use :meth:`run_until_complete` if you want failed jobs to be automatically retried.

        :param job_spec: Job specification to run.
        :return: A :class:`FallbackJob` class instance representing the created job.
        """
        logger.info("Running fallback job...")
        logger.debug("Fallback job spec: %s", job_spec)

        # If streaming, generate client cert and add as input volume
        client_cert: bytes | None = None
        private_key: RSAPrivateKey | bytes | None = None

        def _find_available_port(used_ports: list[int] | None, start: int = 40000) -> str:
            port = start
            used_ports_set = set(used_ports or [])
            while port in used_ports_set:
                port += 1
            return str(port)

        if job_spec.streaming:
            normalized_ports: list[str] = []
            if isinstance(job_spec.ports, int):
                normalized_ports = [str(job_spec.ports)]
            elif isinstance(job_spec.ports, str):
                normalized_ports = [job_spec.ports]
            elif isinstance(job_spec.ports, (list, tuple)):
                normalized_ports = [str(p) for p in job_spec.ports]
            elif job_spec.ports is None:
                normalized_ports = []
            self.streaming_port = _find_available_port([int(p) for p in normalized_ports if p.isdigit()])
            if not normalized_ports:
                normalized_ports = [self.streaming_port]
            elif self.streaming_port not in normalized_ports:
                normalized_ports.append(self.streaming_port)
            job_spec.ports = normalized_ports
            # Generate client certificate and private key
            cert, key = generate_certificate("localhost")
            cert_bytes = serialize_certificate(cert)
            if job_spec.input_volumes is None or not isinstance(job_spec.input_volumes, dict):
                job_spec.input_volumes = {}
            job_spec.input_volumes["/volume/"] = InlineInputVolume.from_file_contents(
                filename="client.crt", contents=cert_bytes
            )
            client_cert = cert_bytes
            private_key = key

        workdir = self._prepare_workdir()
        setup = self._prepare_setup(workdir, job_spec)
        run = self._prepare_run(workdir, job_spec, self.streaming_port)

        sky_job: SkyJobType = sky.SkyJob(
            cloud=self.cloud,
            workdir=workdir,
            setup=setup,
            run=run,
            envs=job_spec.envs,
            artifacts_dir=job_spec.artifacts_dir,
            accelerators=job_spec.accelerators,
            cpus=job_spec.cpus,
            memory=job_spec.memory,
            disk_size=job_spec.disk_size,
            ports=job_spec.ports,
            instance_type=job_spec.instance_type,
            image_id=job_spec.image_id,
            region=job_spec.region,
            zone=job_spec.zone,
        )
        sky_job.submit(idle_minutes=self.idle_minutes)
        self._jobs[sky_job.job_uuid] = sky_job

        job = FallbackJob(
            self,
            uuid=sky_job.job_uuid,
            status=FallbackJobStatus.SENT,
            client_cert=client_cert,
            private_key=private_key,
            streaming_port=self.streaming_port,
        )
        logger.info("The job has been submitted: %s", job)

        return job

    async def run_until_complete(
        self,
        job_spec: FallbackJobSpec,
        job_attempt_callback: JobAttemptCallbackType | None = None,
        timeout: float | None = None,
        max_attempts: int = DEFAULT_MAX_JOB_RUN_ATTEMPTS,
    ) -> FallbackJob:
        """
        Self.
        Run a fallback job in the SkyPilot cluster until it is successful.
        It will call :meth:`create_job` repeatedly until the job is successful.

        :param job_spec: Job specification to run.
        :param job_attempt_callback: A callback function that will be called after every attempt of running the job.
            The callback will be called immediately after an attempt is made run the job,
            before waiting for the job to complete.
            The function must take one argument of type FallbackJob.
            It can be a regular or an async function.
        :param timeout: Maximum number of seconds to wait for.
        :param max_attempts: Maximum number times the job will be attempted to run within ``timeout`` seconds.
            Negative or ``0`` means unlimited attempts.
        :return: A :class:`FallbackJob` class instance representing the created job.
            If the job was rerun, it will represent the last attempt.
        """
        start_time = time.monotonic()

        def remaining_timeout() -> float | None:
            if timeout is None:
                return None
            new_timeout = timeout - (time.monotonic() - start_time)
            return max(new_timeout, 0)

        attempt = 0
        while True:
            attempt += 1
            attempt_msg = f"{attempt}/{max_attempts}" if max_attempts > 0 else f"{attempt}"
            logger.info("Attempting to run job [%s]", attempt_msg)

            job = await self.create_job(job_spec)

            if job_attempt_callback:
                maybe_coro = job_attempt_callback(job)
                if asyncio.iscoroutine(maybe_coro):
                    await maybe_coro

            await job.wait(timeout=remaining_timeout())

            if job.status.is_successful():
                return job

            if 0 < max_attempts <= attempt:
                return job

    async def get_job(self, job_uuid: str) -> FallbackJob:
        """
        Retrieve information about a job from the SkyPilot cluster.

        :param job_uuid: The UUID of the job to retrieve.
        :return: A :class:`FallbackJob` instance representing this job.
        :raises FallbackNotFoundError: If the job with this UUID does not exist.
        """
        job = self._jobs.get(job_uuid)
        if job is None:
            raise FallbackNotFoundError(f"Job with UUID {job_uuid} not found")

        status = self._get_status(job)
        logger.debug("Fallback job status: %s", status)
        if not status.is_in_progress():
            result = self._get_result(job)
        else:
            result = None

        return FallbackJob(
            self,
            uuid=job_uuid,
            status=status,
            result=result,
        )

    async def get_jobs(self) -> list[FallbackJob]:
        """
        Retrieve information about your jobs from the SkyPilot cluster.

        :return: A list of :class:`FallbackJob` instances representing your jobs.
        """
        return [j async for j in self.iter_jobs()]

    async def iter_jobs(self) -> AsyncIterator[FallbackJob]:
        """
        Retrieve information about your jobs from the ComputeHorde.

        :return: An async iterator of :class:`FallbackJob` instances representing your jobs.
        """
        for job_uuid in self._jobs.keys():
            yield await self.get_job(job_uuid)

    async def get_streaming_server_certificate(self, job_uuid: str) -> bytes | None:
        """
        Retrieve the streaming server certificate for a job.
        """
        job = self._jobs[job_uuid]
        path = "/streaming_server_data"
        server_data = job.download(path, max_size=self.MAX_ARTIFACT_SIZE)
        return server_data.get("/streaming_server_data/certificate.pem")

    async def get_streaming_server_head_ip(self, job_uuid: str) -> str | None:
        """
        Retrieve the head IP of the streaming server for a streaming_portjob.
        """
        job = self._jobs[job_uuid]
        ip = job.get_job_head_ip()
        logger.debug("Streaming server head IP: %s", ip)
        return ip

    async def get_job_streaming_port(self, job_uuid: str) -> int | None:
        """
        Retrieve the SSH ports of the job.
        """
        job = self._jobs[job_uuid]
        if not self.streaming_port:
            return None
        return job.get_job_ssh_port(self.streaming_port)

    async def find_streaming_port(
        self,
        job_uuid: str,
        client_cert_path: str,
        client_key_path: str,
        server_cert_path: str,
        max_ports_to_scan: int = 10,
        timeout: int = 2,
    ) -> int | None:
        """
        Scan ports above the SSH port to find the streaming port by probing /health.
        Returns the port if found, else None.

        For some reason SkyPilot does not provide all the ports in the job resource handle when using RunPod as a
        backend. So we need to scan the ports above the base SSH port to find the streaming port.
        """
        head_ip = await self.get_streaming_server_head_ip(job_uuid)
        if not head_ip:
            logger.error("Could not get head_ip for job %s", job_uuid)
            return None
        port = await self.get_job_streaming_port(job_uuid)
        if not port:
            logger.error("Could not get streaming port for job %s", job_uuid)
            return None

        url = f"https://{head_ip}:{port}/health"
        try:
            response = requests.get(
                url,
                cert=(client_cert_path, client_key_path),
                verify=server_cert_path,
                timeout=timeout,
                headers={"Cache-Control": "no-cache", "Pragma": "no-cache"},
            )
            if response.status_code == 200 and response.text.strip() == '{"status":"Healthy"}':
                logger.info(f"Streaming server is running on: {head_ip}:{port}")
                return port
        except Exception as e:
            logger.error("Error: %s", e)
        logger.error(f"Streaming server is not running on: {head_ip}:{port} for job {job_uuid}")
        return None

    @classmethod
    def _prepare_workdir(cls) -> pathlib.Path:
        workdir = pathlib.Path(tempfile.mkdtemp(prefix="ch-"))
        logger.debug("Working directory: %s", workdir)

        return workdir

    @classmethod
    def _prepare_setup(cls, workdir: pathlib.Path, job_spec: FallbackJobSpec) -> str:
        script = "./setup.sh"
        with change_dir(workdir):
            pa = PackageAnalyzer("compute-horde-sdk")
            source = pa.to_source()
            logger.debug("ComputeHorde SDK installable: %s", source)

            setup_sh = pathlib.Path(script)
            # TODO(maciek): use already preinstalled uv (by SkyPilot) for managing python and virtualenv
            setup_sh.write_text(
                _SETUP_TMPL.format(
                    command=f"pip install --force-reinstall {source}", artifacts_dir=job_spec.artifacts_dir or ""
                )
            )
            setup_sh.chmod(0o755)

        return script

    @classmethod
    def _prepare_run(cls, workdir: pathlib.Path, job_spec: FallbackJobSpec, streaming_port: str | None) -> str:
        script = "./run.sh"
        with change_dir(workdir):
            if job_spec.input_volumes is not None:
                for index, input_volume in enumerate(job_spec.input_volumes):
                    volume_json = pathlib.Path(f"./volume-{index}.json")
                    volume_json.write_text(
                        job_spec.input_volumes[input_volume].to_compute_horde_volume(input_volume).json()
                    )
            if job_spec.output_volumes is not None:
                for index, output_volume in enumerate(job_spec.output_volumes):
                    output_upload_json = pathlib.Path(f"./output_upload-{index}.json")
                    output_upload_json.write_text(
                        job_spec.output_volumes[output_volume].to_compute_horde_output_upload(output_volume).json()
                    )

            run_sh = pathlib.Path(script)
            main_command = job_spec.run
            streaming_setup = ""
            if job_spec.streaming:
                streaming_setup = textwrap.dedent(STREAMING_SETUP_BASH_TEMPLATE) % {
                    "streaming_port": streaming_port,
                    "proxy_pass_port": job_spec.proxy_pass_port,
                    "workdir": job_spec.work_dir,
                }
            run_script = _RUN_TMPL.format(
                command=main_command, workdir=job_spec.work_dir, streaming_setup=streaming_setup
            )
            run_sh.write_text(run_script)
            run_sh.chmod(0o755)
        return script

    def _get_status(self, job: "SkyJobType") -> FallbackJobStatus:
        status = job.status()

        if status is None:
            return FallbackJobStatus.SENT
        elif status in {
            sky.SkyJobStatus.INIT,
            sky.SkyJobStatus.PENDING,
            sky.SkyJobStatus.SETTING_UP,
            sky.SkyJobStatus.RUNNING,
        }:
            return FallbackJobStatus.ACCEPTED
        elif status in {sky.SkyJobStatus.FAILED_DRIVER, sky.SkyJobStatus.FAILED_SETUP}:
            return FallbackJobStatus.REJECTED
        elif status == sky.SkyJobStatus.SUCCEEDED:
            return FallbackJobStatus.COMPLETED
        elif status == sky.SkyJobStatus.FAILED:
            return FallbackJobStatus.FAILED
        else:
            raise NotImplementedError(f"Unsupported status: {status}")

    def _get_result(self, job: "SkyJobType") -> FallbackJobResult:
        stdout = job.output()
        if job.artifacts_dir is not None:
            artifacts = job.download(job.artifacts_dir, max_size=self.MAX_ARTIFACT_SIZE)
        else:
            artifacts = {}

        return FallbackJobResult(stdout=stdout, artifacts=artifacts)
