import asyncio
import base64
import dataclasses
import json
import logging
import random
import sys
import time
from collections.abc import AsyncIterator, Awaitable, Callable, Coroutine, Mapping, Sequence
from datetime import timedelta
from typing import Any, TypeAlias
from urllib.parse import urljoin

import bittensor_wallet
import httpx
import pydantic
import tenacity
from cryptography.hazmat.primitives.asymmetric.rsa import RSAPrivateKey
from cryptography.x509 import Certificate

from compute_horde_core.certificate import generate_certificate, serialize_certificate
from compute_horde_core.executor_class import ExecutorClass
from compute_horde_core.output_upload import HttpOutputVolumeResponse
from compute_horde_core.signature import (
    BittensorWalletSigner,
    SignatureScope,
    SignedFields,
    signature_to_headers,
)

from .exceptions import ComputeHordeError, ComputeHordeJobTimeoutError, ComputeHordeNotFoundError
from .models import (
    ComputeHordeJobResult,
    ComputeHordeJobStatus,
    FacilitatorJobResponse,
    FacilitatorJobsResponse,
    InputVolume,
    OutputVolume,
)

if sys.version_info >= (3, 11):  # noqa: UP036
    from typing import Self
else:
    from typing_extensions import Self  # noqa: UP035

logger = logging.getLogger(__name__)

JOB_REFRESH_INTERVAL = timedelta(seconds=3)

DEFAULT_FACILITATOR_URL = "https://facilitator.computehorde.io/"
DEFAULT_MAX_JOB_RUN_ATTEMPTS = 3
HTTP_RETRY_MAX_ATTEMPTS = 5
HTTP_RETRY_MIN_WAIT_SECONDS = 0.2
HTTP_RETRY_MAX_WAIT_SECONDS = 5
RETRYABLE_HTTP_EXCEPTIONS = (
    httpx.ConnectTimeout,
    httpx.PoolTimeout,
    # httpx.ReadTimeout,
    httpx.WriteTimeout,
    # httpx.ReadError,
    httpx.WriteError,
    httpx.ConnectError,
    # httpx.DecodingError,
)

DEFAULT_STREAMING_START_TIME_LIMIT_SEC = 5

JobAttemptCallbackType: TypeAlias = (
    Callable[["ComputeHordeJob"], None]
    | Callable[["ComputeHordeJob"], Awaitable[None]]
    | Callable[["ComputeHordeJob"], Coroutine[Any, Any, None]]
)


def _retryable_status_code(status_code: int) -> bool:
    return status_code == 429 or status_code >= 500


def _retryable_exception(exc: BaseException) -> bool:
    """Retry requests, if request failed for with a retryable exception"""
    if not isinstance(exc, ComputeHordeError):
        return False

    if isinstance(exc.__cause__, httpx.HTTPStatusError):
        return _retryable_status_code(exc.__cause__.response.status_code)

    return isinstance(exc.__cause__, RETRYABLE_HTTP_EXCEPTIONS)


@dataclasses.dataclass
class ComputeHordeJobSpec:
    """
    Specification of a job to run on the ComputeHorde.
    """

    executor_class: ExecutorClass
    """Class of the executor machine to run the job on."""

    job_namespace: str
    """
    Specifies where the job comes from.
    The recommended format is the subnet number and version, like e.g. ``"SN123.0"``.
    """

    docker_image: str
    """Docker image of the job, in the form of ``user/image:tag``."""

    download_time_limit_sec: int
    """
    Time dedicated to downloading job volumes to the executor machine.
    Part of the paid cost to run the job.
    If the limit is reached, the job will fail before starting execution.
    """

    execution_time_limit_sec: int
    """
    Time dedicated to executing the job.
    Part of the paid cost to run the job.
    This is only the upper time limit for the execution stage of the job. When this limit is reached, the job will be
    stopped, but it won't be considered failed - it will proceed to the upload stage anyway.
    """

    upload_time_limit_sec: int
    """
    Time dedicated to uploading the job's output.
    Part of the paid cost to run the job.
    If the limit is reached, the job will fail.
    """

    args: Sequence[str] = dataclasses.field(default_factory=list)
    """Positional arguments and flags to run the job with."""

    env: Mapping[str, str] = dataclasses.field(default_factory=dict)
    """Environment variables to run the job with."""

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

    streaming: bool = False
    """
    If true, the job will be streamed. The streaming server details
    (such as address, port, and SSL certificate) will be available
    in the ComputeHordeJob instance after the `wait_for_streaming()`
    method returns.
    """

    streaming_start_time_limit_sec: int = DEFAULT_STREAMING_START_TIME_LIMIT_SEC
    """
    Time dedicated to starting the streaming server.
    Part of the paid cost to run the job.
    If the limit is reached, the job will fail.
    """


class ComputeHordeJob:
    """
    The class representing a job running on the ComputeHorde.
    Do not construct it directly, always use :class:`ComputeHordeClient`.

    :ivar str uuid: The UUID of the job.
    :ivar ComputeHordeJobStatus status: The status of the job.
    :ivar ComputeHordeJobResult | None result: The result of the job, if it has completed.
    :ivar str | None streaming_server_cert: The PEM-encoded certificate of the streaming server, if available.
    """

    def __init__(
        self,
        client: "ComputeHordeClient",
        uuid: str,
        status: ComputeHordeJobStatus,
        result: ComputeHordeJobResult | None = None,
        streaming_public_cert: Certificate | None = None,
        streaming_private_key: RSAPrivateKey | None = None,
        streaming_server_cert: str | None = None,
        streaming_server_address: str | None = None,
        streaming_server_port: int | None = None,
    ):
        self._client = client
        self.uuid = uuid
        self.status = status
        self.result = result
        self.streaming_public_cert = streaming_public_cert
        self.streaming_private_key = streaming_private_key
        self.streaming_server_cert = streaming_server_cert
        self.streaming_server_address = streaming_server_address
        self.streaming_server_port = streaming_server_port

    async def wait(self, timeout: float | None = None) -> None:
        """
        Wait for this job to complete or fail.

        :param timeout: Maximum number of seconds to wait for.
        :raises ComputeHordeJobTimeoutError: If the job does not complete within ``timeout`` seconds.
        """
        start_time = time.monotonic()

        while self.status.is_in_progress():
            if timeout is not None and time.monotonic() - start_time > timeout:
                raise ComputeHordeJobTimeoutError(
                    f"Job {self.uuid} did not complete within {timeout} seconds, last status: {self.status}"
                )
            await asyncio.sleep(JOB_REFRESH_INTERVAL.total_seconds())
            await self.refresh_from_facilitator()

    async def wait_for_streaming(self, timeout: float | None = None) -> None:
        """
        Wait for the job to be ready for streaming.

        :param timeout: Maximum number of seconds to wait for.
        :raises ComputeHordeJobTimeoutError: If the job does not prepare for streaming within ``timeout`` seconds.
        """
        start_time = time.monotonic()
        while True:
            if timeout is not None and time.monotonic() - start_time > timeout:
                raise ComputeHordeJobTimeoutError(
                    f"Job {self.uuid} did not prepare for streaming within {timeout} seconds,"
                    f" last status: {self.status}"
                )
            await asyncio.sleep(JOB_REFRESH_INTERVAL.total_seconds())
            await self.refresh_from_facilitator()
            if self.status.is_streaming_ready():
                return

    async def refresh_from_facilitator(self) -> None:
        new_job = await self._client.get_job(self.uuid)
        self.status = new_job.status
        self.result = new_job.result
        self.streaming_server_cert = new_job.streaming_server_cert
        self.streaming_server_address = new_job.streaming_server_address
        self.streaming_server_port = new_job.streaming_server_port

    def __repr__(self) -> str:
        return f"<{self.__class__.__qualname__}: {self.uuid!r}>"

    @classmethod
    def _from_response(
        cls,
        client: "ComputeHordeClient",
        response: FacilitatorJobResponse,
        streaming_public_cert: Certificate | None = None,
        streaming_private_key: RSAPrivateKey | None = None,
    ) -> Self:
        result = None
        if not response.status.is_in_progress():
            # TODO: Handle base64 decode errors
            result = ComputeHordeJobResult(
                stdout=response.stdout,
                stderr=response.stderr,
                artifacts={path: base64.b64decode(base64_data) for path, base64_data in response.artifacts.items()},
            )
            for path, raw_data in response.upload_results.items():
                try:
                    result.add_upload_result(path, HttpOutputVolumeResponse.parse_raw(raw_data))
                except Exception:
                    logger.error(f"Failed to parse upload result for '{path}'", exc_info=True)
        return cls(
            client,
            uuid=response.uuid,
            status=response.status,
            result=result,
            streaming_public_cert=streaming_public_cert,
            streaming_private_key=streaming_private_key,
            streaming_server_cert=response.streaming_server_cert,
            streaming_server_address=response.streaming_server_address,
            streaming_server_port=response.streaming_server_port,
        )


class ComputeHordeClient:
    """
    The class used to communicate with the ComputeHorde.
    """

    def __init__(
        self,
        hotkey: bittensor_wallet.Keypair,
        compute_horde_validator_hotkey: str,
        job_queue: str | None = None,
        facilitator_url: str = DEFAULT_FACILITATOR_URL,
    ):
        """
        Create a new ComputeHorde client.

        :param hotkey: Your wallet hotkey, used for signing requests.
        :param compute_horde_validator_hotkey: The hotkey for the validator that your jobs should go to.
        :param job_queue: A user-defined string value that will allow for retrieving all pending jobs,
            for example after process restart, relevant to this process.
        :param facilitator_url: The URL to use for the ComputeHorde Facilitator API.
        """
        self.hotkey = hotkey
        self.compute_horde_validator_hotkey = compute_horde_validator_hotkey
        self.job_queue = job_queue
        self.facilitator_url = facilitator_url
        self._client = httpx.AsyncClient(base_url=self.facilitator_url, follow_redirects=True)
        self._signer = BittensorWalletSigner(hotkey)
        self._token_lock = asyncio.Lock()
        self._token: str | None = None
        self.streaming_public_cert: Certificate | None = None
        self.streaming_private_key: RSAPrivateKey | None = None

    async def authenticate(self) -> None:
        nonce_url = urljoin(self.facilitator_url, "auth/nonce")
        try:
            response = await self._client.get(nonce_url)
            response.raise_for_status()
        except (httpx.HTTPError, httpx.HTTPStatusError) as e:
            raise ComputeHordeError(f"Nonce request failed: {e}") from e

        try:
            data = response.json()
            nonce = data.get("nonce")
        except Exception as e:
            raise ComputeHordeError("Failed to parse nonce response") from e

        if not nonce:
            raise ComputeHordeError("Failed to obtain nonce from facilitator")

        signature = self.hotkey.sign(nonce.encode("utf-8")).hex()

        login_url = urljoin(self.facilitator_url, "auth/login")
        payload = {"hotkey": self.hotkey.ss58_address, "signature": signature, "nonce": nonce}

        try:
            login_response = await self._client.post(login_url, json=payload)
            login_response.raise_for_status()
        except (httpx.HTTPError, httpx.HTTPStatusError) as e:
            raise ComputeHordeError(f"Login request failed: {e}") from e

        try:
            data = login_response.json()
            token = data.get("token")
        except Exception as e:
            raise ComputeHordeError("Failed to parse login response") from e

        if not token:
            raise ComputeHordeError("Failed to obtain JWT token from facilitator")
        self._token = token
        logger.info("Authenticated successfully")

    @tenacity.retry(
        retry=tenacity.retry_if_exception(_retryable_exception),
        stop=tenacity.stop_after_attempt(HTTP_RETRY_MAX_ATTEMPTS),
        wait=tenacity.wait_exponential_jitter(initial=HTTP_RETRY_MIN_WAIT_SECONDS, max=HTTP_RETRY_MAX_WAIT_SECONDS),
        reraise=True,
    )
    async def _make_request(
        self,
        method: str,
        url: str,
        *,
        json: dict[str, Any] | None = None,
        params: Mapping[str, str | int] | None = None,
        headers: Mapping[str, str] | None = None,
    ) -> str:
        """
        :return: Response content as string.
        """
        # If we receive a 401 token expired error, we will re-authenticate and immediately retry the failed request.
        max_attempts = 2
        for attempt in range(max_attempts):
            async with self._token_lock:
                if not self._token:
                    await self.authenticate()
                auth_headers = self._get_authentication_headers()

            request = self._client.build_request(
                method=method,
                url=url,
                json=json,
                params=params,
                headers={**headers, **auth_headers} if headers else auth_headers,
            )
            logger.debug("%s %s", method, request.url)
            try:
                response = await self._client.send(request)
            except httpx.HTTPError as e:
                raise ComputeHordeError(f"ComputeHorde request failed: {e}") from e

            try:
                response.raise_for_status()
            except httpx.HTTPStatusError as e:
                logger.error("Response status=%d: %s", e.response.status_code, e.response.text)
                if e.response.status_code == 401 and "Token expired" in e.response.text and attempt == 0:
                    async with self._token_lock:
                        self._token = None
                    logger.info("Token expired, re-authenticating")
                    continue
                if e.response.status_code == 404:
                    raise ComputeHordeNotFoundError(f"Resource under {request.url} not found") from e
                raise ComputeHordeError(
                    f"ComputeHorde responded with status code {e.response.status_code} and text {response.text}"
                ) from e

            return response.text
        raise ComputeHordeError("ComputeHorde request failed after re-authentication.")

    def _get_signature_headers(self, data: dict[str, pydantic.JsonValue]) -> dict[str, str]:
        signed_fields = SignedFields.from_facilitator_sdk_json(data)
        signature = self._signer.sign(payload=signed_fields.model_dump_json())
        return signature_to_headers(signature, SignatureScope.SignedFields)

    def _get_cheated_job_headers(self, data: dict[str, str]) -> dict[str, str]:
        payload = json.dumps(data, sort_keys=True)
        signature = self._signer.sign(payload=payload)
        return signature_to_headers(signature, SignatureScope.FullRequest)

    def _get_authentication_headers(self) -> dict[str, str]:
        if not self._token:
            raise ComputeHordeError("Not authenticated")
        return {
            "Authorization": f"Bearer {self._token}",
            "Realm": "mainnet",
            "SubnetID": "12",
        }

    async def report_cheated_job(self, job_uuid: str) -> str:
        """
        Reports to validator that a miner has cheated on a job.

        :param job_uuid: The UUID of the job that was cheated on.
        """
        data = {"job_uuid": job_uuid}
        signature_headers = self._get_cheated_job_headers(data)
        response = await self._make_request("POST", "/api/v1/cheated-job/", json=data, headers=signature_headers)
        logger.debug("Reported job %s", job_uuid)
        return response

    async def create_job(self, job_spec: ComputeHordeJobSpec, on_trusted_miner: bool = False) -> ComputeHordeJob:
        """
        Run a job in the ComputeHorde. This method does not retry a failed job.
        Use :meth:`run_until_complete` if you want failed jobs to be automatically retried.

        :param job_spec: Job specification to run.
        :param on_trusted_miner: If true, the job will be run on the sn12 validator's trusted miner.
        :return: A :class:`ComputeHordeJob` class instance representing the created job.
        """

        # TODO: make this a pydantic model?
        data: dict[str, pydantic.JsonValue] = {
            "target_validator_hotkey": self.compute_horde_validator_hotkey,
            "executor_class": job_spec.executor_class,
            "docker_image": job_spec.docker_image,
            "args": job_spec.args or [],  # type: ignore
            "env": job_spec.env or {},  # type: ignore
            "use_gpu": True,
            "artifacts_dir": job_spec.artifacts_dir,
            "on_trusted_miner": on_trusted_miner,
            "download_time_limit": job_spec.download_time_limit_sec,
            "execution_time_limit": job_spec.execution_time_limit_sec,
            "streaming_start_time_limit": job_spec.streaming_start_time_limit_sec,
            "upload_time_limit": job_spec.upload_time_limit_sec,
        }
        if job_spec.streaming:
            self.streaming_public_cert, self.streaming_private_key = generate_certificate("127.0.0.1")
            data["streaming_details"] = {
                "public_key": serialize_certificate(self.streaming_public_cert).decode("utf-8"),
            }
        if job_spec.input_volumes is not None:
            data["volumes"] = [
                input_volume.to_compute_horde_volume(mount_path).model_dump()
                for mount_path, input_volume in job_spec.input_volumes.items()
            ]
        if job_spec.output_volumes is not None:
            data["uploads"] = [
                output_volume.to_compute_horde_output_upload(mount_path).model_dump()
                for mount_path, output_volume in job_spec.output_volumes.items()
            ]

        logger.debug("Creating job from image %s", job_spec.docker_image)
        signature_headers = self._get_signature_headers(data)

        response = await self._make_request("POST", "/api/v1/job-docker/", json=data, headers=signature_headers)

        try:
            job_response = FacilitatorJobResponse.model_validate_json(response)
        except pydantic.ValidationError as e:
            raise ComputeHordeError("ComputeHorde returned malformed response") from e

        job = ComputeHordeJob._from_response(self, job_response, self.streaming_public_cert, self.streaming_private_key)
        logger.debug("Created job with UUID=%s", job.uuid)

        return job

    async def run_until_complete(
        self,
        job_spec: ComputeHordeJobSpec,
        on_trusted_miner: bool = False,
        job_attempt_callback: JobAttemptCallbackType | None = None,
        timeout: float | None = None,
        max_attempts: int = DEFAULT_MAX_JOB_RUN_ATTEMPTS,
    ) -> ComputeHordeJob:
        """
        Run a job in the ComputeHorde until it is successful.
        It will call :meth:`create_job` repeatedly until the job is successful.

        :param job_spec: Job specification to run.
        :param on_trusted_miner: If true, the job will be run on the sn12 validator's trusted miner.
        :param job_attempt_callback: A callback function that will be called after every attempt of running the job.
            The callback will be called immediately after an attempt is made run the job,
            before waiting for the job to complete.
            The function must take one argument of type ComputeHordeJob.
            It can be a regular or an async function.
        :param timeout: Maximum number of seconds to wait for.
        :param max_attempts: Maximum number times the job will be attempted to run within ``timeout`` seconds.
            Negative or ``0`` means unlimited attempts.
        :return: A :class:`ComputeHordeJob` class instance representing the created job.
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
            logger.info(f"Attempting to run job [{attempt_msg}]")

            job = await self.create_job(job_spec, on_trusted_miner)

            if job_attempt_callback:
                maybe_coro = job_attempt_callback(job)
                if asyncio.iscoroutine(maybe_coro):
                    await maybe_coro

            await job.wait(timeout=remaining_timeout())

            if job.status.is_successful():
                return job

            if max_attempts > 0 and attempt >= max_attempts:
                return job

            # Apply exponential backoff with jitter before retrying
            backoff_seconds = min(HTTP_RETRY_MIN_WAIT_SECONDS * (2 ** (attempt - 1)), HTTP_RETRY_MAX_WAIT_SECONDS)
            jitter = random.uniform(0, backoff_seconds * 0.1)
            wait_time = backoff_seconds + jitter
            logger.info(f"Job attempt {attempt_msg} failed, waiting {wait_time:.2f} seconds before retry")
            await asyncio.sleep(wait_time)

    async def get_job(self, job_uuid: str) -> ComputeHordeJob:
        """
        Retrieve information about a job from the ComputeHorde.

        :param job_uuid: The UUID of the job to retrieve.
        :return: A :class:`ComputeHordeJob` instance representing this job.
        :raises ComputeHordeNotFoundError: If the job with this UUID does not exist.
        """
        logger.debug("Fetching job with UUID=%s", job_uuid)

        response = await self._make_request("GET", f"/api/v1/jobs/{job_uuid}/")
        try:
            job_response = FacilitatorJobResponse.model_validate_json(response)
        except pydantic.ValidationError as e:
            raise ComputeHordeError("ComputeHorde returned malformed response") from e

        return ComputeHordeJob._from_response(self, job_response)

    async def _get_jobs_page(self, page: int = 1, page_size: int = 10) -> FacilitatorJobsResponse:
        params = {"page": page, "page_size": page_size}

        response = await self._make_request("GET", "/api/v1/jobs/", params=params)

        try:
            jobs_response = FacilitatorJobsResponse.model_validate_json(response)
        except pydantic.ValidationError as e:
            raise ComputeHordeError("ComputeHorde returned malformed response") from e

        return jobs_response

    async def get_jobs(self, page: int = 1, page_size: int = 10) -> list[ComputeHordeJob]:
        """
        Retrieve information about your jobs from the ComputeHorde.

        :param page: The page number.
        :param page_size: The page size.
        :return: A list of :class:`ComputeHordeJob` instances representing your jobs.
        :raises ComputeHordeNotFoundError: If the requested page does not exist.
        """
        logger.debug("Fetching jobs page=%d, page_size%d", page, page_size)
        jobs_response = await self._get_jobs_page(page=page, page_size=page_size)

        return [ComputeHordeJob._from_response(self, job) for job in jobs_response.results]

    async def iter_jobs(self) -> AsyncIterator[ComputeHordeJob]:
        """
        Retrieve information about your jobs from the ComputeHorde.

        :return: An async iterator of :class:`ComputeHordeJob` instances representing your jobs.

        Usage::

            async for job in client.iter_jobs():
                process(job)
        """
        logger.debug("Iterating over jobs.")
        page = 1
        page_size = 10
        has_next_page = True
        while has_next_page:
            jobs_response = await self._get_jobs_page(page=page, page_size=page_size)
            for job_response in jobs_response.results:
                yield ComputeHordeJob._from_response(self, job_response)

            has_next_page = jobs_response.next is not None
            page += 1
