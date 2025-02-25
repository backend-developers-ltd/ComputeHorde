import abc
import asyncio
import logging
import time
import typing

import httpx
from compute_horde.executor_class import DEFAULT_EXECUTOR_CLASS, ExecutorClass  # type: ignore
from compute_horde.fv_protocol.facilitator_requests import SignedFields, to_json_array
from compute_horde.signature import Signer, signature_to_headers
from pydantic import JsonValue

from compute_horde_facilitator_sdk._internal.api_models import (
    JobFeedback,
    JobState,
    SingleFileUpload,
    Volume,
    is_in_progress,
)
from compute_horde_facilitator_sdk._internal.exceptions import (
    FacilitatorClientTimeoutException,
    SignatureRequiredException,
)

BASE_URL = "https://facilitator.computehorde.io/api/v1/"

logger = logging.getLogger(__name__)


HTTPClientType = typing.TypeVar("HTTPClientType", bound=httpx.Client | httpx.AsyncClient)
HTTPResponseType = typing.TypeVar("HTTPResponseType", bound=httpx.Response | typing.Awaitable[httpx.Response])


class FacilitatorClientBase(abc.ABC, typing.Generic[HTTPClientType, HTTPResponseType]):
    def __init__(
        self,
        token: str,
        base_url: str = BASE_URL,
        signer: Signer | None = None,
    ):
        self.base_url = base_url
        self.token = token
        self.signer = signer
        self.client: HTTPClientType = self._get_client()

    @abc.abstractmethod
    def _get_client(self) -> HTTPClientType:
        raise NotImplementedError

    def _prepare_request(
        self,
        method: str,
        url: str,
        *,
        json: JsonValue | None = None,
        params: dict[str, str | int] | None = None,
    ) -> HTTPResponseType:
        request = self.client.build_request(method=method, url=url, json=json, params=params)
        if self.signer and json:
            try:
                signed_fields = SignedFields.from_facilitator_sdk_json(json)
                signature = self.signer.sign(payload=signed_fields.model_dump_json())
                signature_headers = signature_to_headers(signature)
                request.headers.update(signature_headers)
            except Exception:
                logger.warning(f"No valid fields to sign in json: {json}")

        return typing.cast(HTTPResponseType, self.client.send(request, follow_redirects=True))

    def _require_signer(self):
        if not self.signer:
            raise SignatureRequiredException(
                "This operation requires request signing. Initialize the client with a `signer` parameter."
            )

    def _get_jobs(self, page: int = 1, page_size: int = 10) -> HTTPResponseType:
        return self._prepare_request("GET", "/jobs/", params={"page": page, "page_size": page_size})

    def _get_job(self, job_uuid: str) -> HTTPResponseType:
        return self._prepare_request("GET", f"/jobs/{job_uuid}/")

    def _create_raw_job(
        self,
        raw_script: str,
        input_url: str = "",
        uploads: list[SingleFileUpload] | None = None,
        volumes: list[Volume] | None = None,
    ) -> HTTPResponseType:
        data: dict[str, JsonValue] = {"raw_script": raw_script, "input_url": input_url}
        if uploads is not None:
            data["uploads"] = to_json_array(uploads)
        if volumes is not None:
            data["volumes"] = to_json_array(volumes)
        return self._prepare_request("POST", "/job-raw/", json=data)

    def _create_docker_job(
        self,
        docker_image: str,
        args: str = "",
        env: dict[str, str] | None = None,
        use_gpu: bool = False,
        input_url: str = "",
        executor_class: ExecutorClass = DEFAULT_EXECUTOR_CLASS,
        uploads: list[SingleFileUpload] | None = None,
        volumes: list[Volume] | None = None,
        target_validator_hotkey: str | None = None,
    ) -> HTTPResponseType:
        data: dict[str, JsonValue] = {
            "target_validator_hotkey": target_validator_hotkey,
            "executor_class": executor_class,
            "docker_image": docker_image,
            "args": args,
            "env": env or {},  # type: ignore # mypy doesn't acknowledge dict[str, str] | None as subtype of JSONDict
            "use_gpu": use_gpu,
            "input_url": input_url,
        }
        if uploads is not None:
            data["uploads"] = to_json_array(uploads)
        if volumes is not None:
            data["volumes"] = to_json_array(volumes)
        return self._prepare_request("POST", "/job-docker/", json=data)

    def _submit_job_feedback(
        self,
        job_uuid: str,
        result_correctness: float,
        expected_duration: float | None = None,
    ) -> HTTPResponseType:
        self._require_signer()
        data: JobFeedback = {
            "result_correctness": result_correctness,
        }
        if expected_duration is not None:
            data["expected_duration"] = expected_duration
        return self._prepare_request("PUT", f"/jobs/{job_uuid}/feedback/", json=typing.cast(JsonValue, data))


class FacilitatorClient(FacilitatorClientBase[httpx.Client, httpx.Response]):
    def _get_client(self) -> httpx.Client:
        return httpx.Client(base_url=self.base_url, headers={"Authorization": f"Token {self.token}"})

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def close(self):
        self.client.close()

    def handle_response(self, response: httpx.Response) -> JsonValue:
        response.raise_for_status()
        return response.json()

    def get_jobs(self, page: int = 1, page_size: int = 10) -> JsonValue:
        return self.handle_response(self._get_jobs(page, page_size))

    def get_job(self, job_uuid: str) -> JobState:
        response = self.handle_response(self._get_job(job_uuid))
        return typing.cast(JobState, response)

    def create_raw_job(
        self,
        raw_script: str,
        input_url: str = "",
        uploads: list[SingleFileUpload] | None = None,
        volumes: list[Volume] | None = None,
    ) -> JobState:
        response = self.handle_response(
            self._create_raw_job(
                raw_script,
                input_url=input_url,
                uploads=uploads,
                volumes=volumes,
            )
        )
        return typing.cast(JobState, response)

    def create_docker_job(
        self,
        docker_image: str,
        args: str = "",
        env: dict[str, str] | None = None,
        use_gpu: bool = False,
        input_url: str = "",
        executor_class: ExecutorClass = DEFAULT_EXECUTOR_CLASS,
        uploads: list[SingleFileUpload] | None = None,
        volumes: list[Volume] | None = None,
        target_validator_hotkey: str | None = None,
    ) -> JobState:
        response = self.handle_response(
            self._create_docker_job(
                executor_class=executor_class,
                docker_image=docker_image,
                args=args,
                env=env,
                use_gpu=use_gpu,
                input_url=input_url,
                uploads=uploads,
                volumes=volumes,
                target_validator_hotkey=target_validator_hotkey,
            )
        )
        return typing.cast(JobState, response)

    def wait_for_job(self, job_uuid: str, timeout: float = 600) -> JobState:
        """
        Wait for a job to complete.

        This will poll the facilitator for the job's state until it is no longer in progress.

        :param job_uuid: The UUID of the job to wait for.
        :param timeout: The maximum time in seconds to wait for the job to complete.
        :return: The final state of the job.
        """
        start_time = time.time()

        job = None
        while not job or time.time() - start_time < timeout:
            job = self.get_job(job_uuid)
            if not is_in_progress(job["status"]):
                return job
            time.sleep(3)

        raise FacilitatorClientTimeoutException(
            f"Job {job_uuid} did not complete within {timeout} seconds, last status: {job and job['status']!r}"
        )

    def submit_job_feedback(
        self,
        job_uuid: str,
        result_correctness: float,
        expected_duration: float | None = None,
    ) -> JobFeedback:
        """
        Submit feedback for a job.

        This can be used to inform the facilitator about the correctness of the job's result.

        :param job_uuid: The UUID of the job to submit feedback for.
        :param result_correctness: The correctness of the job's result expressed as a float between 0.0 and 1.0.
            - 0.0 indicates 0% correctness (completely incorrect).
            - 1.0 indicates 100% correctness (completely correct).
        :param expected_duration: An optional field indicating the expected time in seconds for the job to complete.
            This can highlight if the job's execution was slower than expected, suggesting performance issues
            on executor side.
        """
        response = self.handle_response(
            self._submit_job_feedback(
                job_uuid=job_uuid,
                result_correctness=result_correctness,
                expected_duration=expected_duration,
            )
        )
        return typing.cast(JobFeedback, response)


class AsyncFacilitatorClient(FacilitatorClientBase[httpx.AsyncClient, typing.Awaitable[httpx.Response]]):
    def _get_client(self) -> httpx.AsyncClient:
        return httpx.AsyncClient(base_url=self.base_url, headers={"Authorization": f"Token {self.token}"})

    async def handle_response(self, response: typing.Awaitable[httpx.Response]) -> JsonValue:
        awaited_response = await response
        awaited_response.raise_for_status()
        return awaited_response.json()

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()

    async def close(self):
        await self.client.aclose()

    async def get_jobs(self, page: int = 1, page_size: int = 10) -> JsonValue:
        return await self.handle_response(self._get_jobs(page=page, page_size=page_size))

    async def get_job(self, job_uuid: str) -> JobState:
        response = await self.handle_response(self._get_job(job_uuid=job_uuid))
        return typing.cast(JobState, response)

    async def create_raw_job(
        self,
        raw_script: str,
        input_url: str = "",
        uploads: list[SingleFileUpload] | None = None,
        volumes: list[Volume] | None = None,
    ) -> JobState:
        response = await self.handle_response(
            self._create_raw_job(
                raw_script=raw_script,
                input_url=input_url,
                uploads=uploads,
                volumes=volumes,
            )
        )
        return typing.cast(JobState, response)

    async def create_docker_job(
        self,
        docker_image: str,
        args: str = "",
        env: dict[str, str] | None = None,
        use_gpu: bool = False,
        input_url: str = "",
        executor_class: ExecutorClass = DEFAULT_EXECUTOR_CLASS,
        uploads: list[SingleFileUpload] | None = None,
        volumes: list[Volume] | None = None,
        target_validator_hotkey: str | None = None,
    ) -> JobState:
        response = await self.handle_response(
            self._create_docker_job(
                executor_class=executor_class,
                docker_image=docker_image,
                args=args,
                env=env,
                use_gpu=use_gpu,
                input_url=input_url,
                uploads=uploads,
                volumes=volumes,
                target_validator_hotkey=target_validator_hotkey,
            )
        )
        return typing.cast(JobState, response)

    async def wait_for_job(self, job_uuid: str, timeout: float = 600) -> JobState:
        """
        Wait for a job to complete.

        This will poll the facilitator for the job's state until it is no longer in progress.

        :param job_uuid: The UUID of the job to wait for.
        :param timeout: The maximum time in seconds to wait for the job to complete.
        :return: The final state of the job.
        """
        start_time = time.time()
        job = None
        while time.time() - start_time < timeout:
            job = await self.get_job(job_uuid)
            if not is_in_progress(job["status"]):
                return job
            await asyncio.sleep(3)
        raise FacilitatorClientTimeoutException(
            f"Job {job_uuid} did not complete within {timeout} seconds, last status: {job and job['status']!r}"
        )

    async def submit_job_feedback(
        self,
        job_uuid: str,
        result_correctness: float,
        expected_duration: float | None = None,
    ) -> JobFeedback:
        """
        Submit feedback for a job.

        This can be used to inform the facilitator about the correctness of the job's result.

        :param job_uuid: The UUID of the job to submit feedback for.
        :param result_correctness: The correctness of the job's result expressed as a float between 0.0 and 1.0.
            - 0.0 indicates 0% correctness (completely incorrect).
            - 1.0 indicates 100% correctness (completely correct).
        :param expected_duration: An optional field indicating the expected time in seconds for the job to complete.
            This can highlight if the job's execution was slower than expected, suggesting performance issues
            on executor side.
        """
        response = await self.handle_response(
            self._submit_job_feedback(
                job_uuid=job_uuid,
                result_correctness=result_correctness,
                expected_duration=expected_duration,
            )
        )
        return typing.cast(JobFeedback, response)
