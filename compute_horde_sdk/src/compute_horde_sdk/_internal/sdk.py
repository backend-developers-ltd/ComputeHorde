import asyncio
import base64
import json
import logging
import time
from collections.abc import AsyncIterator, Mapping, Sequence
from datetime import timedelta
from typing import Self
from urllib.parse import urljoin

import bittensor
import httpx
import pydantic

from _compute_horde_models.executor_class import ExecutorClass
from _compute_horde_models.signature import BittensorWalletSigner, SignatureScope, SignedFields, signature_to_headers

from .exceptions import ComputeHordeError, ComputeHordeJobTimeoutError, ComputeHordeNotFoundError
from .models import (
    ComputeHordeJobResult,
    ComputeHordeJobStatus,
    FacilitatorJobResponse,
    FacilitatorJobsResponse,
    InputVolume,
    OutputVolume,
)

logger = logging.getLogger(__name__)

JOB_REFRESH_INTERVAL = timedelta(seconds=3)

DEFAULT_FACILITATOR_URL = "https://facilitator.computehorde.io/"


class ComputeHordeJob:
    """
    The class representing a job running on the Compute Horde.
    """

    def __init__(
        self,
        client: "ComputeHordeClient",
        uuid: str,
        status: ComputeHordeJobStatus,
        result: ComputeHordeJobResult | None = None,
    ):
        self._client = client
        self.uuid = uuid
        self.status = status
        self.result = result

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

    async def refresh_from_facilitator(self) -> None:
        new_job = await self._client.get_job(self.uuid)
        self.status = new_job.status
        self.result = new_job.result

    def __repr__(self):
        return f"<{self.__class__.__qualname__}: {self.uuid!r}>"

    @classmethod
    def _from_response(cls, client: "ComputeHordeClient", response: FacilitatorJobResponse) -> Self:
        result = None
        if not response.status.is_in_progress():
            # TODO: Handle base64 decode errors
            result = ComputeHordeJobResult(
                stdout=response.stdout,
                artifacts={path: base64.b64decode(base64_data) for path, base64_data in response.artifacts.items()},
            )
        return cls(
            client,
            uuid=response.uuid,
            status=response.status,
            result=result,
        )


class ComputeHordeClient:
    """
    The class used to communicate with the Compute Horde.
    """

    def __init__(
        self,
        hotkey: bittensor.Keypair,
        compute_horde_validator_hotkey: str,
        job_queue: str | None = None,
        facilitator_url: str = DEFAULT_FACILITATOR_URL,
    ):
        """
        Create a new Compute Horde client.

        :param hotkey: Your wallet hotkey, used for signing requests.
        :param compute_horde_validator_hotkey: The hotkey for the validator that your jobs should go to.
        :param job_queue: A user-defined string value that will allow for retrieving all pending jobs,
            for example after process restart, relevant to this process.
        :param facilitator_url: The URL to use for the Compute Horde Facilitator API.
        """
        self.hotkey = hotkey
        self.compute_horde_validator_hotkey = compute_horde_validator_hotkey
        self.job_queue = job_queue
        self.facilitator_url = facilitator_url
        self._client = httpx.AsyncClient(base_url=self.facilitator_url, follow_redirects=True)
        self._signer = BittensorWalletSigner(hotkey)

    async def _make_request(
        self,
        method: str,
        url: str,
        *,
        json: dict | None = None,
        params: Mapping[str, str | int] | None = None,
        headers: Mapping[str, str] | None = None,
    ) -> str:
        """
        :return: Response content as string.
        """
        auth_headers = self._get_authentication_headers(method, urljoin(self.facilitator_url, url))
        request = self._client.build_request(
            method=method,
            url=url,
            json=json,
            params=params,
            headers={**headers, **auth_headers} if headers else auth_headers,
        )
        logger.debug("%s %s", method, request.url)
        response = await self._client.send(request)
        try:
            response.raise_for_status()
        except httpx.HTTPStatusError as e:
            logger.error("Response status=%d: %s", e.response.status_code, e.response.text)
            if e.response.status_code == 404:
                raise ComputeHordeNotFoundError(f"Resource under {request.url} not found") from e
            raise ComputeHordeError(f"Compute Horde responded with status code {e.response.status_code}") from e

        return response.text

    def _get_signature_headers(self, data: dict) -> dict[str, str]:
        signed_fields = SignedFields.from_facilitator_sdk_json(data)
        signature = self._signer.sign(payload=signed_fields.model_dump_json())
        return signature_to_headers(signature, SignatureScope.SignedFields)

    def _get_cheated_job_headers(self, data: dict) -> dict[str, str]:
        payload = json.dumps(data, sort_keys=True)
        signature = self._signer.sign(payload=payload)
        return signature_to_headers(signature, SignatureScope.FullRequest)

    def _get_authentication_headers(
        self,
        method: str,
        url: str,
    ):
        headers = {
            "Realm": "mainnet",
            "SubnetID": "12",
            "Nonce": str(time.time()),
            "Hotkey": self.hotkey.ss58_address,
        }

        headers_str = json.dumps(headers, sort_keys=True)

        data_to_sign = f"{method}{url}{headers_str}".encode()

        signature = self.hotkey.sign(
            data_to_sign,
        ).hex()
        headers["Signature"] = signature

        return headers

    async def report_cheated_job(self, job_uuid: str) -> str:
        """
        Reports to validator that a miner has cheated on a job.

        :param job_uuid: The UUID of the job that was cheated on.
        :param miner_hotkey: The hotkey of the miner that cheated.
        """
        data = {"job_uuid": job_uuid}
        signature_headers = self._get_cheated_job_headers(data)
        response = await self._make_request("POST", "/api/v1/cheated-job/", json=data, headers=signature_headers)
        logger.debug("Reported job %s", job_uuid)
        return response

    async def create_job(
        self,
        executor_class: ExecutorClass,
        job_namespace: str,
        docker_image: str,
        args: Sequence[str] | None = None,
        env: Mapping[str, str] | None = None,
        artifacts_dir: str | None = None,
        input_volumes: Mapping[str, InputVolume] | None = None,
        output_volumes: Mapping[str, OutputVolume] | None = None,
        run_cross_validation: bool = False,
        trusted_output_volumes: Mapping[str, OutputVolume] | None = None,
    ) -> ComputeHordeJob:
        """
        Create a new job to run in the Compute Horde.

        :param executor_class: Class of the executor machine to run the job on.
        :param job_namespace: Specifies where the job comes from.
            The recommended format is the subnet number and version, like e.g. ``"SN123.0"``.
        :param docker_image: Docker image of the job, in the form of ``user/image:tag``.
        :param args: Positional arguments and flags to run the job with.
        :param env: Environment variables to run the job with.
        :param artifacts_dir: Path of the directory that the job will write its results to.
            Contents of files found in this directory will be returned after the job completes
            as a part of the job result. It should be an absolute path (starting with ``/``).
        :param input_volumes: The data to be made available to the job in Docker volumes.
            The keys should be absolute file/directory paths under which you want your data to be available.
            The values should be ``InputVolume`` instances representing how to obtain the input data.
            For now, input volume paths must start with ``/volume/``.
        :param output_volumes: The data to be read from the Docker volumes after job completion
            and uploaded to the described destinations. Use this for outputs that are too big
            or too unstable to be treated as ``artifacts``.
            The keys should be absolute file paths under which job output data will be available.
            The values should be ``OutputVolume`` instances representing how to handle the output data.
            For now, output volume paths must start with ``/output/``.
        :param run_cross_validation: Whether to run cross validation on a trusted miner.
        :param trusted_output_volumes: Output volumes for cross validation on a trusted miner.
            If these are omitted then cross validating on a trusted miner will not result in any uploads.
        :return: A ``ComputeHordeJob`` class instance representing the created job.
        """

        if run_cross_validation:
            # Or should we remove this argument for now?
            raise NotImplementedError(
                "Cross validation is not supported yet, please call create_job with run_cross_validation=False"
            )

        # TODO: make this a pydantic model?
        data: dict[str, pydantic.JsonValue] = {
            "target_validator_hotkey": self.compute_horde_validator_hotkey,
            "executor_class": executor_class,
            "docker_image": docker_image,
            "args": args or [],  # type: ignore
            "env": env or {},  # type: ignore
            "use_gpu": True,
            "artifacts_dir": artifacts_dir,
        }
        if input_volumes is not None:
            data["volumes"] = [
                input_volume.to_compute_horde_volume(mount_path).model_dump()
                for mount_path, input_volume in input_volumes.items()
            ]

        if output_volumes is not None:
            data["uploads"] = [
                output_volume.to_compute_horde_output_upload(mount_path).model_dump()
                for mount_path, output_volume in output_volumes.items()
            ]

        logger.debug("Creating job from image %s", docker_image)
        signature_headers = self._get_signature_headers(data)

        response = await self._make_request("POST", "/api/v1/job-docker/", json=data, headers=signature_headers)

        try:
            job_response = FacilitatorJobResponse.model_validate_json(response)
        except pydantic.ValidationError as e:
            raise ComputeHordeError("Compute Horde returned malformed response") from e

        job = ComputeHordeJob._from_response(self, job_response)
        logger.debug("Created job with UUID=%s", job.uuid)

        return job

    async def get_job(self, job_uuid: str) -> ComputeHordeJob:
        """
        Retrieve information about a job from the Compute Horde.

        :param job_uuid: The UUID of the job to retrieve.
        :return: A ``ComputeHordeJob`` instance representing this job.
        :raises ComputeHordeNotFoundError: If the job with this UUID does not exist.
        """
        logger.debug("Fetching job with UUID=%s", job_uuid)

        response = await self._make_request("GET", f"/api/v1/jobs/{job_uuid}/")

        try:
            job_response = FacilitatorJobResponse.model_validate_json(response)
        except pydantic.ValidationError as e:
            raise ComputeHordeError("Compute Horde returned malformed response") from e

        return ComputeHordeJob._from_response(self, job_response)

    async def _get_jobs_page(self, page: int = 1, page_size: int = 10) -> FacilitatorJobsResponse:
        params = {"page": page, "page_size": page_size}

        response = await self._make_request("GET", "/api/v1/jobs/", params=params)

        try:
            jobs_response = FacilitatorJobsResponse.model_validate_json(response)
        except pydantic.ValidationError as e:
            raise ComputeHordeError("Compute Horde returned malformed response") from e

        return jobs_response

    async def get_jobs(self, page: int = 1, page_size: int = 10) -> list[ComputeHordeJob]:
        """
        Retrieve information about your jobs from the Compute Horde.

        :param page: The page number.
        :param page_size: The page size.
        :return: A list of ``ComputeHordeJob`` instances representing your jobs.
        :raises ComputeHordeNotFoundError: If the requested page does not exist.
        """
        logger.debug("Fetching jobs page=%d, page_size%d", page, page_size)
        jobs_response = await self._get_jobs_page(page=page, page_size=page_size)

        return [ComputeHordeJob._from_response(self, job) for job in jobs_response.results]

    async def iter_jobs(self) -> AsyncIterator[ComputeHordeJob]:
        """
        Retrieve information about your jobs from the Compute Horde.

        :return: An async iterator of ``ComputeHordeJob`` instances representing your jobs.

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
