from __future__ import annotations

import json
import logging
import os
from typing import Any

import httpx
import tenacity

from ._models import Volume

logger = logging.getLogger(__name__)


def get_volume_manager_headers() -> dict[str, str]:
    """Extract volume manager headers from environment variables."""
    headers = {}
    prefix = "COMPUTE_HORDE_VOLUME_MANAGER_HEADER_"
    for key, value in os.environ.items():
        if key.startswith(prefix):
            header_name = key[len(prefix) :]
            headers[header_name] = value
    return headers


class VolumeManagerError(Exception):
    """Exception raised when volume manager operations fail."""

    def __init__(self, description: str, error_detail: str | None = None) -> None:
        self.description = description
        self.error_detail = error_detail

    def __str__(self) -> str:
        if self.error_detail is not None:
            return f"{self.description}: {self.error_detail}"
        return self.description


class VolumeManagerClient:
    """Client for communicating with the Volume Manager service."""

    def __init__(self, base_url: str, headers: dict[str, str] | None = None):
        self.base_url = base_url.rstrip("/")
        self.headers = headers or {}
        self._client: httpx.AsyncClient | None = None

    def _get_client(self) -> httpx.AsyncClient:
        """Get or create the HTTP client."""
        if self._client is None:
            self._client = httpx.AsyncClient()
        return self._client

    async def close(self) -> None:
        """Close the HTTP client."""
        if self._client is not None:
            await self._client.aclose()
            self._client = None

    async def prepare_volume(
        self, job_uuid: str, volume: Volume, job_metadata: dict[str, Any], timeout: float = 60.0
    ) -> list[list[str]]:
        """
        Request the volume manager to prepare a volume for the job.

        Args:
            job_uuid: Unique identifier for the job
            volume: Volume specification to prepare
            job_metadata: Additional metadata about the job
            timeout: Request timeout in seconds

        Returns:
            List of mount flag lists for Docker

        Raises:
            VolumeManagerError: If the request fails

        """
        url = f"{self.base_url}/prepare_volume"
        volume_data = volume.model_dump()
        payload = {"job_uuid": job_uuid, "volume": volume_data, "job_metadata": job_metadata}

        response_data = await self._make_request(url, payload, "prepare_volume", timeout=timeout)

        # Convert string mount types to MountType objects
        mounts = []
        for mount_data in response_data["mounts"]:
            mounts.append(mount_data)

        return mounts

    async def job_finished(self, job_uuid: str) -> None:
        """
        Notify the volume manager that a job has finished.

        Args:
            job_uuid: Unique identifier for the finished job

        Raises:
            VolumeManagerError: If the notification fails

        """
        url = f"{self.base_url}/job_finished"
        payload = {"job_uuid": job_uuid}

        await self._make_request(url, payload, "job_finished", timeout=30.0)

    @tenacity.retry(
        stop=tenacity.stop_after_attempt(3),
        wait=tenacity.wait_fixed(1),
        retry=tenacity.retry_if_exception_type((httpx.RequestError, httpx.HTTPStatusError)),
    )
    async def _make_request(
        self, url: str, payload: dict[str, Any], operation: str, timeout: float = 60.0
    ) -> dict[str, list[list[str]]]:
        """
        Make a POST request to the Volume Manager with standardized error handling.

        Args:
            url: The endpoint URL
            payload: The JSON payload to send
            operation: Operation name for error messages
            timeout: Request timeout in seconds

        Returns:
            Mount options for the job

        Raises:
            VolumeManagerError: If the request fails or response is invalid

        """
        logger.debug(f"Making {operation} request to Volume Manager at {url}")

        try:
            client = self._get_client()
            response = await client.post(url, json=payload, headers=self.headers, timeout=timeout)
            response.raise_for_status()

            logger.debug(f"Volume Manager {operation} response status: {response.status_code}")
            logger.debug(f"Volume Manager {operation} response: {response.text}")

            return response.json()  # type: ignore

        except httpx.HTTPStatusError as e:
            # Handle non-retryable status codes
            error_msg = f"Volume Manager {operation} returned status {e.response.status_code}"
            error_detail = None

            try:
                error_data = e.response.json()
                if "error" in error_data:
                    error_detail = error_data["error"]
            except (json.JSONDecodeError, KeyError):
                error_detail = e.response.text

            raise VolumeManagerError(error_msg, error_detail=error_detail)
        except httpx.RequestError as e:
            raise VolumeManagerError(f"Network error during Volume Manager {operation}: {e}", error_detail=str(e))
        except json.JSONDecodeError as e:
            raise VolumeManagerError(f"Invalid JSON response from Volume Manager {operation}: {e}", error_detail=str(e))


def create_volume_manager_client(base_url: str, headers: dict[str, str] | None = None) -> VolumeManagerClient:
    """
    Create a Volume Manager client with the specified configuration.

    Args:
        base_url: The base URL of the Volume Manager service
        headers: Optional headers to include in requests

    Returns:
        A configured VolumeManagerClient instance

    """
    logger.debug(f"Volume manager created at {base_url}")
    return VolumeManagerClient(base_url=base_url, headers=headers)
