from __future__ import annotations

import json
import logging
import os
from dataclasses import dataclass
from typing import Any

import httpx
import tenacity
from ._models import Volume

logger = logging.getLogger(__name__)


def get_volume_manager_headers():
    """Extract volume manager headers from environment variables."""
    headers = {}
    prefix = "COMPUTE_HORDE_VOLUME_MANAGER_HEADER_"
    for key, value in os.environ.items():
        if key.startswith(prefix):
            header_name = key[len(prefix) :]
            headers[header_name] = value
    return headers


@dataclass
class VolumeManagerMount:
    """Represents a Docker mount option returned by the volume manager."""

    type: str
    source: str
    target: str


@dataclass
class VolumeManagerResponse:
    """Response from volume manager prepare_volume endpoint."""

    mounts: list[VolumeManagerMount]


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

    async def prepare_volume(
        self, job_uuid: str, volume: Volume, job_metadata: dict[str, Any]
    ) -> VolumeManagerResponse:
        """
        Request the volume manager to prepare a volume for the job.

        Args:
            job_uuid: Unique identifier for the job
            volume: Volume specification to prepare
            job_metadata: Additional metadata about the job

        Returns:
            VolumeManagerResponse with mount options

        Raises:
            VolumeManagerError: If the request fails
        """
        url = f"{self.base_url}/prepare_volume"
        volume_data = volume.model_dump()
        payload = {"job_uuid": job_uuid, "volume": volume_data, "job_metadata": job_metadata}

        response_data = await self._make_request(url, payload, "prepare_volume")
        mounts = [VolumeManagerMount(**mount) for mount in response_data["mounts"]]
        return VolumeManagerResponse(mounts=mounts)

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
        self, 
        url: str, 
        payload: dict[str, Any], 
        operation: str, 
        timeout: float = 300.0
    ) -> dict[str, Any]:
        """
        Make a POST request to the Volume Manager with standardized error handling.
        
        Args:
            url: The endpoint URL
            payload: The JSON payload to send
            operation: Operation name for error messages
            timeout: Request timeout in seconds
            
        Returns:
            Parsed JSON response
            
        Raises:
            VolumeManagerError: If the request fails or response is invalid
        """
        logger.debug(f"Making {operation} request to Volume Manager at {url}")
        logger.debug(f"Request payload: {json.dumps(payload, indent=2)}")

        try:
            async with httpx.AsyncClient(timeout=timeout) as client:
                response = await client.post(url, json=payload, headers=self.headers)
                response.raise_for_status()
            
            logger.debug(f"Volume Manager {operation} response status: {response.status_code}")
            logger.debug(f"Volume Manager {operation} response: {response.text}")

            return response.json()

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