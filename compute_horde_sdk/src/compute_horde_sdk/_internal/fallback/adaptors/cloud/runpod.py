import logging
import os
import tomllib
from pathlib import Path
from typing import Any, cast

import requests
import sky.backends

from .base import CloudAdapter

logger = logging.getLogger(__name__)


class RunPodError(Exception):
    """Base class for RunPod API errors."""

    pass


class RunPodClient:
    """Client for interacting with the RunPod API."""

    BASE_URL = "https://rest.runpod.io/v1"

    def __init__(self, api_key: str | None = None):
        """
        Initialize the RunPod client.

        Args:
            api_key: Optional API key. If not provided, will try to get from:
                    1. RUNPOD_API_KEY environment variable
                    2. ~/.runpod/config.toml file

        """
        self.api_key = api_key or self._get_api_key()
        if not self.api_key:
            raise RunPodError("No RunPod API key found")

        self.session = requests.Session()
        self.session.headers.update({"Authorization": f"Bearer {self.api_key}", "Content-Type": "application/json"})

    def _get_api_key(self) -> str | None:
        """Get the RunPod API key from environment or config file."""
        # Try environment variable first
        api_key = os.environ.get("RUNPOD_API_KEY")
        if api_key:
            return str(api_key)

        # Try config file
        config_path = Path.home() / ".runpod" / "config.toml"
        if config_path.exists():
            try:
                with open(config_path, "rb") as f:
                    config = tomllib.load(f)
                    api_key = config.get("default", {}).get("api_key")
                    if api_key:
                        return str(api_key)
            except Exception as e:
                logger.warning(f"Failed to read RunPod config file: {e}")

        return None

    def get_pods(self) -> list[dict[str, Any]]:
        """Get all pods."""
        url = f"{self.BASE_URL}/pods"
        response = self.session.get(url)
        response.raise_for_status()
        return cast(list[dict[str, Any]], response.json())

    def get_pod(self, pod_id: str, include_machine: bool = True) -> dict[str, Any]:
        """
        Get information about a specific pod.

        Args:
            pod_id: The ID of the pod to get information about
            include_machine: Whether to include machine information in the response

        Returns:
            Dict containing pod information

        Raises:
            RunPodError: If the API request fails

        """
        url = f"{self.BASE_URL}/pods/{pod_id}"
        params = {"includeMachine": include_machine}

        try:
            response = self.session.get(url, params=params)
            response.raise_for_status()
            return cast(dict[str, Any], response.json())
        except requests.exceptions.RequestException as e:
            raise RunPodError(f"Failed to get pod information: {e}")

    def get_pod_port_mapping(self, pod_id: str) -> dict[str, int]:
        """
        Get the port mappings for a pod.

        Args:
            pod_id: The ID of the pod to get port mappings for

        Returns:
            Dict mapping internal ports to public ports

        Raises:
            RunPodError: If the API request fails

        """
        pod_info = self.get_pod(pod_id)
        port_mappings = pod_info.get("portMappings", {})
        return cast(dict[str, int], port_mappings)


class RunPodAdapter(CloudAdapter):
    """Adapter for RunPod-specific operations."""

    def __init__(self, api_key: str | None = None) -> None:
        self._client: RunPodClient | None = None
        self._pod_id: str | None = None
        self._client = RunPodClient(api_key)

    def set_job_resource_handle(self, handle: sky.backends.CloudVmRayResourceHandle) -> None:
        """
        Set the job resource handle.

        Args:
            handle: The job resource handle containing cloud-specific information

        """
        if not handle:
            logger.warning("Invalid job resource handle")
            return

        if self._client is None:
            logger.warning("RunPod client not initialized")
            return

        try:
            pods = self._client.get_pods()
            # CloudVmRayResourceHandle does not provide any information about the pod beside its public IP,
            # so we need to get it from the API. To do it, we're going to fetch all pods
            # and find the one with the matching public IP.
            for pod in pods:
                if pod.get("publicIp") == handle.head_ip:
                    self._pod_id = pod["id"]
                    logger.debug("Found RunPod pod ID: %s", self._pod_id)
                    return
            logger.warning("No RunPod pod found with public IP: %s", handle.head_ip)
        except RunPodError as e:
            logger.warning("Failed to find RunPod pod by IP: %s", e)

    def get_ssh_port(self, internal_port: str) -> int | None:
        """Get the SSH port mapping for a given internal port."""
        if self._client is None or self._pod_id is None:
            logger.debug("RunPod client or pod ID not available")
            return None

        try:
            port_mappings = self._client.get_pod_port_mapping(self._pod_id)
            logger.debug("Retrieved RunPod port mappings: %s", port_mappings)
            if port_mappings:
                ssh_port = port_mappings.get(internal_port)
                if ssh_port:
                    logger.info("Found SSH port mapping: %s -> %d", internal_port, ssh_port)
                    return ssh_port
                logger.warning("No SSH port mapping found for internal port %s", internal_port)
            else:
                logger.warning("No port mappings available from RunPod API")
        except RunPodError as e:
            logger.warning("Failed to get RunPod port mappings: %s", e)
        return None
