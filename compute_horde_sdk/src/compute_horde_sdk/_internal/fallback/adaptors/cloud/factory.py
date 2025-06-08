import logging
from typing import Any

from .base import CloudAdapter
from .runpod import RunPodAdapter

logger = logging.getLogger(__name__)


class CloudAdapterFactory:
    """Factory for creating cloud provider adapters."""

    @staticmethod
    def create_adapter(cloud: str, **kwargs: Any) -> CloudAdapter | None:
        """
        Create a cloud provider adapter.

        Args:
            cloud: The name of the cloud provider
            **kwargs: Additional arguments to pass to the adapter constructor

        Returns:
            A cloud provider adapter instance or None if the cloud is not supported

        """
        if cloud == "runpod":
            return RunPodAdapter(**kwargs)
        logger.warning("Unsupported cloud provider: %s", cloud)
        return None
