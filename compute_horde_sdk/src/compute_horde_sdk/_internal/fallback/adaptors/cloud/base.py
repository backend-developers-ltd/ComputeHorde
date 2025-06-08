from abc import ABC, abstractmethod

import sky.backends


class CloudAdapter(ABC):
    """
    Base class for cloud provider adapters.
    """

    @abstractmethod
    def set_job_resource_handle(self, handle: sky.backends.CloudVmRayResourceHandle) -> None:
        """
        Set the job resource handle for this adapter.

        Args:
            handle: The job resource handle containing cloud-specific information

        """
        pass

    @abstractmethod
    def get_ssh_port(self, internal_port: str) -> int | None:
        """
        Get the SSH port mapping for a given internal port.

        Args:
            internal_port: The internal port to get the mapping for

        Returns:
            The mapped SSH port if found, None otherwise

        """
        pass
