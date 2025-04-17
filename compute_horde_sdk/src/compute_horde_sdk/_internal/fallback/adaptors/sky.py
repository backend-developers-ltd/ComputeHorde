import uuid
from functools import cache, cached_property
from typing import Any

import sky
import sky.sky_logging

from ..exceptions import FallbackError
from ..job import FallbackJobSpec, FallbackJobStatus


class SkyCloud:
    """
    Represents a cloud environment configuration and setup handler.

    This class provides functionality to configure and initialize cloud-based
    setups dynamically based on the provided cloud name. It ensures proper setup
    of the cloud environment and raises an error if the specified cloud is not
    supported.

    :ivar cloud: The name of the cloud environment to set up.
    :type cloud: str
    """

    def __init__(self, cloud: str, **kwargs: Any) -> None:
        self.cloud = cloud

        setup = getattr(self, f"_{self.cloud}")
        if setup is None:
            raise FallbackError(f"Unsupported cloud: {cloud}")

        setup(**kwargs)

    @classmethod
    def _runpod(cls, api_key: str | None = None) -> None:
        if api_key is not None:
            import runpod

            runpod.api_key = api_key

    def __str__(self) -> str:
        return self.cloud


class SkyJob:
    """
    Represents a SkyPilot Job that manages the submission and management of jobs on a specific clouds.

    This class encapsulates functionality for defining and managing jobs, including preparing job resources,
    submitting jobs, tracking their status, and handling unique identifiers for job instances.
    It works with pre-defined supported clouds and ensures proper setup for jobs utilizing the `sky` library.

    :ivar cloud: The cloud environment configuration.
    :type cloud: SkyCloud
    :ivar job_spec: The specifications of the job to be run.
    :type job_spec: FallbackJobSpec
    """

    UUID_NAMESPACE = uuid.NAMESPACE_OID

    def __init__(self, cloud: SkyCloud, job_spec: FallbackJobSpec) -> None:
        self.cloud = cloud
        self.job_spec = job_spec

        self._job_id: int | None = None

    @property
    def submitted(self) -> bool:
        return self.job_id is not None

    @property
    def job_id(self) -> int | None:
        return self._job_id

    @cached_property
    def job_uuid(self) -> str:
        if not self.submitted:
            return ""

        return str(uuid.uuid5(self.UUID_NAMESPACE, f"{self.cluster_name}-{self.job_id}"))

    @cached_property
    def cluster_name(self) -> str:
        resources_token = str(uuid.uuid5(self.UUID_NAMESPACE, str(self._get_resources())))[:8]
        return f"ch-{resources_token}"

    def submit(self, idle_minutes: int = 1) -> FallbackJobStatus:
        if self.submitted:
            raise FallbackError("Job already submitted")

        resources = self._get_resources()
        task = self._get_task(resources)

        with sky.sky_logging.silent():
            self._job_id, _ = sky.launch(
                task,
                cluster_name=self.cluster_name,
                idle_minutes_to_autostop=idle_minutes,
                down=True,
                detach_setup=True,
                detach_run=True,
                stream_logs=False,
            )

        return FallbackJobStatus.SENT

    def status(self) -> FallbackJobStatus:
        if not self.submitted:
            raise FallbackError("Job not yet submitted")

        with sky.sky_logging.silent():
            sky_status = sky.job_status(self.cluster_name, job_ids=[self.job_id]).get(self.job_id)

        if sky_status is None:
            return FallbackJobStatus.SENT
        elif sky_status in {sky.JobStatus.INIT, sky.JobStatus.PENDING, sky.JobStatus.SETTING_UP, sky.JobStatus.RUNNING}:
            return FallbackJobStatus.ACCEPTED
        elif sky_status in {sky.JobStatus.FAILED_DRIVER, sky.JobStatus.FAILED_SETUP}:
            return FallbackJobStatus.REJECTED
        elif sky_status == sky.JobStatus.SUCCEEDED:
            return FallbackJobStatus.COMPLETED
        elif sky_status == sky.JobStatus.FAILED:
            return FallbackJobStatus.FAILED
        else:
            raise NotImplementedError()

    @cache
    def _get_resources(self) -> sky.Resources:
        return sky.Resources(
            cloud=sky.clouds.CLOUD_REGISTRY.from_str(str(self.cloud)),
            accelerators=self.job_spec.accelerators,
            cpus=self.job_spec.cpus,
            memory=self.job_spec.memory,
            disk_size=self.job_spec.disk_size,
            ports=self.job_spec.ports,
            instance_type=self.job_spec.instance_type,
            image_id=self.job_spec.image_id,
            region=self.job_spec.region,
            zone=self.job_spec.zone,
        )

    @cache
    def _get_task(self, resources: sky.Resources | None = None) -> sky.Task:
        task = sky.Task(
            setup='echo "Running setup."',
            run=self.job_spec.run,
            envs=self.job_spec.envs,
        )
        if resources is not None:
            task = task.set_resources(resources)

        return task
