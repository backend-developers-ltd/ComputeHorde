import base64
import logging
import os
import shutil
import tempfile
import uuid
from functools import cache, cached_property
from pathlib import Path
from typing import Any

import sky
import sky.backends
import sky.exceptions
import sky.sky_logging

from ..exceptions import FallbackError
from ..job import FallbackJobSpec, FallbackJobStatus

logger = logging.getLogger(__name__)


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
            logger.error("Unsupported cloud: %s", cloud)
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
    PREFIX = "ch-"

    def __init__(self, cloud: SkyCloud, job_spec: FallbackJobSpec) -> None:
        self.cloud = cloud
        self.job_spec = job_spec

        self.temp_dir = self._get_tempdir()

        self._job_id: int | None = None
        self._job_resource_handle: sky.backends.CloudVmRayResourceHandle | None = None

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
        return f"{self.PREFIX}{resources_token}"

    def submit(self, idle_minutes: int = 1) -> FallbackJobStatus:
        if self.submitted:
            logger.error("Attempted to submit a job that is already submitted.")
            raise FallbackError("Job already submitted")

        logger.info("Submitting job to cloud: %s", self.cloud)

        resources = self._get_resources()
        logger.debug("Resources for job submission: %s", resources)
        task = self._get_task(resources)
        logger.debug("Task for job submission: %s", task)

        with sky.sky_logging.silent():
            logger.debug("Launching job on cluster: %s", self.cluster_name)
            self._job_id, self._job_resource_handle = sky.launch(
                task,
                cluster_name=self.cluster_name,
                backend=sky.backends.CloudVmRayBackend(),
                idle_minutes_to_autostop=idle_minutes,
                down=True,
                detach_setup=True,
                detach_run=True,
                stream_logs=False,
            )

        logger.info("Job submitted successfully with UUID: %s", self.job_uuid)

        return FallbackJobStatus.SENT

    def status(self) -> FallbackJobStatus:
        if not self.submitted:
            logger.error("Attempted to get a status from a job that is not yet submitted.")
            raise FallbackError("Job not yet submitted")

        with sky.sky_logging.silent():
            sky_status = sky.job_status(self.cluster_name, job_ids=[self.job_id]).get(self.job_id)

        logger.debug("Job %s status is: %s", self.job_uuid, sky_status)

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

    def output(self) -> str:
        if not self.submitted:
            logger.error("Attempted to get a status from a job that is not yet submitted.")
            raise FallbackError("Job not yet submitted")

        with sky.sky_logging.silent():
            logs_dir = sky.download_logs(self.cluster_name, [str(self.job_id)], local_dir=str(self.temp_dir))[
                str(self.job_id)
            ]

        logger.debug("Job %s output downloaded to: %s", self.job_uuid, logs_dir)

        return (Path(logs_dir) / "tasks/run.log").read_text()

    def artifacts(self, max_size: int = 1_000_000) -> dict[str, bytes]:
        if not self.submitted:
            logger.error("Attempted to get a status from a job that is not yet submitted.")
            raise FallbackError("Job not yet submitted")

        if self.job_spec.artifacts_dir is None:
            return {}

        assert self._job_resource_handle is not None  # make mypy happy
        head_runner = self._job_resource_handle.get_command_runners()[0]

        source = Path(self.job_spec.artifacts_dir)
        destination = Path(tempfile.mkdtemp(prefix="download-", dir=self.temp_dir))
        try:
            head_runner.rsync(f"{source}{os.sep}", str(destination), up=False, stream_logs=False)
        except sky.exceptions.CommandError as e:
            if e.returncode == sky.exceptions.RSYNC_FILE_NOT_FOUND_CODE:
                return {}
            else:
                raise

        logger.debug("Job %s artifacts downloaded to: %s", self.job_uuid, destination)

        artifacts = {}
        for item in destination.iterdir():
            if item.is_dir():
                logger.warning("Directory found in artifacts: %s", item)
            else:
                content = item.read_bytes()
                size = len(content)
                if size < max_size:
                    path = str(Path(self.job_spec.artifacts_dir) / item.name)
                    artifacts[path] = base64.b64encode(content)
                else:
                    logger.error(f"Artifact {item} too large: {size:,} bytes")

        return artifacts

    def _get_tempdir(self) -> Path:
        tempdir = Path(tempfile.mkdtemp(prefix=self.cluster_name))
        for item in tempdir.iterdir():
            if item.is_dir():
                shutil.rmtree(item)
            else:
                item.unlink()

        return tempdir

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
