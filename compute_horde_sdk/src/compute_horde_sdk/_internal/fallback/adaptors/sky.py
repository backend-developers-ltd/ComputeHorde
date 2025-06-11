import base64
import logging
import os
import tempfile
import uuid
from functools import cache, cached_property
from pathlib import Path
from typing import Any

import sky
import sky.backends
import sky.exceptions
import sky.sky_logging
import sky.skylet.constants

logger = logging.getLogger(__name__)


class SkyError(Exception):
    """The base class for all errors thrown by the SkyPilot adaptor."""


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
        try:
            sky.clouds.CLOUD_REGISTRY.from_str(cloud)
        except ValueError:
            logger.error("Unsupported cloud: %s", cloud)
            raise SkyError(f"Unsupported cloud: {cloud}")

        self.cloud = cloud

        setup = getattr(self, f"_{self.cloud}")
        if setup is not None:
            logger.info("Running cloud setup for: %s", cloud)
            setup(**kwargs)

    def __str__(self) -> str:
        return self.cloud

    @classmethod
    def _runpod(cls, api_key: str | None = None) -> None:
        if api_key is not None:
            import runpod

            runpod.api_key = api_key


class SkyJob:
    """
    Represents a SkyPilot Job that manages the submission and management of jobs on a specific clouds.

    This class encapsulates functionality for defining and managing jobs, including preparing job resources,
    submitting jobs, tracking their status, and handling unique identifiers for job instances.
    It works with pre-defined supported clouds and ensures proper setup for jobs utilizing the `sky` library.

    :ivar cloud: The cloud environment configuration.
    :type cloud: SkyCloud
    :ivar workdir: The working directory.
    :type workdir: str
    :ivar setup: The command to setup.
    :type setup: str or None
    :ivar run: The command to run.
    :type run: str or None
    :ivar envs: Environment variables for the job.
    :type envs: dict[str, str] or None
    :ivar artifacts_dir: Directory to store job artifacts.
    :type artifacts_dir: str or None
    :ivar accelerators: GPU accelerators required.
    :type accelerators: str or dict[str, int] or None
    :ivar cpus: Number of CPUs required.
    :type cpus: int or float or str or None
    :ivar memory: Amount of memory required.
    :type memory: int or float or str or None
    :ivar disk_size: Amount of disk space required.
    :type disk_size: int or None
    :ivar ports: Ports to expose.
    :type ports: int or str or list[str] or None
    :ivar instance_type: Type of instance to use.
    :type instance_type: str or None
    :ivar image_id: Docker image ID.
    :type image_id: str or None
    :ivar region: Cloud region.
    :type region: str or None
    :ivar zone: Cloud zone.
    :type zone: str or None
    """

    UUID_NAMESPACE = uuid.NAMESPACE_OID
    PREFIX = "ch-"

    def __init__(
        self,
        cloud: SkyCloud,
        workdir: str,
        setup: str | None = None,
        run: str | None = None,
        envs: dict[str, str] | None = None,
        artifacts_dir: str | None = None,
        accelerators: str | dict[str, int] | None = None,
        cpus: int | float | str | None = None,
        memory: int | float | str | None = None,
        disk_size: int | None = None,
        ports: int | str | list[str] | None = None,
        instance_type: str | None = None,
        image_id: str | None = None,
        region: str | None = None,
        zone: str | None = None,
    ) -> None:
        self.cloud = cloud
        self.workdir = workdir
        self.setup = setup
        self.run = run
        self.envs = envs
        self.artifacts_dir = artifacts_dir
        self.accelerators = accelerators
        self.cpus = cpus
        self.memory = memory
        self.disk_size = disk_size
        self.ports = ports
        self.instance_type = instance_type
        self.image_id = image_id
        self.region = region
        self.zone = zone

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

    def submit(self, idle_minutes: int = 1) -> None:
        if self.submitted:
            logger.error("Attempted to submit a job that is already submitted.")
            raise SkyError("Job already submitted")

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

    def status(self) -> sky.JobStatus:
        if not self.submitted:
            logger.error("Attempted to get a status from a job that is not yet submitted.")
            raise SkyError("Job not yet submitted")

        with sky.sky_logging.silent():
            sky_status = sky.job_status(self.cluster_name, job_ids=[self.job_id]).get(self.job_id)

        logger.debug("Job %s status is: %s", self.job_uuid, sky_status)

        return sky_status

    def get_job_head_ip(self) -> str:
        if not self.submitted or self._job_resource_handle is None:
            logger.error("Attempted to get a head IP from a job that is not yet submitted.")
            raise SkyError("Job not yet submitted")
        return str(self._job_resource_handle.head_ip)

    def get_job_ssh_port(self) -> int | None:
        """
        Get the SSH port directly from the job resource handle.

        Returns:
            The SSH port if available, None otherwise.

        """
        if not self._job_resource_handle:
            logger.warning("No job resource handle available")
            return None

        try:
            ports = self._job_resource_handle.external_ssh_ports()
            if len(ports) > 0:
                port = ports[0]
                logger.info("Found SSH port in job resource handle: %d", port)
                return int(port)
            logger.warning("No SSH port found in job resource handle")
        except Exception as e:
            logger.warning("Failed to get SSH port from job resource handle: %s", e)
        return None

    def output(self) -> str:
        if not self.submitted:
            logger.error("Attempted to get an output from a job that is not yet submitted.")
            raise SkyError("Job not yet submitted")

        with sky.sky_logging.silent():
            logs_dir = sky.download_logs(self.cluster_name, [str(self.job_id)], local_dir=str(self.workdir))[
                str(self.job_id)
            ]

        logger.debug("Job %s output downloaded to: %s", self.job_uuid, logs_dir)

        return (Path(logs_dir) / "tasks/run.log").read_text()

    def download(self, source_dir: str, max_size: int = 1_000_000) -> dict[str, bytes]:
        if not self.submitted:
            logger.error("Attempted to download artifacts from a job that is not yet submitted.")
            raise SkyError("Job not yet submitted")

        assert self._job_resource_handle is not None  # make mypy happy
        head_runner = self._job_resource_handle.get_command_runners()[0]

        source = Path(source_dir)
        destination = Path(tempfile.mkdtemp(prefix="sky-download-", dir=self.workdir))
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
                    path = str(source / item.name)
                    artifacts[path] = base64.b64encode(content)
                else:
                    logger.error(f"Artifact {item} too large: {size:,} bytes")

        return artifacts

    @cache
    def _get_resources(self) -> sky.Resources:
        return sky.Resources(
            cloud=sky.clouds.CLOUD_REGISTRY.from_str(str(self.cloud)),
            accelerators=self.accelerators,
            cpus=self.cpus,
            memory=self.memory,
            disk_size=self.disk_size,
            ports=self.ports,
            instance_type=self.instance_type,
            image_id=self.image_id,
            region=self.region,
            zone=self.zone,
        )

    @cache
    def _get_task(self, resources: sky.Resources | None = None) -> sky.Task:
        task = sky.Task(
            setup=self.setup,
            run=self.run,
            envs=self.envs,
            workdir=str(self.workdir),
        )
        if resources is not None:
            task = task.set_resources(resources)

        return task


SkyJobStatus = sky.JobStatus
