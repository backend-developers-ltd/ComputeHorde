import abc
import asyncio
import uuid

from compute_horde.base.docker import DockerRunOptionsPreset
from compute_horde.mv_protocol.miner_requests import V0JobFinishedRequest
from compute_horde_core.executor_class import ExecutorClass
from compute_horde_core.output_upload import OutputUpload
from compute_horde_core.volume import Volume


class BaseSyntheticJobGenerator(abc.ABC):
    def __init__(self, **kwargs):
        self._uuid = uuid.uuid4()

    def __repr__(self):
        return f"{self.__class__.__name__}({self._uuid})"

    async def ainit(self, miner_hotkey: str):
        """Allow to initialize generator in asyncio and non blocking"""

    def uuid(self) -> uuid.UUID:
        return self._uuid

    @abc.abstractmethod
    def timeout_seconds(self) -> int: ...

    @abc.abstractmethod
    def base_docker_image_name(self) -> str: ...

    @abc.abstractmethod
    def docker_image_name(self) -> str: ...

    @abc.abstractmethod
    def docker_run_options_preset(self) -> DockerRunOptionsPreset: ...

    async def streaming_preparation_timeout(self) -> float | None:
        """For streaming jobs, the timeout between sending a JobRequest and receiving StreamingReadyRequest"""
        return None

    async def trigger_streaming_job_execution(
        self,
        job_uuid,
        start_barrier: asyncio.Barrier,
        server_public_key,
        client_key_pair,
        server_address,
        server_port,
    ):
        """For streaming jobs, perform whatever calls are necessary to trigger actions required to perform validation
        of given executor class."""
        raise NotImplementedError

    def docker_run_cmd(self) -> list[str]:
        return []

    def raw_script(self) -> str | None:
        return None

    @abc.abstractmethod
    async def volume(self) -> Volume | None: ...

    async def output_upload(self) -> OutputUpload | None:
        return None

    @abc.abstractmethod
    def verify_time(self, time_took: float) -> bool | None:
        """Check whether the job finished in time. Return None if no data available (for example it didn't finish)"""

    @abc.abstractmethod
    def verify_correctness(self, msg: V0JobFinishedRequest) -> tuple[bool, str]:
        """Check whether the job yielded the right result."""

    @abc.abstractmethod
    def job_description(self) -> str: ...

    def volume_in_initial_req(self) -> bool:
        return False

    def get_execution_time(self) -> float | None:
        """Return just the execution time of a job (for reporting reasons) - useful if the job has custom logic of
        measuring time. If None, reports will contain the broad time the job took."""
        return None


class BaseSyntheticJobGeneratorFactory(abc.ABC):
    @abc.abstractmethod
    async def create(
        self, executor_class: ExecutorClass, **kwargs
    ) -> BaseSyntheticJobGenerator: ...
