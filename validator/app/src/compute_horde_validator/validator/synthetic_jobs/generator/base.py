import abc
import uuid

from compute_horde.base.volume import Volume
from compute_horde.executor_class import ExecutorClass
from compute_horde.mv_protocol.miner_requests import V0JobFinishedRequest


class BaseSyntheticJobGenerator(abc.ABC):
    def __init__(self):
        self._uuid = uuid.uuid4()

    def __repr__(self):
        return f"{self.__class__.__name__}({self._uuid})"

    async def ainit(self):
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
    def docker_run_options_preset(self) -> str: ...

    def docker_run_cmd(self) -> list[str]:
        return []

    def raw_script(self) -> str | None:
        return None

    @abc.abstractmethod
    async def volume(self) -> Volume: ...

    @abc.abstractmethod
    def verify(self, msg: V0JobFinishedRequest, time_took: float) -> tuple[bool, str, float]: ...

    @abc.abstractmethod
    def job_description(self) -> str: ...


class BaseSyntheticJobGeneratorFactory(abc.ABC):
    @abc.abstractmethod
    async def create(self, executor_class: ExecutorClass) -> BaseSyntheticJobGenerator: ...
