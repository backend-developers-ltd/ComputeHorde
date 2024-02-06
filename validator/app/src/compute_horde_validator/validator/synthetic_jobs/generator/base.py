import abc

from compute_horde.mv_protocol.miner_requests import V0JobFinishedRequest


class AbstractSyntheticJobGenerator(abc.ABC):
    @abc.abstractmethod
    def timeout_seconds(self) -> int:
        ...

    @abc.abstractmethod
    def base_docker_image_name(self) -> str:
        ...

    @abc.abstractmethod
    def docker_image_name(self) -> str:
        ...

    @abc.abstractmethod
    def docker_run_options_preset(self) -> str:
        ...

    @abc.abstractmethod
    def docker_run_cmd(self) -> list[str]:
        ...

    @abc.abstractmethod
    def volume_contents(self) -> str:
        ...

    @abc.abstractmethod
    def verify(self, msg: V0JobFinishedRequest, time_took: float) -> tuple[bool, str, float]:
        ...
