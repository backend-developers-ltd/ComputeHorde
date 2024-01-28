import abc

from compute_horde.mv_protocol.miner_requests import V0JobFinishedRequest


class AbstractChallengeGenerator(abc.ABC):
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
    def volume_contents(self) -> str:
        ...

    @abc.abstractmethod
    def verify(self, msg: V0JobFinishedRequest) -> bool:
        ...
