import abc
import uuid

from compute_horde.base.volume import Volume
from compute_horde.executor_class import ExecutorClass
from compute_horde.miner_client.organic import OrganicJobDetails


class BasePromptJobGenerator(abc.ABC):
    def __init__(
        self,
        _uuid: uuid.UUID,
        *,
        num_prompts_per_batch: int,
        batch_uuids: list[uuid.UUID],
        upload_urls: list[str],
    ) -> None:
        self._uuid = _uuid
        self.num_prompts_per_batch = num_prompts_per_batch
        self.batch_uuids = batch_uuids
        self.upload_urls = upload_urls

    @abc.abstractmethod
    def timeout_seconds(self) -> int: ...

    @abc.abstractmethod
    def generator_version(self) -> int: ...

    @abc.abstractmethod
    def docker_image_name(self) -> str: ...

    @abc.abstractmethod
    def executor_class(self) -> ExecutorClass: ...

    def docker_run_options_preset(self) -> str:
        return "nvidia_all"

    def docker_run_cmd(self) -> list[str]:
        return []

    def raw_script(self) -> str | None:
        return None

    def volume_contents(self) -> str:
        return None

    def volume(self) -> Volume | None:
        return None

    def output(self) -> str | None:
        return None

    def get_job_details(self) -> OrganicJobDetails:
        return OrganicJobDetails(
            job_uuid=str(self._uuid),
            executor_class=self.executor_class(),
            docker_image=self.docker_image_name(),
            raw_script=self.raw_script(),
            docker_run_options_preset=self.docker_run_options_preset(),
            docker_run_cmd=self.docker_run_cmd(),
            total_job_timeout=self.timeout_seconds(),
            volume=self.volume(),
            output=self.output(),
        )
