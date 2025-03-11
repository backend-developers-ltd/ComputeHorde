import abc
import uuid

from compute_horde.base.docker import DockerRunOptionsPreset
from compute_horde.miner_client.organic import OrganicJobDetails
from compute_horde_core.executor_class import ExecutorClass
from compute_horde_core.output_upload import OutputUpload
from compute_horde_core.volume import Volume


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

    def docker_run_options_preset(self) -> DockerRunOptionsPreset:
        return "nvidia_all"

    def docker_run_cmd(self) -> list[str]:
        return []

    def volume(self) -> Volume | None:
        return None

    def output(self) -> OutputUpload | None:
        return None

    def get_job_details(self) -> OrganicJobDetails:
        return OrganicJobDetails(
            job_uuid=str(self._uuid),
            executor_class=self.executor_class(),
            docker_image=self.docker_image_name(),
            docker_run_options_preset=self.docker_run_options_preset(),
            docker_run_cmd=self.docker_run_cmd(),
            total_job_timeout=self.timeout_seconds(),
            volume=self.volume(),
            output=self.output(),
        )
