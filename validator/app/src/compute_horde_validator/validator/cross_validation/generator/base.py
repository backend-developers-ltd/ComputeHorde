import abc
import uuid

from compute_horde.base.docker import DockerRunOptionsPreset
from compute_horde.miner_client.organic import OrganicJobDetails
from compute_horde_core.executor_class import ExecutorClass
from compute_horde_core.output_upload import OutputUpload
from compute_horde_core.volume import Volume
from compute_horde_validator.validator.generation_profile import PromptGenerationProfile, PROMPT_GENERATION_PROFILES


class BasePromptJobGenerator(abc.ABC):
    def __init__(
        self,
        _uuid: uuid.UUID,
        *,
        profile: PromptGenerationProfile,
        batch_uuids: list[uuid.UUID],
        upload_urls: list[str],
    ) -> None:
        self._uuid = _uuid
        self.profile = profile
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
    """
        This is the executor class to run the generation job.
        It need not be related to the actual generation profile
    """

    def docker_run_options_preset(self) -> DockerRunOptionsPreset:
        return "nvidia_all"

    def docker_run_cmd(self) -> list[str]:
        return []

    def volume(self) -> Volume | None:
        return None

    def output(self) -> OutputUpload | None:
        return None

    def num_prompts_per_batch(self) -> int:
        return PROMPT_GENERATION_PROFILES[self.profile].num_prompts

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
