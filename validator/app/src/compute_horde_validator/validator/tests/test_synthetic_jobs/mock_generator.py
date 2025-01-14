import logging
import uuid

from compute_horde.base.docker import DockerRunOptionsPreset
from compute_horde.base.volume import InlineVolume, Volume
from compute_horde.executor_class import ExecutorClass
from compute_horde.mv_protocol.miner_requests import (
    V0JobFinishedRequest,
)

from compute_horde_validator.validator.models import (
    Prompt,
    PromptSample,
)
from compute_horde_validator.validator.synthetic_jobs.generator.base import (
    BaseSyntheticJobGenerator,
    BaseSyntheticJobGeneratorFactory,
)
from compute_horde_validator.validator.synthetic_jobs.generator.llm_prompts import (
    LlmPromptsSyntheticJobGenerator,
)

logger = logging.getLogger(__name__)

MOCK_SCORE = 0.8
NOT_SCORED = 0.0


class MockSyntheticJobGenerator(BaseSyntheticJobGenerator):
    def __init__(self, _uuid: uuid.UUID, **kwargs):
        super().__init__(**kwargs)
        self._uuid = _uuid

    async def ainit(self, miner_hotkey: str):
        pass

    def timeout_seconds(self) -> int:
        return 1

    def base_docker_image_name(self) -> str:
        return "mock"

    def docker_image_name(self) -> str:
        return "mock"

    def docker_run_options_preset(self) -> DockerRunOptionsPreset:
        return "none"

    def docker_run_cmd(self) -> list[str]:
        return ["mock"]

    async def volume(self) -> Volume | None:
        return InlineVolume(contents="mock")

    def verify(self, msg: V0JobFinishedRequest, time_took: float) -> tuple[bool, str, float]:
        return True, "mock", MOCK_SCORE

    def job_description(self) -> str:
        return "mock"


class TimeTookScoreMockSyntheticJobGenerator(MockSyntheticJobGenerator):
    def verify(self, msg: V0JobFinishedRequest, time_took: float) -> tuple[bool, str, float]:
        return True, "mock", 1 / time_took


class MockSyntheticJobGeneratorFactory(BaseSyntheticJobGeneratorFactory):
    def __init__(self, uuids: list[uuid.UUID] = None, **kwargs):
        super().__init__(**kwargs)
        self._uuids = uuids.copy() if uuids else []

    async def create(self, executor_class: ExecutorClass, **kwargs) -> BaseSyntheticJobGenerator:
        _uuid = self._uuids.pop(0)
        return MockSyntheticJobGenerator(_uuid, **kwargs)


class LlmPromptsSyntheticJobGeneratorFactory:
    def __init__(
        self, uuids: list[uuid.UUID], prompt_samples: list[PromptSample], prompts: list[Prompt]
    ):
        self._uuids = uuids
        self._prompt_samples = prompt_samples
        self._prompts = prompts

    async def create(
        self, executor_class: ExecutorClass, *args, **kwargs
    ) -> BaseSyntheticJobGenerator:
        generator = LlmPromptsSyntheticJobGenerator(
            prompt_sample=self._prompt_samples.pop(0),
            expected_prompts=self._prompts,
            s3_url="mock",
            seed=0,
            streaming=False,
        )
        generator._uuid = self._uuids.pop(0)
        return generator


class TimeTookScoreMockSyntheticJobGeneratorFactory(MockSyntheticJobGeneratorFactory):
    async def create(self, executor_class: ExecutorClass, *args) -> BaseSyntheticJobGenerator:
        _uuid = self._uuids.pop(0)
        return TimeTookScoreMockSyntheticJobGenerator(_uuid)
