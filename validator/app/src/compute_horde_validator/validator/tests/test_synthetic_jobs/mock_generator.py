import uuid

from compute_horde.base.volume import InlineVolume, Volume
from compute_horde.executor_class import ExecutorClass
from compute_horde.mv_protocol.miner_requests import (
    V0JobFinishedRequest,
)

from compute_horde_validator.validator.synthetic_jobs.generator.base import (
    BaseSyntheticJobGenerator,
    BaseSyntheticJobGeneratorFactory,
)

MOCK_SCORE = 0.8
NOT_SCORED = 0.0


class MockSyntheticJobGenerator(BaseSyntheticJobGenerator):
    def __init__(self, _uuid: uuid.UUID):
        self._uuid = _uuid

    async def ainit(self):
        pass

    def timeout_seconds(self) -> int:
        return 1

    def base_docker_image_name(self) -> str:
        return "mock"

    def docker_image_name(self) -> str:
        return "mock"

    def docker_run_options_preset(self) -> str:
        return "mock"

    def docker_run_cmd(self) -> list[str]:
        return ["mock"]

    async def volume(self) -> Volume:
        return InlineVolume(contents="mock")

    def verify(self, msg: V0JobFinishedRequest, time_took: float) -> tuple[bool, str, float]:
        return True, "mock", MOCK_SCORE

    def job_description(self) -> str:
        return "mock"


class TimeTookScoreMockSyntheticJobGenerator(MockSyntheticJobGenerator):
    def verify(self, msg: V0JobFinishedRequest, time_took: float) -> tuple[bool, str, float]:
        return True, "mock", 1 / time_took


class MockSyntheticJobGeneratorFactory(BaseSyntheticJobGeneratorFactory):
    def __init__(self, uuids: list[uuid.UUID] = None):
        self._uuids = uuids or []

    async def create(self, executor_class: ExecutorClass) -> BaseSyntheticJobGenerator:
        _uuid = self._uuids.pop(0)
        return MockSyntheticJobGenerator(_uuid)


class TimeTookScoreMockSyntheticJobGeneratorFactory(MockSyntheticJobGeneratorFactory):
    async def create(self, executor_class: ExecutorClass) -> BaseSyntheticJobGenerator:
        _uuid = self._uuids.pop(0)
        return TimeTookScoreMockSyntheticJobGenerator(_uuid)
