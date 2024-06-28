from compute_horde.executor_class import ExecutorClass

from compute_horde_validator.validator.synthetic_jobs.generator.base import (
    BaseSyntheticJobGenerator,
    BaseSyntheticJobGeneratorFactory,
)
from compute_horde_validator.validator.synthetic_jobs.generator.gpu_hashcat import (
    GPUHashcatSyntheticJobGenerator,
)


class DefaultSyntheticJobGeneratorFactory(BaseSyntheticJobGeneratorFactory):
    async def create(self, executor_class: ExecutorClass) -> BaseSyntheticJobGenerator:
        return GPUHashcatSyntheticJobGenerator()
