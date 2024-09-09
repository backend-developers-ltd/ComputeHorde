from compute_horde.executor_class import ExecutorClass

from compute_horde_validator.validator.synthetic_jobs.generator.base import (
    BaseSyntheticJobGenerator,
    BaseSyntheticJobGeneratorFactory,
)
from compute_horde_validator.validator.synthetic_jobs.generator.gpu_hashcat import (
    GPUHashcatSyntheticJobGenerator,
)
from compute_horde_validator.validator.synthetic_jobs.generator.llama_prompts import (
    LlamaPromptsSyntheticJobGenerator,
)


class DefaultSyntheticJobGeneratorFactory(BaseSyntheticJobGeneratorFactory):
    async def create(self, executor_class: ExecutorClass, *args) -> BaseSyntheticJobGenerator:
        if executor_class == ExecutorClass.always_on__llama:
            return LlamaPromptsSyntheticJobGenerator(*args)
        return GPUHashcatSyntheticJobGenerator(*args)
