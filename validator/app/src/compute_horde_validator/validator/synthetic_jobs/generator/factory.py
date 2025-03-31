from compute_horde_core.executor_class import ExecutorClass
from compute_horde.executor_class import LLM_EXECUTOR_CLASSES

from compute_horde_validator.validator.synthetic_jobs.generator.base import (
    BaseSyntheticJobGenerator,
    BaseSyntheticJobGeneratorFactory,
)
from compute_horde_validator.validator.synthetic_jobs.generator.gpu_hashcat import (
    GPUHashcatSyntheticJobGenerator,
)
from compute_horde_validator.validator.synthetic_jobs.generator.llm_prompts import (
    LlmPromptsSyntheticJobGenerator,
)


class DefaultSyntheticJobGeneratorFactory(BaseSyntheticJobGeneratorFactory):
    async def create(self, executor_class: ExecutorClass, **kwargs) -> BaseSyntheticJobGenerator:
        if executor_class in LLM_EXECUTOR_CLASSES:
            return LlmPromptsSyntheticJobGenerator(**kwargs)
        return GPUHashcatSyntheticJobGenerator(**kwargs)
