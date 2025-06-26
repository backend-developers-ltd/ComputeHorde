from dataclasses import dataclass
from enum import StrEnum

from compute_horde_core.executor_class import ExecutorClass
from compute_horde_validator.settings import PROMPT_JOB_GENERATOR


class PromptGenerationProfile(StrEnum):
    default_a6000 = "default_a6000"
    default_a100 = "default_a100"


@dataclass
class GenerationProfileSpec:
    description: str
    num_prompts: int


PROMPT_GENERATION_PROFILES = {
    PromptGenerationProfile.default_a6000: GenerationProfileSpec(
        description="The default generation profile for a6000 cards",
        num_prompts=240
    ),
    # this is a currently unused in production, fake generation profile
    # that's used for testing the multi-hw support
    PromptGenerationProfile.default_a100: GenerationProfileSpec(
        description="The default generation profile for a100 cards",
        num_prompts=777
    )
}


EXECUTOR_TO_PROMPT_GENERATION_PROFILE = {
    ExecutorClass.always_on__llm__a6000: PromptGenerationProfile.default_a6000,
}
