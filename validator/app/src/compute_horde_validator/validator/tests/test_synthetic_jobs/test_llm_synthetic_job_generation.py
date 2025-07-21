import asyncio
import uuid
from constance import config
from django.utils.timezone import now
from unittest.mock import patch
from unittest.mock import Mock
from unittest.mock import AsyncMock

import pytest
from compute_horde.executor_class import LLM_EXECUTOR_CLASSES
from compute_horde_core.executor_class import ExecutorClass

from compute_horde_validator.validator.generation_profile import (
    PromptGenerationProfile,
    GenerationProfileSpec,
    PROMPT_GENERATION_PROFILES,
    EXECUTOR_TO_PROMPT_GENERATION_PROFILE,
)

from compute_horde_validator.validator.models import (
    Prompt,
    PromptSample,
    PromptSeries,
    SolveWorkload,
    SystemEvent,
)
from compute_horde_validator.validator.tasks import llm_prompt_generation, llm_prompt_sampling, llm_prompt_answering
from compute_horde_validator.validator.synthetic_jobs.generator.llm_prompts import LlmPromptsJobGenerator
from compute_horde_validator.validator.synthetic_jobs.batch_run import _generate_jobs, BatchContext

from .mock_generator import MockSyntheticJobGeneratorFactory

@pytest.fixture
def job_generator_factory():
    job_uuids = [uuid.uuid4() for _ in range(13)]
    return MockSyntheticJobGeneratorFactory(uuids=job_uuids)

@pytest.mark.override_config(DYNAMIC_TARGET_NUMBER_OF_PROMPT_SAMPLES_READY=5)
@pytest.mark.django_db(transaction=True)
def test_jobs_are_generated_for_executor_classes_which_have_samples():
    setup_two_profiles_and_two_llm_executor_classes()

    executors = {
        ExecutorClass.always_on__gpu_24gb: 1,
        ExecutorClass.always_on__llm__a6000: 2,
    }

    # generate PromptSample for always_on__gpu_24gb class
    prompt_sample = create_prompt_sample(ExecutorClass.always_on__gpu_24gb)

    # Create a BatchContext mock
    hotkey = 'hotkey'
    mock_ctx = create_batch_context(executors, hotkey)

    # generate jobs
    asyncio.run(_generate_jobs(mock_ctx))

    # verify only jobs for the executor class that has PromptSamples are generated
    assert len(mock_ctx.jobs) == 1
    for job in mock_ctx.jobs.values():
        assert job.executor_class == ExecutorClass.always_on__gpu_24gb

    assert len(mock_ctx.job_generators[hotkey][ExecutorClass.always_on__llm__a6000]) == 0
    assert len(mock_ctx.job_generators[hotkey][ExecutorClass.always_on__gpu_24gb]) == 1
    assert mock_ctx.job_generators[hotkey][ExecutorClass.always_on__gpu_24gb][0]._init_args['prompt_sample'] == prompt_sample
    assert mock_ctx.job_generators[hotkey][ExecutorClass.always_on__gpu_24gb][0]._init_args['expected_prompts'] == list(prompt_sample.prompts.all())


@pytest.mark.override_config(DYNAMIC_TARGET_NUMBER_OF_PROMPT_SAMPLES_READY=5)
@pytest.mark.django_db(transaction=True)
def test_jobs_are_generated_for_multiple_executor_classes():
    setup_two_profiles_and_two_llm_executor_classes()

    executors = {
        ExecutorClass.always_on__gpu_24gb: 1,
        ExecutorClass.always_on__llm__a6000: 2,
    }

    prompt_sample_gpu_24gb = create_prompt_sample(ExecutorClass.always_on__gpu_24gb)
    prompt_sample_a6000_1 = create_prompt_sample(ExecutorClass.always_on__llm__a6000)
    prompt_sample_a6000_2 = create_prompt_sample(ExecutorClass.always_on__llm__a6000)

    # Create a BatchContext mock
    hotkey = 'hotkey'
    mock_ctx = create_batch_context(executors, hotkey)

    # generate jobs
    asyncio.run(_generate_jobs(mock_ctx))

    # verify all the executors have jobs
    assert len(mock_ctx.jobs) == 3  # 1 for gpu_24gb + 2 for a6000

    # both jobs have relevant executor classes
    executor_classes_in_jobs = {job.executor_class for job in mock_ctx.jobs.values()}
    assert ExecutorClass.always_on__gpu_24gb in executor_classes_in_jobs
    assert ExecutorClass.always_on__llm__a6000 in executor_classes_in_jobs

    # there are 1 and 2 job generators, respectively
    assert len(mock_ctx.job_generators[hotkey][ExecutorClass.always_on__gpu_24gb]) == 1
    assert len(mock_ctx.job_generators[hotkey][ExecutorClass.always_on__llm__a6000]) == 2

    gpu_24gb_generators = mock_ctx.job_generators[hotkey][ExecutorClass.always_on__gpu_24gb]
    a6000_generators = mock_ctx.job_generators[hotkey][ExecutorClass.always_on__llm__a6000]

    # Collect all prompt samples used by gpu_24gb generators
    gpu_24gb_used_samples = {gen._init_args['prompt_sample'] for gen in gpu_24gb_generators}
    assert gpu_24gb_used_samples == {prompt_sample_gpu_24gb}

    # Collect all prompt samples used by a6000 generators
    a6000_used_samples = {gen._init_args['prompt_sample'] for gen in a6000_generators}
    assert a6000_used_samples == {prompt_sample_a6000_1, prompt_sample_a6000_2}

    for gen in gpu_24gb_generators:
        assert gen._init_args['expected_prompts'] == list(gen._init_args['prompt_sample'].prompts.all())

    for gen in a6000_generators:
        assert gen._init_args['expected_prompts'] == list(gen._init_args['prompt_sample'].prompts.all())


def setup_two_profiles_and_two_llm_executor_classes():
    LLM_EXECUTOR_CLASSES.clear()
    LLM_EXECUTOR_CLASSES.add(ExecutorClass.always_on__llm__a6000)
    LLM_EXECUTOR_CLASSES.add(ExecutorClass.always_on__gpu_24gb)

    EXECUTOR_TO_PROMPT_GENERATION_PROFILE.clear()
    PROMPT_GENERATION_PROFILES.clear()

    EXECUTOR_TO_PROMPT_GENERATION_PROFILE[ExecutorClass.always_on__llm__a6000] = PromptGenerationProfile.default_a6000
    PROMPT_GENERATION_PROFILES[PromptGenerationProfile.default_a6000] = GenerationProfileSpec(
            description="The default generation profile for a6000 cards",
            num_prompts=24)

    EXECUTOR_TO_PROMPT_GENERATION_PROFILE[ExecutorClass.always_on__gpu_24gb] = PromptGenerationProfile.default_a100
    PROMPT_GENERATION_PROFILES[PromptGenerationProfile.default_a100] = GenerationProfileSpec(
            description="Fake generation profile for a100 cards",
            num_prompts=13)


def create_prompt_sample(executor_class: ExecutorClass):
    prompt_series = PromptSeries.objects.create(s3_url="", generator_version=1, generation_profile=EXECUTOR_TO_PROMPT_GENERATION_PROFILE[executor_class])
    workload = SolveWorkload.objects.create(seed=17, s3_url="s3://test", executor_class=executor_class, finished_at=now())
    prompt_sample = PromptSample.objects.create(series=prompt_series, workload=workload)
    for prompt_idx in range(13):
        Prompt.objects.create(sample=prompt_sample, content=f"prompt{prompt_idx}", answer=f"answer{prompt_idx}")

    return prompt_sample


def create_batch_context(executors: dict[ExecutorClass, int], hotkey):
    mock_ctx = Mock(spec=BatchContext)
    mock_ctx.get_executor_count_per_class = BatchContext.get_executor_count_per_class.__get__(mock_ctx)
    mock_ctx.executors = {
        hotkey: executors
    }
    mock_ctx.names = {hotkey: 'test_miner'}
    mock_ctx.jobs = {}
    mock_ctx.job_uuids = []
    mock_ctx.job_generators = {hotkey: {}}

    return mock_ctx