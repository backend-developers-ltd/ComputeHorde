import uuid
from constance import config
from unittest.mock import patch
from unittest.mock import Mock
from unittest.mock import AsyncMock

import pytest
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

def create_prompt_series(num: int, generation_profile=PromptGenerationProfile.default_a6000, s3_url=""):
    PromptSeries.objects.bulk_create(
        [PromptSeries(s3_url=s3_url, generator_version=1, generation_profile=generation_profile) for _ in range(num)]
    )


def default_generation_profiles_configuration_for_a6000_only():
    EXECUTOR_TO_PROMPT_GENERATION_PROFILE.clear()
    PROMPT_GENERATION_PROFILES.clear()

    EXECUTOR_TO_PROMPT_GENERATION_PROFILE[ExecutorClass.always_on__llm__a6000] = PromptGenerationProfile.default_a6000
    PROMPT_GENERATION_PROFILES[PromptGenerationProfile.default_a6000] = GenerationProfileSpec(
            description="The default generation profile for a6000 cards",
            num_prompts=240)


@pytest.mark.override_config(DYNAMIC_TARGET_NUMBER_OF_PROMPT_SAMPLES_READY=5)
@pytest.mark.django_db(transaction=True)
def test_llm_prompt_sampling__will_not_trigger():
    default_generation_profiles_configuration_for_a6000_only()
    create_prompt_series(5)
    prompt_series = PromptSeries.objects.create(s3_url="", generator_version=1)
    for i in range(5):
        workload = SolveWorkload.objects.create(seed=i, s3_url="s3://test")
        # PromptSample(series=prompt_series, workload=workload)

    with patch("compute_horde_validator.validator.tasks.create_sample_workloads", lambda *_: 0):
        llm_prompt_sampling()
        assert (
            SystemEvent.objects.filter(
                subtype=SystemEvent.EventSubType.PROMPT_SAMPLING_SKIPPED
            ).count()
            == 1
        )


@pytest.mark.override_config(DYNAMIC_TARGET_NUMBER_OF_PROMPT_SAMPLES_READY=5)
@pytest.mark.django_db(transaction=True)
@patch("compute_horde_validator.validator.tasks.upload_prompts_to_s3_url", lambda *args: False)
@patch(
    "compute_horde_validator.validator.tasks.download_prompts_from_s3_url",
    lambda *args: ["test" for _ in range(10)],
)
def test_llm_prompt_sampling__fail_upload_to_s3():
    default_generation_profiles_configuration_for_a6000_only()
    create_prompt_series(4)
    llm_prompt_sampling()
    assert SolveWorkload.objects.count() == 0
    assert PromptSample.objects.count() == 0
    assert Prompt.objects.count() == 0


@pytest.mark.override_config(DYNAMIC_TARGET_NUMBER_OF_PROMPT_SAMPLES_READY=5)
@pytest.mark.django_db(transaction=True)
@patch("compute_horde_validator.validator.tasks.download_prompts_from_s3_url", lambda *args: [])
def test_llm_prompt_sampling__fail_download_from_s3():
    default_generation_profiles_configuration_for_a6000_only()
    create_prompt_series(4)
    llm_prompt_sampling()
    assert SolveWorkload.objects.count() == 0
    assert PromptSample.objects.count() == 0
    assert Prompt.objects.count() == 0


@pytest.mark.override_config(
    DYNAMIC_TARGET_NUMBER_OF_PROMPT_SAMPLES_READY=5,
    DYNAMIC_NUMBER_OF_PROMPTS_TO_SAMPLE_FROM_SERIES=10,
)
@pytest.mark.django_db(transaction=True)
@patch("compute_horde_validator.validator.tasks.upload_prompts_to_s3_url", lambda *args: True)
@patch(
    "compute_horde_validator.validator.tasks.download_prompts_from_s3_url",
    lambda *args: ["test" for _ in range(20)],
)
def test_llm_prompt_sampling__success():
    default_generation_profiles_configuration_for_a6000_only()
    PROMPT_GENERATION_PROFILES[PromptGenerationProfile.default_a6000].num_prompts = 20
    create_prompt_series(10, generation_profile=PromptGenerationProfile.default_a6000)
    llm_prompt_sampling()
    assert SolveWorkload.objects.count() == 3
    assert PromptSample.objects.count() == 6
    assert Prompt.objects.count() == 60


@pytest.mark.override_config(
    DYNAMIC_TARGET_NUMBER_OF_PROMPT_SAMPLES_READY=4,
    DYNAMIC_NUMBER_OF_PROMPTS_TO_SAMPLE_FROM_SERIES=100,
)
@pytest.mark.django_db(transaction=True)
@patch("compute_horde_validator.validator.tasks.upload_prompts_to_s3_url", lambda *args: True)
@patch(
    "compute_horde_validator.validator.tasks.download_prompts_from_s3_url",
    lambda *args: ["test" for _ in range(101)],
)
def test_llm_prompt_sampling__one_sample_per_workload():
    # FIXME: this test doesn't seem to make sense;
    # FIXME: it tests series larger than solve workloads, but is this even a valid configuration?
    default_generation_profiles_configuration_for_a6000_only()
    PROMPT_GENERATION_PROFILES[PromptGenerationProfile.default_a6000].num_prompts = 100
    create_prompt_series(8)
    llm_prompt_sampling()
    assert SolveWorkload.objects.count() == 4
    assert PromptSample.objects.count() == 4
    assert Prompt.objects.count() == 400


@pytest.mark.override_config(
    DYNAMIC_TARGET_NUMBER_OF_PROMPT_SAMPLES_READY=1,
    DYNAMIC_NUMBER_OF_PROMPTS_TO_SAMPLE_FROM_SERIES=1,
)
@pytest.mark.django_db(transaction=True)
@patch("compute_horde_validator.validator.tasks.upload_prompts_to_s3_url", lambda *args: True)
@patch(
    "compute_horde_validator.validator.tasks.download_prompts_from_s3_url",
    lambda *args: ["test" for _ in range(5)],
)
def test_llm_prompt_sampling__not_enough_for_one_workload():
    default_generation_profiles_configuration_for_a6000_only()
    PROMPT_GENERATION_PROFILES[PromptGenerationProfile.default_a6000].num_prompts = 5
    create_prompt_series(4)
    llm_prompt_sampling()
    assert SolveWorkload.objects.count() == 0
    assert PromptSample.objects.count() == 0
    assert Prompt.objects.count() == 0


@pytest.mark.override_config(DYNAMIC_MAX_PROMPT_SERIES_PER_EXECUTOR_CLASS=5)
@pytest.mark.django_db(transaction=True)
def test_llm_prompt_generation__will_trigger():
    default_generation_profiles_configuration_for_a6000_only()
    create_prompt_series(4)
    with patch("compute_horde_validator.validator.tasks.generate_prompts") as mock_generate_prompts:
        llm_prompt_generation()
        assert mock_generate_prompts.called


@pytest.mark.override_config(DYNAMIC_MAX_PROMPT_SERIES_PER_EXECUTOR_CLASS=5)
@pytest.mark.django_db(transaction=True)
def test_llm_prompt_generation__will_not_trigger():
    default_generation_profiles_configuration_for_a6000_only()
    create_prompt_series(10)
    with patch("compute_horde_validator.validator.tasks.generate_prompts") as mock_generate_prompts:
        llm_prompt_generation()
        assert mock_generate_prompts.not_called
        assert PromptSeries.objects.count() == 10


@pytest.mark.override_config(DYNAMIC_MAX_PROMPT_SERIES_PER_EXECUTOR_CLASS=5)
@pytest.mark.django_db(transaction=True)
def test_llm_prompt_generation__skips_generation_when_profile_unused():
    default_generation_profiles_configuration_for_a6000_only()
    PROMPT_GENERATION_PROFILES[PromptGenerationProfile.default_a100] = GenerationProfileSpec(
            description="Unused profile",
            num_prompts=777)

    with patch("compute_horde_validator.validator.tasks.generate_prompts") as mock_generate_prompts:
        llm_prompt_generation()
        mock_generate_prompts.assert_awaited_once()


@pytest.mark.override_config(DYNAMIC_MAX_PROMPT_SERIES_PER_EXECUTOR_CLASS=5)
@pytest.mark.django_db(transaction=True)
def test_llm_prompt_generation__skips_generation_when_workload_exists():
    default_generation_profiles_configuration_for_a6000_only()

    SolveWorkload.objects.create(seed=77, s3_url="s3://test", executor_class=ExecutorClass.always_on__llm__a6000)
    with patch("compute_horde_validator.validator.tasks.generate_prompts") as mock_generate_prompts:
        llm_prompt_generation()
        mock_generate_prompts.assert_not_awaited()
    assert PromptSeries.objects.count() == 0


@pytest.mark.override_config(DYNAMIC_MAX_PROMPT_SERIES_PER_EXECUTOR_CLASS=5, DYNAMIC_PROMPTS_SERIES_IN_A_SINGLE_GENERATION=3)
@pytest.mark.django_db(transaction=True)
def test_llm_prompt_generation__generates_enough_series_to_satisfy_all_executors_sharing_a_generation_profile():
    default_generation_profiles_configuration_for_a6000_only()
    # two executor classes use the same generation profile
    EXECUTOR_TO_PROMPT_GENERATION_PROFILE[ExecutorClass.always_on__llm__a6000] = PromptGenerationProfile.default_a6000
    EXECUTOR_TO_PROMPT_GENERATION_PROFILE[ExecutorClass.always_on__gpu_24gb] = PromptGenerationProfile.default_a6000

    # generate more series than needed for 1 executor class but not enough for both executor classes
    existing_prompt_series_count = 6
    assert existing_prompt_series_count > config.DYNAMIC_MAX_PROMPT_SERIES_PER_EXECUTOR_CLASS
    assert existing_prompt_series_count < 2 * config.DYNAMIC_MAX_PROMPT_SERIES_PER_EXECUTOR_CLASS
    create_prompt_series(existing_prompt_series_count, generation_profile=PromptGenerationProfile.default_a6000)

    with patch("compute_horde_validator.validator.cross_validation.prompt_generation.run_organic_job") as mock_run_organic_job:
        llm_prompt_generation()
        assert mock_run_organic_job.await_count == 1

    assert PromptSeries.objects.count() == existing_prompt_series_count + config.DYNAMIC_PROMPTS_SERIES_IN_A_SINGLE_GENERATION


@pytest.mark.override_config(DYNAMIC_MAX_PROMPT_SERIES_PER_EXECUTOR_CLASS=5, DYNAMIC_PROMPTS_SERIES_IN_A_SINGLE_GENERATION=3)
@pytest.mark.django_db(transaction=True)
def test_llm_prompt_generation__generates_prompts_for_multiple_profiles():
    default_generation_profiles_configuration_for_a6000_only()
    # add another (fake) profile/executor pair
    PROMPT_GENERATION_PROFILES[PromptGenerationProfile.default_a100] = GenerationProfileSpec(
            description="Additional testing profile",
            num_prompts=234)
    EXECUTOR_TO_PROMPT_GENERATION_PROFILE[ExecutorClass.always_on__gpu_24gb] = PromptGenerationProfile.default_a100

    with patch("compute_horde_validator.validator.cross_validation.prompt_generation.run_organic_job") as mock_run_organic_job:
        llm_prompt_generation()
        assert mock_run_organic_job.await_count == 2

    assert PromptSeries.objects.count() == len(PROMPT_GENERATION_PROFILES) * config.DYNAMIC_PROMPTS_SERIES_IN_A_SINGLE_GENERATION


@pytest.mark.django_db(transaction=True)
def test_llm_prompt_sampling__skips_sampling_when_unused_prompt_samples_exist():
    default_generation_profiles_configuration_for_a6000_only()
    create_prompt_series(5)
    prompt_series = PromptSeries.objects.create(s3_url="", generator_version=1)
    workload = SolveWorkload.objects.create(seed=17, s3_url="s3://test", executor_class=ExecutorClass.always_on__llm__a6000)
    PromptSample.objects.create(series=prompt_series, workload=workload)

    llm_prompt_sampling()
    assert (
        SystemEvent.objects.filter(
            subtype=SystemEvent.EventSubType.PROMPT_SAMPLING_SKIPPED
        ).count()
        == 1
    )


@pytest.mark.override_config(
    DYNAMIC_TARGET_NUMBER_OF_PROMPT_SAMPLES_READY=7,
    DYNAMIC_NUMBER_OF_PROMPTS_TO_SAMPLE_FROM_SERIES=12,
)
@pytest.mark.django_db(transaction=True)
@patch("compute_horde_validator.validator.tasks.upload_prompts_to_s3_url", lambda *args: True)
@patch(
    "compute_horde_validator.validator.tasks.download_prompts_from_s3_url",
    lambda s3_url: ["test" for _ in range(s3_url == 's3://a6000' and 24 or 36)]
)
@pytest.mark.django_db(transaction=True)
def test_llm_prompt_sampling__generates_samples_for_multiple_executor_classes():
    default_generation_profiles_configuration_for_a6000_only()
    PROMPT_GENERATION_PROFILES[PromptGenerationProfile.default_a6000].num_prompts = 24
    # add another (fake) profile/executor pair
    PROMPT_GENERATION_PROFILES[PromptGenerationProfile.default_a100] = GenerationProfileSpec(
            description="Additional testing profile",
            num_prompts=36)
    EXECUTOR_TO_PROMPT_GENERATION_PROFILE[ExecutorClass.always_on__gpu_24gb] = PromptGenerationProfile.default_a100

    create_prompt_series(55, generation_profile=PromptGenerationProfile.default_a6000, s3_url="s3://a6000")
    create_prompt_series(78, generation_profile=PromptGenerationProfile.default_a100, s3_url="s3://a100")

    llm_prompt_sampling()

    # each workload for a6000 fits 24 prompts, so 2 samples, 12 prompts each
    # to get 7 samples, we need 4 workloads
    assert SolveWorkload.objects.filter(executor_class=ExecutorClass.always_on__llm__a6000).count() == 4
    assert (
        SystemEvent.objects.filter(
            subtype=SystemEvent.EventSubType.NEW_WORKLOADS_CREATED,
            data__executor_class=ExecutorClass.always_on__llm__a6000,
            data__new_workloads_count=4,
        ).count()
        == 1
    )

    # each workload for a100 fits 36 prompts, so 3 samples, 12 prompts each
    # to get 7 samples, we need 3 workloads
    assert SolveWorkload.objects.filter(executor_class=ExecutorClass.always_on__gpu_24gb).count() == 3
    assert (
        SystemEvent.objects.filter(
            subtype=SystemEvent.EventSubType.NEW_WORKLOADS_CREATED,
            data__executor_class=ExecutorClass.always_on__gpu_24gb,
            data__new_workloads_count=3,
        ).count()
        == 1
    )

    # 4 workloads for a6000 contain exactly 8 samples
    assert PromptSample.objects.filter(workload__executor_class=ExecutorClass.always_on__llm__a6000).count() == 8

    # 3 workloads for a100 contain 9 samples
    assert PromptSample.objects.filter(workload__executor_class=ExecutorClass.always_on__gpu_24gb).count() == 9

    assert Prompt.objects.count() == 8 * 12 + 9 * 12



@pytest.mark.override_config(
    DYNAMIC_TARGET_NUMBER_OF_PROMPT_SAMPLES_READY=7,
    DYNAMIC_NUMBER_OF_PROMPTS_TO_SAMPLE_FROM_SERIES=12,
)
@pytest.mark.django_db(transaction=True)
@patch(
    "compute_horde_validator.validator.tasks.download_prompts_from_s3_url",
    lambda s3_url: ["test" for _ in range(24)]
)
def test_llm_prompt_sampling__is_resilient_to_s3_upload_errors_when_creating_workloads():
    default_generation_profiles_configuration_for_a6000_only()
    PROMPT_GENERATION_PROFILES[PromptGenerationProfile.default_a6000].num_prompts = 24

    create_prompt_series(50, generation_profile=PromptGenerationProfile.default_a6000)

    with patch("compute_horde_validator.validator.tasks.upload_prompts_to_s3_url",
               Mock(side_effect=[True, Exception("Upload failed"), True, True, True])):
        llm_prompt_sampling()

    # there should have been upload error
    assert (
        SystemEvent.objects.filter(
            subtype=SystemEvent.EventSubType.ERROR_UPLOADING_TO_S3
        ).count()
        == 1
    )

    # but the workloads should have been created nevertheless
    # each workload for a6000 fits 24 prompts, so 2 samples, 12 prompts each
    # to get 7 samples, we need 4 workloads
    assert SolveWorkload.objects.filter(executor_class=ExecutorClass.always_on__llm__a6000).count() == 4

    # 4 workloads for a6000 contain exactly 8 samples
    assert PromptSample.objects.filter(workload__executor_class=ExecutorClass.always_on__llm__a6000).count() == 8

    assert Prompt.objects.count() == 8 * 12


@pytest.mark.override_config(
    DYNAMIC_TARGET_NUMBER_OF_PROMPT_SAMPLES_READY=7,
    DYNAMIC_NUMBER_OF_PROMPTS_TO_SAMPLE_FROM_SERIES=12,
)
@patch("compute_horde_validator.validator.tasks.upload_prompts_to_s3_url", lambda *args: True)
@pytest.mark.django_db(transaction=True)
def test_llm_prompt_sampling__is_resilient_to_s3_download_errors_when_creating_workloads():
    default_generation_profiles_configuration_for_a6000_only()
    PROMPT_GENERATION_PROFILES[PromptGenerationProfile.default_a6000].num_prompts = 24

    create_prompt_series(50, generation_profile=PromptGenerationProfile.default_a6000)

    with patch("compute_horde_validator.validator.tasks.download_prompts_from_s3_url",
        Mock(side_effect=[["test" for _ in range(24)],
                          Exception("Download failed")] +
                          [["test" for _ in range(24)]] * 50)):
        llm_prompt_sampling()

    # there should have been download error
    assert (
        SystemEvent.objects.filter(
            subtype=SystemEvent.EventSubType.ERROR_DOWNLOADING_FROM_S3
        ).count()
        == 1
    )

    # but the workloads should have been created nevertheless
    # each workload for a6000 fits 24 prompts, so 2 samples, 12 prompts each
    # to get 7 samples, we need 4 workloads
    assert SolveWorkload.objects.filter(executor_class=ExecutorClass.always_on__llm__a6000).count() == 4

    # 4 workloads for a6000 contain exactly 8 samples
    assert PromptSample.objects.filter(workload__executor_class=ExecutorClass.always_on__llm__a6000).count() == 8

    assert Prompt.objects.count() == 8 * 12



@pytest.mark.django_db(transaction=True)
def test_llm_prompt_answering__uses_executor_specified_by_workload():
    SolveWorkload.objects.create(seed=1, s3_url="s3://test1", executor_class=ExecutorClass.always_on__llm__a6000)
    SolveWorkload.objects.create(seed=2, s3_url="s3://test2", executor_class=ExecutorClass.always_on__llm__a6000)
    SolveWorkload.objects.create(seed=3, s3_url="s3://test3", executor_class=ExecutorClass.always_on__gpu_24gb)
    SolveWorkload.objects.create(seed=4, s3_url="s3://test4", executor_class=ExecutorClass.always_on__llm__a6000)

    async def set_answers(self, *args, **kwargs):
        self.prompt_answers = {f"prompt{i}": f"answer{i}" for i in range(10)}

    with patch("compute_horde_validator.validator.cross_validation.prompt_answering.run_organic_job") as mock_run_organic_job:
        with patch.object(LlmPromptsJobGenerator, "download_answers", autospec=True, side_effect=set_answers):

            llm_prompt_answering()
            assert mock_run_organic_job.await_count == 4
            assert mock_run_organic_job.await_args_list[0].args[1].executor_class == ExecutorClass.always_on__llm__a6000
            assert mock_run_organic_job.await_args_list[1].args[1].executor_class == ExecutorClass.always_on__llm__a6000
            assert mock_run_organic_job.await_args_list[2].args[1].executor_class == ExecutorClass.always_on__gpu_24gb
            assert mock_run_organic_job.await_args_list[3].args[1].executor_class == ExecutorClass.always_on__llm__a6000


