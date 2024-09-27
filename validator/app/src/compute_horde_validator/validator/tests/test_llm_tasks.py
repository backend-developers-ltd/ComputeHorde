from unittest.mock import patch

import pytest

from compute_horde_validator.validator.models import (
    Prompt,
    PromptSample,
    PromptSeries,
    SolveWorkload,
)
from compute_horde_validator.validator.tasks import llm_prompt_generation, llm_prompt_sampling


def create_prompt_series(num: int):
    PromptSeries.objects.bulk_create(
        [PromptSeries(s3_url="", generator_version=1) for _ in range(num)]
    )


@pytest.mark.override_config(DYNAMIC_TARGET_NUMBER_OF_PROMPT_SAMPLES_READY=5)
@pytest.mark.django_db(transaction=True)
def test_llm_prompt_sampling__will_not_trigger():
    create_prompt_series(10)
    prompt_series = PromptSeries.objects.create(s3_url="", generator_version=1)
    for i in range(5):
        workload = SolveWorkload.objects.create(seed=i, s3_url="s3://test")
        PromptSample(series=prompt_series, workload=workload)

    with patch(
        "compute_horde_validator.validator.tasks.create_sample_workloads"
    ) as mock_create_sample_workloads:
        llm_prompt_sampling()
        assert mock_create_sample_workloads.called


@pytest.mark.override_config(DYNAMIC_TARGET_NUMBER_OF_PROMPT_SAMPLES_READY=5)
@pytest.mark.django_db(transaction=True)
@patch("compute_horde_validator.validator.tasks.upload_prompts_to_s3_url", lambda *args: False)
@patch(
    "compute_horde_validator.validator.tasks.download_prompts_from_s3_url",
    lambda *args: ["test" for _ in range(10)],
)
def test_llm_prompt_sampling__fail_upload_to_s3():
    create_prompt_series(4)
    llm_prompt_sampling()
    assert SolveWorkload.objects.count() == 0
    assert PromptSample.objects.count() == 0
    assert Prompt.objects.count() == 0


@pytest.mark.override_config(DYNAMIC_TARGET_NUMBER_OF_PROMPT_SAMPLES_READY=5)
@pytest.mark.django_db(transaction=True)
@patch("compute_horde_validator.validator.tasks.download_prompts_from_s3_url", lambda *args: [])
def test_llm_prompt_sampling__fail_download_from_s3():
    create_prompt_series(4)
    llm_prompt_sampling()
    assert SolveWorkload.objects.count() == 0
    assert PromptSample.objects.count() == 0
    assert Prompt.objects.count() == 0


@pytest.mark.override_config(
    DYNAMIC_TARGET_NUMBER_OF_PROMPT_SAMPLES_READY=5,
    DYNAMIC_NUMBER_OF_PROMPTS_TO_SAMPLE_FROM_SERIES=10,
    DYNAMIC_NUMBER_OF_PROMPTS_PER_WORKLOAD=20,
)
@pytest.mark.django_db(transaction=True)
@patch("compute_horde_validator.validator.tasks.upload_prompts_to_s3_url", lambda *args: True)
@patch(
    "compute_horde_validator.validator.tasks.download_prompts_from_s3_url",
    lambda *args: ["test" for _ in range(240)],
)
def test_llm_prompt_sampling__success():
    create_prompt_series(10)
    llm_prompt_sampling()
    assert SolveWorkload.objects.count() == 3
    assert PromptSample.objects.count() == 6
    assert Prompt.objects.count() == 60


@pytest.mark.override_config(
    DYNAMIC_TARGET_NUMBER_OF_PROMPT_SAMPLES_READY=4,
    DYNAMIC_NUMBER_OF_PROMPTS_TO_SAMPLE_FROM_SERIES=100,
    DYNAMIC_NUMBER_OF_PROMPTS_PER_WORKLOAD=80,
)
@pytest.mark.django_db(transaction=True)
@patch("compute_horde_validator.validator.tasks.upload_prompts_to_s3_url", lambda *args: True)
@patch(
    "compute_horde_validator.validator.tasks.download_prompts_from_s3_url",
    lambda *args: ["test" for _ in range(240)],
)
def test_llm_prompt_sampling__one_sample_per_workload():
    create_prompt_series(8)
    llm_prompt_sampling()
    assert SolveWorkload.objects.count() == 4
    assert PromptSample.objects.count() == 4
    assert Prompt.objects.count() == 400


@pytest.mark.override_config(
    DYNAMIC_TARGET_NUMBER_OF_PROMPT_SAMPLES_READY=1,
    DYNAMIC_NUMBER_OF_PROMPTS_TO_SAMPLE_FROM_SERIES=1,
    DYNAMIC_NUMBER_OF_PROMPTS_PER_WORKLOAD=5,
)
@pytest.mark.django_db(transaction=True)
@patch("compute_horde_validator.validator.tasks.upload_prompts_to_s3_url", lambda *args: True)
@patch(
    "compute_horde_validator.validator.tasks.download_prompts_from_s3_url",
    lambda *args: ["test" for _ in range(240)],
)
def test_llm_prompt_sampling__not_enough_for_one_workload():
    create_prompt_series(4)
    llm_prompt_sampling()
    assert SolveWorkload.objects.count() == 0
    assert PromptSample.objects.count() == 0
    assert Prompt.objects.count() == 0


@pytest.mark.override_config(DYNAMIC_MAX_PROMPT_SERIES=5)
@pytest.mark.django_db(transaction=True)
def test_llm_prompt_generation__will_trigger():
    create_prompt_series(4)
    with patch("compute_horde_validator.validator.tasks.generate_prompts") as mock_generate_prompts:
        llm_prompt_generation()
        assert mock_generate_prompts.called


@pytest.mark.override_config(DYNAMIC_MAX_PROMPT_SERIES=5)
@pytest.mark.django_db(transaction=True)
def test_llm_prompt_generation__will_not_trigger():
    create_prompt_series(10)
    with patch("compute_horde_validator.validator.tasks.generate_prompts") as mock_generate_prompts:
        llm_prompt_generation()
        assert mock_generate_prompts.not_called
        assert PromptSeries.objects.count() == 10
