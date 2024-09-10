from collections.abc import Iterable
from unittest.mock import MagicMock, patch

import pytest
from faker import Faker

from compute_horde_miner.miner.models import (
    Prompt,
    PromptSample,
    PromptSeries,
    SolveWorkload,
    Validator,
)
from compute_horde_miner.miner.tasks import create_sample_workloads, fetch_validators


@pytest.mark.django_db(databases=["default", "default_alias"], transaction=True)
@patch("your_module.get_number_of_workloads_to_trigger_local_inference", 2)
@patch("your_module.get_number_of_prompts_to_validate_from_series", 3)
@patch("your_module.get_prompts_from_s3_url", return_value=[f"prompt{i}" for i in range(5)])
@patch("your_module.random.sample", return_value=[f"prompt{i}" for i in range(3)])
def test_create_sample_workloads(self):
    # Create test data
    for i in range(3):
        PromptSeries.objects.create(uuid=f"test-uuid-{i}", s3_url=f"s3://test-bucket/test-key-{i}")

    create_sample_workloads()

    self.assertEqual(SolveWorkload.objects.count(), 2)
    self.assertEqual(PromptSample.objects.count(), 2)
    self.assertEqual(Prompt.objects.count(), 6)


@patch("your_module.get_number_of_workloads_to_trigger_local_inference", 1)
@patch("your_module.get_number_of_prompts_to_validate_from_series", 3)
@patch("your_module.get_prompts_from_s3_url", return_value=[f"prompt{i}" for i in range(2)])
def test_create_sample_workloads_not_enough_prompts(self):
    PromptSeries.objects.create(uuid="test-uuid", s3_url="s3://test-bucket/test-key")

    create_sample_workloads()

    self.assertEqual(SolveWorkload.objects.count(), 1)
    self.assertEqual(PromptSample.objects.count(), 0)
    self.assertEqual(Prompt.objects.count(), 0)


@patch("your_module.get_number_of_workloads_to_trigger_local_inference", 1)
@patch("your_module.get_number_of_prompts_to_validate_from_series", 3)
@patch("your_module.get_prompts_from_s3_url", return_value=[f"prompt{i}" for i in range(5)])
@patch("your_module.random.sample", return_value=[f"prompt{i}" for i in range(3)])
def test_create_sample_workloads_delete_empty_workload(self):
    PromptSeries.objects.create(uuid="test-uuid", s3_url="s3://test-bucket/test-key")

    create_sample_workloads()

    self.assertEqual(SolveWorkload.objects.count(), 1)
    self.assertEqual(PromptSample.objects.count(), 1)
    self.assertEqual(Prompt.objects.count(), 3)

    self.assertEqual(SolveWorkload.objects.filter(prompt_sample__isnull=True).count(), 0)


@pytest.fixture(autouse=True)
def mock_get_validators(mocker):
    return mocker.patch("compute_horde_miner.miner.tasks.get_validators")


@pytest.fixture
def active_validators_keys(faker: Faker):
    return [faker.pystr() for _ in range(5)]


@pytest.fixture
def inactive_validators_keys(faker: Faker):
    return [faker.pystr() for _ in range(4)]


@pytest.fixture(autouse=True)
def active_validators(active_validators_keys: list[str]):
    objs = [Validator(public_key=key, active=True) for key in active_validators_keys]
    Validator.objects.bulk_create(objs)
    return objs


@pytest.fixture(autouse=True)
def inactive_validators(inactive_validators_keys: list[str]):
    objs = [Validator(public_key=key, active=False) for key in inactive_validators_keys]
    Validator.objects.bulk_create(objs)
    return objs


def _generate_validator_mocks(hotkeys: Iterable[str]):
    return [MagicMock(hotkey=key) for key in hotkeys]


def test_fetch_validators_creates_new_active_validators(
    faker: Faker, mock_get_validators: MagicMock
):
    new_keys = [faker.pystr() for _ in range(3)]
    mock_get_validators.return_value = _generate_validator_mocks(new_keys)

    fetch_validators()

    for key in new_keys:
        assert Validator.objects.filter(public_key=key, active=True).exists()


def test_fetch_validators_updates_existing_validators(
    inactive_validators_keys: list[str],
    active_validators: list[Validator],
    inactive_validators: list[Validator],
    mock_get_validators: MagicMock,
):
    mock_get_validators.return_value = _generate_validator_mocks(inactive_validators_keys)

    fetch_validators()

    assert Validator.objects.count() == len(active_validators) + len(inactive_validators)

    assert Validator.objects.filter(active=True).count() == len(inactive_validators)

    for key in inactive_validators_keys:
        assert Validator.objects.filter(public_key=key, active=True).exists()


def test_fetch_validators_debug(
    active_validators: list[Validator],
    active_validators_keys: list[str],
    inactive_validators: list[Validator],
    mock_get_validators: MagicMock,
    faker: Faker,
):
    mock_get_validators.return_value = _generate_validator_mocks(active_validators_keys)

    debug_validator_key = faker.pystr()
    Validator.objects.create(public_key=debug_validator_key, active=True, debug=True)

    fetch_validators()

    assert Validator.objects.count() == len(active_validators) + len(inactive_validators) + 1
    assert set(Validator.objects.filter(active=True).values_list("public_key", flat=True)) == set(
        active_validators_keys
    ) | {debug_validator_key}


def test_fetch_validators_debug_inactive(
    active_validators_keys: list[str],
    mock_get_validators: MagicMock,
    faker: Faker,
):
    mock_get_validators.return_value = _generate_validator_mocks(active_validators_keys)

    debug_validator_key = faker.pystr()
    debug_validator = Validator.objects.create(
        public_key=debug_validator_key, active=False, debug=True
    )

    fetch_validators()

    debug_validator.refresh_from_db()
    assert not debug_validator.active
