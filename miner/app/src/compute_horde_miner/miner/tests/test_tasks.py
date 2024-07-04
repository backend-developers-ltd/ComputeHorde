from collections.abc import Iterable
from unittest.mock import MagicMock

import pytest
from faker import Faker

from compute_horde_miner.miner.models import Validator
from compute_horde_miner.miner.tasks import fetch_validators

pytestmark = [pytest.mark.django_db]


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


def test_fetch_validators_debug_validator_key(
    active_validators: list[Validator],
    active_validators_keys: list[str],
    inactive_validators: list[Validator],
    mock_get_validators: MagicMock,
    faker: Faker,
    settings,
):
    mock_get_validators.return_value = _generate_validator_mocks(active_validators_keys)

    debug_validator_key = faker.pystr()
    settings.DEBUG_VALIDATOR_KEY = debug_validator_key

    fetch_validators()

    assert Validator.objects.count() == len(active_validators) + len(inactive_validators) + 1
    assert set(Validator.objects.filter(active=True).values_list("public_key", flat=True)) == set(
        active_validators_keys
    ) | {debug_validator_key}
