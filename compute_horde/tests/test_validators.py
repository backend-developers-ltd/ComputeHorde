from collections.abc import Iterable
from unittest.mock import MagicMock, patch

import pytest
from faker import Faker

from compute_horde.validators.tasks import fetch_validators
from tests.test_app.models import Validator


@pytest.fixture(autouse=True)
def mock_get_validators():
    with patch("compute_horde.validators.tasks.get_validators") as p:
        yield p


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


@pytest.mark.django_db(transaction=True)
def test_fetch_validators_creates_new_active_validators(
    faker: Faker, mock_get_validators: MagicMock
):
    new_keys = [faker.pystr() for _ in range(3)]
    mock_get_validators.return_value = _generate_validator_mocks(new_keys)

    fetch_validators()

    for key in new_keys:
        assert Validator.objects.filter(public_key=key, active=True).exists()


@pytest.mark.django_db(transaction=True)
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


@pytest.mark.django_db(transaction=True)
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


@pytest.mark.django_db(transaction=True)
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
