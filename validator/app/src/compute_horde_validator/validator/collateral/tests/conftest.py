from __future__ import annotations

import os

import pytest

from compute_horde_validator.validator.collateral import default as collateral_default

os.environ.setdefault(
    "DJANGO_SETTINGS_MODULE",
    "compute_horde_validator.validator.tests.settings",
)


@pytest.fixture(autouse=True)  # type: ignore[misc]
def _reset_collateral_cache() -> None:
    collateral_default._cached_contract_address = None
