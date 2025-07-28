from unittest import mock

import pytest
from compute_horde_validator.validator.allowance.utils import supertensor
from compute_horde_validator.validator.allowance import settings
from . import mockchain

@pytest.fixture(autouse=True)
def basic_mocks():
    def throw(*a, **kwargs):
        raise RuntimeError("Don't do that")
    with (
        mock.patch.object(supertensor.SuperTensor, '__init__', throw),
        mock.patch.object(settings, 'MANIFEST_FETCHING_TIMEOUT', mockchain.MANIFEST_FETCHING_TIMEOUT),
    ):
        yield
