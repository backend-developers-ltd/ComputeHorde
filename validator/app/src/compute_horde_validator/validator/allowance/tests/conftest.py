from unittest import mock

import pytest
from compute_horde_validator.validator.allowance.utils import supertensor
from compute_horde_validator.validator.allowance import settings
from . import mockchain
from ...tests.helpers import patch_constance


@pytest.fixture(autouse=True)
def basic_mocks():
    def throw(*a, **kwargs):
        raise RuntimeError("Don't do that")

    with (
        mock.patch.object(supertensor.SuperTensor, '__init__', throw),
        mock.patch.object(settings, 'MANIFEST_FETCHING_TIMEOUT', mockchain.MANIFEST_FETCHING_TIMEOUT),
        mock.patch.object(settings, 'RESERVATION_MARGIN_SECONDS', 100),
        mock.patch.object(settings, 'BLOCK_LOOKBACK', 361 * 4),
        mock.patch.object(settings, 'BLOCK_EVICTION_THRESHOLD', int(361 * 4 * 1.5)),
        mock.patch.object(settings, 'BLOCK_EXPIRY', 722),
        mock.patch.object(settings, 'MAX_JOB_RUN_TIME', 60 * 60.0),
        patch_constance({
            "DYNAMIC_MINER_MAX_EXECUTORS_PER_CLASS": "always_on.llm.a6000=3,always_on.gpu-24gb=5,spin_up-4min.gpu-24gb=10"
        })
    ):
        yield
