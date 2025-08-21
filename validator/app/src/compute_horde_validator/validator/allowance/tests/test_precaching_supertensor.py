import contextlib
import contextvars
import functools
import os
import time
from unittest import mock

import pytest
from compute_horde.blockchain.block_cache import _BLOCK_CACHE_KEY
from django.core.cache import cache

from ..utils import supertensor
from ..utils.supertensor_django_cache import DjangoCache
from . import mockchain

block_number_context: contextvars.ContextVar[int] = contextvars.ContextVar("block_number")


@contextlib.contextmanager
def prepare_mocks():
    class BittensorMock:
        def __init__(self, *args, **kwargs):
            pass

        def subnet(self, *args, **kwargs):
            return self

        async def list_neurons(self):
            return mockchain.list_neurons(block_number_context.get(), with_shield=False)

        @contextlib.asynccontextmanager
        async def block(self, block_number):
            block_number_context.set(block_number)
            block_context = mock.MagicMock()

            async def get_timestamp():
                return mockchain.get_block_timestamp(block_number)

            block_context.get_timestamp = get_timestamp

            yield block_context

    with (
        mock.patch("turbobt.Bittensor", new=BittensorMock),
        mock.patch.object(
            supertensor.SuperTensor,
            "__init__",
            supertensor.SuperTensor.__orig__init__,  # type: ignore
        ),
    ):
        yield


def keep_trying_until_success_or_timeout(func, timeout=1):
    start_time = time.time()
    while True:
        try:
            return func()
        except supertensor.PrecachingSuperTensorCacheMiss:
            if time.time() - start_time > timeout:
                raise
            time.sleep(0.1)


@pytest.mark.skipif(os.getenv("GITHUB_ACTIONS") == "true", reason="Skip in GitHub CI")
def test_precaching_supertensor_smoke_test():
    cache.set(_BLOCK_CACHE_KEY, 1100, 60)
    with prepare_mocks():
        precaching_supertensor = supertensor.PrecachingSuperTensor(
            cache=DjangoCache(), throw_on_cache_miss=True
        )
        with pytest.raises(supertensor.PrecachingSuperTensorCacheMiss):
            precaching_supertensor.list_neurons(1001)
        keep_trying_until_success_or_timeout(
            functools.partial(precaching_supertensor.get_block_timestamp, 1001)
        )
        precaching_supertensor.list_neurons(1001)

        with pytest.raises(supertensor.PrecachingSuperTensorCacheMiss):
            precaching_supertensor.get_block_timestamp(1012)

        for i in range(1012, 1030):
            keep_trying_until_success_or_timeout(
                functools.partial(precaching_supertensor.list_neurons, i)
            )
            precaching_supertensor.get_block_timestamp(i)

        time.sleep(1)
