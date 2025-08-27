import logging

import pytest
from django.conf import settings
from django.core.cache import cache

logger = logging.getLogger(__name__)


@pytest.fixture(autouse=True)
def cache_block_number():
    cache.set(settings.COMPUTE_HORDE_BLOCK_CACHE_KEY, 1005)


@pytest.fixture(scope="function", autouse=True)
def clear_cache():
    yield

    cache.clear()
