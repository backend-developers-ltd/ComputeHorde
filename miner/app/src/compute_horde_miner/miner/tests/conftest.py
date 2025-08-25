import logging
from collections.abc import Generator

import pytest

logger = logging.getLogger(__name__)


@pytest.fixture
def some() -> Generator[int, None, None]:
    # setup code
    yield 1
    # teardown code
