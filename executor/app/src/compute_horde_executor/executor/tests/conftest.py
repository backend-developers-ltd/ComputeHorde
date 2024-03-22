import uuid
from collections.abc import Generator

import pytest


@pytest.fixture
def some() -> Generator[int, None, None]:
    # setup code
    yield 1
    # teardown code


@pytest.fixture(autouse=True)
def unique_settings(settings):
    settings.EXECUTOR_TOKEN = str(uuid.uuid4())
