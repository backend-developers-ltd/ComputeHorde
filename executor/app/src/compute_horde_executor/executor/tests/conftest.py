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


@pytest.fixture(autouse=True, scope="session")
def print_all_child_subprocesses():
    yield

    import logging

    import psutil

    current_process = psutil.Process()
    children = current_process.children(recursive=True)
    for child in children:
        logging.error(f"Subprocess still running: {child=} {child.cmdline()=}")
