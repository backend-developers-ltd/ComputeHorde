import subprocess
import uuid
from collections.abc import Generator

import pytest
from compute_horde_core.certificate import NGINX_IMAGE


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


@pytest.fixture(autouse=True, scope="session")
def pull_required_images():
    """
    Pulls docker images required by the tests to prevent job executor startup timeout.
    """
    for image_name in (
        "backenddevelopersltd/compute-horde-streaming-job-test:v0-latest",
        "backenddevelopersltd/compute-horde-job-echo:v0-latest",
        "python:3.11-slim",
        NGINX_IMAGE,
    ):
        subprocess.run(["docker", "pull", image_name])
