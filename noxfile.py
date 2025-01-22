from __future__ import annotations

import os
from pathlib import Path

import nox


CI = os.environ.get("CI") is not None

ROOT = Path(".")
PYTHON_VERSIONS = ["3.11"]
PYTHON_DEFAULT_VERSION = PYTHON_VERSIONS[-1]

nox.options.default_venv_backend = "uv"
nox.options.stop_on_first_error = True
nox.options.reuse_existing_virtualenvs = not CI


@nox.session(python=PYTHON_VERSIONS)
def test(session):
    uv_env = getattr(session.virtualenv, "location", os.getenv("VIRTUAL_ENV"))
    session.run_install(
        "uv",
        "sync",
        "--locked",
        "--group",
        "dev",
        env={"UV_PROJECT_ENVIRONMENT": uv_env},
    )
    session.run(
        "pytest",
        "-s",
        "-x",
        "-vv",
        "--junitxml",
        "test-report.xml",
        "tests",
        *session.posargs,
    )
