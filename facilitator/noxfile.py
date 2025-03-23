from __future__ import annotations

import functools
import os
import subprocess
from pathlib import Path

import nox

CI = os.environ.get("CI") is not None

ROOT = Path(".")
PYTHON_VERSIONS = ["3.11"]
PYTHON_DEFAULT_VERSION = PYTHON_VERSIONS[-1]
APP_ROOT = ROOT / "app" / "src"

nox.options.default_venv_backend = "uv"
nox.options.stop_on_first_error = True
nox.options.reuse_existing_virtualenvs = not CI


def install(session: nox.Session, *args):
    groups = []
    for group in args:
        groups.extend(["--group", group])

    uv_env = getattr(session.virtualenv, "location", os.getenv("VIRTUAL_ENV"))
    session.run_install(
        "uv",
        "sync",
        "--locked",
        *groups,
        env={"UV_PROJECT_ENVIRONMENT": uv_env},
    )


@functools.lru_cache
def _list_files() -> list[Path]:
    file_list = []
    for cmd in (
        ["git", "ls-files"],
        ["git", "ls-files", "--others", "--exclude-standard"],
    ):
        cmd_result = subprocess.run(cmd, check=True, text=True, capture_output=True)
        file_list.extend(cmd_result.stdout.splitlines())
    file_paths = [Path(p) for p in file_list]
    return file_paths


def list_files(suffix: str | None = None) -> list[Path]:
    """List all non-files not-ignored by git."""
    file_paths = _list_files()
    if suffix is not None:
        file_paths = [p for p in file_paths if p.suffix == suffix]
    return file_paths


@nox.session(name="format", python=PYTHON_DEFAULT_VERSION)
def format_(session):
    """Lint the code and apply fixes in-place whenever possible."""
    install(session, "format")
    session.run("ruff", "check", "--fix", ".")
    session.run("ruff", "format", ".")


@nox.session(python=PYTHON_DEFAULT_VERSION)
def lint(session):
    """Run linters in readonly mode."""
    install(session, "lint")
    session.run("ruff", "check", "--diff", ".")
    # session.run("codespell", ".", "--skip='*.lock'")
    session.run("ruff", "format", "--diff", ".")


@nox.session(python=PYTHON_DEFAULT_VERSION)
def type_check(session):
    install(session, "type_check")
    with session.chdir(str(APP_ROOT)):
        session.run("mypy", "--config-file", "mypy.ini", ".", *session.posargs)


@nox.session(python=PYTHON_DEFAULT_VERSION)
def security_check(session):
    install(session, "security_check")
    with session.chdir(str(APP_ROOT)):
        session.run("bandit", "--ini", "bandit.ini", "-r", ".", *session.posargs)


@nox.session(python=PYTHON_VERSIONS)
def test(session):
    install(session, "test")
    with session.chdir(str(APP_ROOT)):
        session.run(
            "pytest",
            "-W",
            "ignore::DeprecationWarning",
            "-s",
            # "-x",
            "-vv",
            # "-n",
            # "auto",
            "project",
            *session.posargs,
        )
