from __future__ import annotations

import argparse
import os
import re
import subprocess
from pathlib import Path

import nox

os.environ["PDM_IGNORE_SAVED_PYTHON"] = "1"

CI = os.environ.get("CI") is not None

ROOT = Path(".")
MAIN_BRANCH_NAME = "master"
PYTHON_VERSIONS = ["3.11", "3.12"]
PYTHON_VERSION = ["3.11"]
BITTENSOR_VERSION = os.environ.get("BITTENSOR_VERSION")

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
        "--all-extras",
        *groups,
        env={"UV_PROJECT_ENVIRONMENT": uv_env},
    )


@nox.session(name="format", python=PYTHON_VERSION)
def format_(session: nox.Session):
    """Lint the code and apply fixes in-place whenever possible."""
    install(session, "format")
    session.run("ruff", "format", ".")
    session.run("ruff", "check", "--fix", ".")


@nox.session(python=PYTHON_VERSION)
def lint(session: nox.Session):
    """Run linters in readonly mode."""
    install(session, "lint")
    session.run("ruff", "check", "--diff", ".")
    session.run("codespell", ".", "--skip='*.lock'")
    session.run("ruff", "format", "--diff", ".")


@nox.session(python=PYTHON_VERSION)
def type_check(session):
    install(session, "type_check")
    session.run("mypy", "src", *session.posargs)


@nox.session(python=PYTHON_VERSION)
def check_missing_migrations(session):
    install(session)
    session.run(
        "django-admin",
        "makemigrations",
        "--dry-run",
        "--check",
        env={"DJANGO_SETTINGS_MODULE": "compute_horde.settings"},
    )


@nox.session(python=PYTHON_VERSIONS, tags=["test", "check"])
def test(session):
    install(session, "test")
    session.run("pytest", "-s", "-vv", "tests", *session.posargs)


@nox.session(python=PYTHON_VERSION)
def make_release(session):
    install(session, "release")
    parser = argparse.ArgumentParser()

    def version(value):
        if not re.match(r"\d+\.\d+\.\d+(?:(?:a|b|rc)\d+)?", value):
            raise argparse.ArgumentTypeError("Invalid version format")
        return value

    parser.add_argument(
        "release_version",
        help="Release version in semver format (e.g. 1.2.3)",
        type=version,
    )
    parser.add_argument(
        "--draft",
        action="store_true",
        help="Create a draft release",
    )
    parsed_args = parser.parse_args(session.posargs)

    local_changes = subprocess.check_output(["git", "diff", "--stat"])
    if local_changes:
        session.error("Uncommitted changes detected")

    current_branch = subprocess.check_output(["git", "rev-parse", "--abbrev-ref", "HEAD"], text=True).strip()
    if current_branch != MAIN_BRANCH_NAME:
        session.warn(f"Releasing from a branch {current_branch!r}, while main branch is {MAIN_BRANCH_NAME!r}")
        if not parsed_args.draft:
            session.error("Only draft releases are allowed from non-main branch")

    session.run("towncrier", "build", "--yes", "--version", parsed_args.release_version)

    if parsed_args.draft:
        tag = f"draft/v{parsed_args.release_version}"
        message = f"Draft release {tag}"
    else:
        tag = f"v{parsed_args.release_version}"
        message = f"release {tag}"

    session.log(
        f"CHANGELOG updated, please review changes, and execute when ready:\n"
        f"    git commit -m {message!r}\n"
        f"    git push origin {current_branch}\n"
        f"    git tag {tag}\n"
        f"    git push origin {tag}\n"
    )


@nox.session(python=PYTHON_VERSION)
def docs(session):
    install(session, "docs")
    source_dir = "docs/source"
    output_dir = "docs/build/html"
    session.run("rm", "-rf", output_dir, external=True)
    session.run("sphinx-multiversion", source_dir, output_dir, *session.posargs)
    session.run(
        "cp", "-f", f"{source_dir}/_templates/gh-pages-redirect.html", f"{output_dir}/index.html", external=True
    )
