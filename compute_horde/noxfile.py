import os
import re
import subprocess

import nox

os.environ["PDM_IGNORE_SAVED_PYTHON"] = "1"

CI = os.environ.get("CI") is not None
PYTHON_VERSION = ["3.11"]
RELEASE_TAG_PREFIX = "library"

nox.options.default_venv_backend = "venv"
nox.options.stop_on_first_error = True
nox.options.reuse_existing_virtualenvs = not CI


def install(session: nox.Session, *args):
    groups = []
    for group in args:
        groups.extend(["--group", group])
    session.run("pdm", "install", "--check", *groups, external=True)


@nox.session(name="format", python=PYTHON_VERSION)
def format_(session: nox.Session):
    """Lint the code and apply fixes in-place whenever possible."""
    install(session, "format")
    session.run("ruff", "check", "--fix", ".")
    session.run("ruff", "format", ".")


@nox.session(python=PYTHON_VERSION)
def lint(session: nox.Session):
    """Run linters in readonly mode."""
    install(session, "lint")
    session.run("ruff", "check", "--diff", ".")
    session.run("codespell", ".")
    session.run("ruff", "format", ".")


@nox.session(python=PYTHON_VERSION)
def make_release_commit(session: nox.Session):
    """
    Runs `towncrier build`, commits changes, tags, all that is left to do is pushing
    """
    if session.posargs:
        version = session.posargs[0]
    else:
        session.error('Provide -- {release_version} (X.Y.Z - without leading "v")')

    if not re.match(r"^\d+\.\d+\.\d+(a\d+)?$", version):
        session.error(
            f'Provided version="{version}". Version must be of the form X.Y.Z where '
            f"X, Y and Z are integers"
        )

    local_changes = subprocess.check_output(["git", "diff", "--stat"])
    if local_changes:
        session.error("Uncommitted changes detected")

    current_branch = (
        subprocess.check_output(["git", "rev-parse", "--abbrev-ref", "HEAD"]).decode().strip()
    )
    if current_branch != "master":
        session.error(f"Release must happen from master branch (current branch: {current_branch})")

    install(session, "release")
    session.run("towncrier", "build", "--yes", "--version", version)

    session.log(
        f"CHANGELOG updated, changes ready to commit and push\n"
        f'    git commit -m "Release library version {version}"\n'
        f"    git tag {RELEASE_TAG_PREFIX}-v{version}\n"
        f"    git push origin master\n"
        f"    git push origin {RELEASE_TAG_PREFIX}-v{version}"
    )
