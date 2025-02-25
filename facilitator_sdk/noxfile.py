import os

import nox

CI = os.environ.get("CI") is not None
PYTHON_VERSION = ["3.11"]
RELEASE_TAG_PREFIX = "library"

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
    session.run("codespell", ".", "--skip='*.lock'")
    session.run("ruff", "format", "--diff", ".")


@nox.session(python=PYTHON_VERSION)
def type_check(session):
    install(session, "type_check")
    session.run("mypy", "--config-file", "mypy.ini", ".", *session.posargs)


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


@nox.session(python=PYTHON_VERSION)
def test(session):
    install(session, "test")
    session.run(
        "pytest",
        "-s",
        "-vv",
        "--junitxml",
        "test-report.xml",
        "tests",
        *session.posargs,
        env={"DJANGO_SETTINGS_MODULE": "tests.settings"},
    )
