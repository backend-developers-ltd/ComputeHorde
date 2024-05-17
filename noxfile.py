from __future__ import annotations

import os
from pathlib import Path

import nox


os.environ["PDM_IGNORE_SAVED_PYTHON"] = "1"

CI = os.environ.get('CI') is not None

ROOT = Path('.')
PYTHON_VERSIONS = ['3.11']
PYTHON_DEFAULT_VERSION = PYTHON_VERSIONS[-1]

nox.options.default_venv_backend = 'venv'
nox.options.stop_on_first_error = True
nox.options.reuse_existing_virtualenvs = not CI


@nox.session(python=PYTHON_VERSIONS)
def test(session):
    session.run('pdm', 'install', '--check', external=True)
    session.run(
        'pytest',
        '-W', 'ignore::DeprecationWarning', '-s', '-x', '-vv',
        '-n', 'auto',
        'tests',
        *session.posargs,
    )
