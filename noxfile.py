from __future__ import annotations

import os
from pathlib import Path

import nox

CI = os.environ.get('CI') is not None

ROOT = Path('.')
PYTHON_VERSIONS = ['3.12']
PYTHON_DEFAULT_VERSION = PYTHON_VERSIONS[-1]

nox.options.default_venv_backend = 'venv'
nox.options.stop_on_first_error = True
nox.options.reuse_existing_virtualenvs = True

# In CI, use Python interpreter provided by GitHub Actions
if CI:
    nox.options.force_venv_backend = 'none'


@nox.session(python=PYTHON_VERSIONS)
def test(session):
    session.run('pip', 'install', 'pytest', 'pytest-asyncio', 'pytest-xdist', 'requests')
    for module in ['miner', 'executor', 'validator']:
        with session.chdir(str(ROOT / module / 'app' / 'src')):
            session.run('pip', 'install', '-r', 'requirements.txt')
    session.run(
        'pytest',
        '-W', 'ignore::DeprecationWarning', '-s', '-x', '-vv',
        '-n', 'auto',
        'tests',
        *session.posargs,
    )
