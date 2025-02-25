import os

os.environ["DEBUG_TOOLBAR"] = "False"

from project.settings import *  # noqa: E402,F403

PROMETHEUS_EXPORT_MIGRATIONS = False
