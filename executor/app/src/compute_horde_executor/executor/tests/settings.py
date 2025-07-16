import os

os.environ.update(
    {
        "DEBUG_TOOLBAR": "False",
        "DEBUG_NO_GPU_MODE": "true",
    }
)

from compute_horde_executor.settings import *  # noqa: E402,F403

PROMETHEUS_EXPORT_MIGRATIONS = False
