import os

os.environ.update({
    "DEBUG_TOOLBAR": "False",
})

from compute_horde_miner.settings import *  # noqa: E402,F403

PROMETHEUS_EXPORT_MIGRATIONS = False
