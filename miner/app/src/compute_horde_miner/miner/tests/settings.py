import os

os.environ.update(
    {
        "DEBUG_TOOLBAR": "False",
    }
)

from compute_horde_miner.settings import *  # noqa: E402,F403

PROMETHEUS_EXPORT_MIGRATIONS = False


EXECUTOR_MANAGER_CLASS_PATH = "compute_horde_miner.miner.tests.executor_manager:StubExecutorManager"
DEBUG_TURN_AUTHENTICATION_OFF = True
