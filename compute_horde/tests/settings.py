from compute_horde.settings import *

INSTALLED_APPS += [
    "tests.test_app",
]

CELERY_TASK_ALWAYS_EAGER = True

COMPUTE_HORDE_BLOCK_CACHE_KEY = "test-block-cache-key"

USE_TZ = True

DATABASES = {
    "default": {"ENGINE": "django.db.backends.sqlite3"},
}

COMPUTE_HORDE_VALIDATOR_MODEL = "test_app.Validator"
COMPUTE_HORDE_VALIDATOR_KEY_FIELD = "public_key"
COMPUTE_HORDE_VALIDATOR_ACTIVE_FIELD = "active"
COMPUTE_HORDE_VALIDATOR_DEBUG_FIELD = "debug"

BITTENSOR_NETUID = 49
BITTENSOR_NETWORK = "local"
