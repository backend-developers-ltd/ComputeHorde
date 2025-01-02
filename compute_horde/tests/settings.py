from compute_horde.settings import *

INSTALLED_APPS += [
    "tests.test_app",
]

CELERY_TASK_ALWAYS_EAGER = True

USE_TZ = True

DATABASES = {
    "default": {"ENGINE": "django.db.backends.sqlite3"},
}

VALIDATOR_MODEL = "test_app.Validator"
VALIDATOR_KEY_FIELD = "public_key"
VALIDATOR_ACTIVE_FIELD = "active"
VALIDATOR_DEBUG_FIELD = "debug"

BITTENSOR_NETUID = 49
BITTENSOR_NETWORK = "local"
