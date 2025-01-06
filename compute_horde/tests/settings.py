from compute_horde.settings import *  # noqa

USE_TZ = True

DATABASES = {
    "default": {
        "ENGINE": "django.db.backends.sqlite3",
        "NAME": ":memory:",
    }
}

BITTENSOR_NETWORK = "local"

CELERY_TASK_ALWAYS_EAGER = True

COMPUTE_HORDE_BLOCK_CACHE_KEY = "test-block-cache-key"
