import os

import kombu

# Copied from settings.py to not setup Django in a healthcheck
REDIS_HOST = os.getenv("REDIS_HOST", default="redis")
REDIS_PORT = os.getenv("REDIS_PORT", default="6379")
CELERY_BROKER_URL = os.getenv("CELERY_BROKER_URL", default=f"redis://{REDIS_HOST}:{REDIS_PORT}/0")

with kombu.Connection(CELERY_BROKER_URL) as conn:
    conn.connect()
