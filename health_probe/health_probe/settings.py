import logging

import environ

root = environ.Path(__file__) - 1

env = environ.Env()

# read from the .env file if hasn't been sourced already
if env("ENV", default=None) is None:
    env.read_env(root("../.env"))

ENV = env("ENV", default="prod")

LOGGING_LEVEL = env.str("LOGGING_LEVEL", default="INFO")

BITTENSOR_WALLET_NAME = env.str("BITTENSOR_WALLET_NAME")
BITTENSOR_WALLET_HOTKEY_NAME = env.str("BITTENSOR_WALLET_HOTKEY_NAME")
BITTENSOR_WALLET_PATH = env.str("BITTENSOR_WALLET_PATH")
FACILITATOR_URL = env.str("FACILITATOR_URL")
VALIDATOR_HOTKEY = env.str("VALIDATOR_HOTKEY")

# Which job from jobs.py to run.
JOB_SPEC_NAME = env.str("JOB_SPEC_NAME", "huggingface")
JOB_TIMEOUT = env.int("JOB_TIMEOUT", default=60)

PROBING_INTERVAL_HEALTHY = env.float("PROBING_INTERVAL_HEALTHY", default=60 * 5)
PROBING_INTERVAL_BUSY = env.float("PROBING_INTERVAL_BUSY", default=60 * 2)
PROBING_INTERVAL_UNHEALTHY = env.float("PROBING_INTERVAL_UNHEALTHY", default=60 * 2)

METRICS_SERVER_PORT = env.int("METRICS_SERVER_PORT", default=80)
METRICS_SERVER_CERTFILE = env.str("METRICS_SERVER_CERTFILE", default=None)
METRICS_SERVER_KEYFILE = env.str("METRICS_SERVER_KEYFILE", default=None)

# Sentry
if SENTRY_DSN := env("SENTRY_DSN", default=""):
    import sentry_sdk
    from sentry_sdk.integrations.logging import LoggingIntegration

    sentry_sdk.init(
        dsn=SENTRY_DSN,
        environment=ENV,
        integrations=[
            LoggingIntegration(
                level=logging.INFO,  # Capture info and above as breadcrumbs
                event_level=logging.ERROR,  # Send error events from log messages
            ),
        ],
    )