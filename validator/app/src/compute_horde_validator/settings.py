"""
Django settings for compute_horde_validator project.
"""

import inspect
import logging
import pathlib
from datetime import timedelta
from functools import wraps

import bittensor
import environ
from celery.schedules import crontab
from compute_horde import base  # noqa

root = environ.Path(__file__) - 2

env = environ.Env(DEBUG=(bool, False))

# .env file contents are not passed to docker image during build stage;
# this results in errors if you require some env var to be set, as if in "env('MYVAR')" -
# obviously it's not set during build stage, but you don't care and want to ignore that.
# To mitigate this, we set ENV_FILL_MISSING_VALUES=1 during build phase, and it activates
# monkey-patching of "environ" module, so that all unset variables are set to None and
# the library is not complaining anymore
if env.bool("ENV_FILL_MISSING_VALUES", default=False):

    def patch(fn):
        @wraps(fn)
        def wrapped(*args, **kwargs):
            if kwargs.get("default") is env.NOTSET:
                kwargs["default"] = None
            return fn(*args, **kwargs)

        return wrapped

    for name, method in inspect.getmembers(env, predicate=inspect.ismethod):
        setattr(env, name, patch(method))

# read from the .env file if hasn't been sourced already
if env("ENV", default=None) is None:
    env.read_env(root("../../.env"))

ENV = env("ENV", default="prod")


DEFAULT_ADMIN_PASSWORD = env("DEFAULT_ADMIN_PASSWORD", default=None)
DEFAULT_ADMIN_USERNAME = env("DEFAULT_ADMIN_USERNAME", default="admin")
DEFAULT_ADMIN_EMAIL = env("DEFAULT_ADMIN_EMAIL", default="admin@admin.com")

# SECURITY WARNING: keep the secret key used in production secret!
SECRET_KEY = env("SECRET_KEY")

# SECURITY WARNING: don't run with debug turned on in production!
DEBUG = env.bool("DEBUG", default=False)

ALLOWED_HOSTS = ["*"]

INSTALLED_APPS = [
    #  'django_prometheus',
    # 'django.contrib.admin',
    "django.contrib.auth",
    "django.contrib.contenttypes",
    "django.contrib.sessions",
    "django.contrib.messages",
    "django.contrib.staticfiles",
    "django_extensions",
    "django_probes",
    "constance",
    "compute_horde_validator.validator",
    "compute_horde_validator.validator.admin_config.ValidatorAdminConfig",
    "rangefilter",
]
PROMETHEUS_EXPORT_MIGRATIONS = True
PROMETHEUS_LATENCY_BUCKETS = (
    0.008,
    0.016,
    0.032,
    0.062,
    0.125,
    0.25,
    0.5,
    1.0,
    2.0,
    4.0,
    8.0,
    16.0,
    32.0,
    64.0,
    float("inf"),
)


MIDDLEWARE = [
    #  'django_prometheus.middleware.PrometheusBeforeMiddleware',
    "django.middleware.security.SecurityMiddleware",
    "django.contrib.sessions.middleware.SessionMiddleware",
    "django.middleware.common.CommonMiddleware",
    "django.middleware.csrf.CsrfViewMiddleware",
    "django.contrib.auth.middleware.AuthenticationMiddleware",
    "django.contrib.messages.middleware.MessageMiddleware",
    "django.middleware.clickjacking.XFrameOptionsMiddleware",
    #  'django_prometheus.middleware.PrometheusAfterMiddleware',
]


if DEBUG_TOOLBAR := env.bool("DEBUG_TOOLBAR", default=False):
    INTERNAL_IPS = [
        "127.0.0.1",
    ]

    DEBUG_TOOLBAR_CONFIG = {"SHOW_TOOLBAR_CALLBACK": lambda _request: True}
    INSTALLED_APPS.append("debug_toolbar")
    MIDDLEWARE = ["debug_toolbar.middleware.DebugToolbarMiddleware"] + MIDDLEWARE

if CORS_ENABLED := env.bool("CORS_ENABLED", default=True):
    INSTALLED_APPS.append("corsheaders")
    MIDDLEWARE = ["corsheaders.middleware.CorsMiddleware"] + MIDDLEWARE
    CORS_ALLOWED_ORIGINS = env.list("CORS_ALLOWED_ORIGINS", default=[])
    CORS_ALLOWED_ORIGIN_REGEXES = env.list("CORS_ALLOWED_ORIGIN_REGEXES", default=[])
    CORS_ALLOW_ALL_ORIGINS = env.bool("CORS_ALLOW_ALL_ORIGINS", default=False)

SECURE_PROXY_SSL_HEADER = ("HTTP_X_FORWARDED_PROTO", "https")

BITTENSOR_APPROXIMATE_BLOCK_DURATION = timedelta(seconds=12)

CONSTANCE_BACKEND = "constance.backends.database.DatabaseBackend"
CONSTANCE_CONFIG = {
    "SERVING": (
        not env.bool("MIGRATING", default=False),
        "Whether this validator is serving jobs and setting weights",
        bool,
    ),
    "DYNAMIC_MANIFEST_SCORE_MULTIPLIER": (
        1.05,
        "The bonus rate for miners changing their horde size",
        float,
    ),
    "DYNAMIC_MANIFEST_DANCE_RATIO_THRESHOLD": (
        1.4,
        "The ratio at which the horde size needs to be changed for receiving bonus",
        float,
    ),
    "DYNAMIC_WEIGHTS_VERSION": (1, "The weights version for synthetic jobs", int),
    "DYNAMIC_SYNTHETIC_JOBS_FLOW_VERSION": (
        1,
        "The synthetic jobs flow version",
        int,
    ),
    "DYNAMIC_SYNTHETIC_JOBS_PLANNER_WAIT_IN_ADVANCE_BLOCKS": (
        3,
        "How many blocks in advance to start waiting before synthetic jobs spawn",
        int,
    ),
    "DYNAMIC_SYNTHETIC_JOBS_PLANNER_POLL_INTERVAL": (
        (BITTENSOR_APPROXIMATE_BLOCK_DURATION / 3).total_seconds(),
        "How often (in seconds) to poll for block change",
        float,
    ),
    "DYNAMIC_BLOCK_FINALIZATION_NUMBER": (
        3,
        "After this many blocks pass, a block can be considered final",
        int,
    ),
    "DYNAMIC_COMMIT_REVEAL_WEIGHTS_ENABLED": (
        True,
        "This should be synced with the hyperparam",
        bool,
    ),
    "DYNAMIC_COMMIT_REVEAL_WEIGHTS_INTERVAL": (
        370,
        "In blocks. This should be synced with the hyperparam",
        int,
    ),
    "DYNAMIC_MAX_WEIGHT": (
        65535,
        "This should be synced with the hyperparam",
        int,
    ),
    "DYNAMIC_SYNTHETIC_JOBS_PLANNER_MAX_OVERSLEEP_BLOCKS": (
        3,
        "If the job running task wakes up late by this many blocks (or less), the jobs will still run",
        int,
    ),
    "DYNAMIC_WEIGHT_REVEALING_TTL": (
        120,
        "in seconds",
        int,
    ),
    "DYNAMIC_WEIGHT_REVEALING_HARD_TTL": (
        125,
        "in seconds",
        int,
    ),
    "DYNAMIC_WEIGHT_REVEALING_ATTEMPTS": (
        50,
        "the number of attempts",
        int,
    ),
    "DYNAMIC_WEIGHT_REVEALING_FAILURE_BACKOFF": (
        5,
        "in seconds",
        int,
    ),
    "DYNAMIC_NUMBER_OF_PROMPTS_TO_VALIDATE_FROM_SERIES": (
        10,
        "how many prompts to sample and validate from a series",
        int,
    ),
    "DYNAMIC_NUMBER_OF_WORKLOADS_TO_TRIGGER_LOCAL_INFERENCE": (
        100,
        "how many workloads are needed before running local inference",
        int,
    ),
    "DYNAMIC_MAX_PROMPT_BATCHES": (
        10000,
        "Maximum number of prompt batches upon which the prompt generator will not be triggered",
        int,
    ),
    "DYNAMIC_PROMPTS_BATCHES_IN_A_SINGLE_GO": (
        5,
        "Number of batches that prompt generator will process in a single go",
        int,
    ),
    "DYNAMIC_NUMBER_OF_PROMPTS_IN_BATCH": (
        240,
        "Number of prompts to generate in a single batch",
        int,
    ),
}
DYNAMIC_CONFIG_CACHE_TIMEOUT = 300

# Content Security Policy
if CSP_ENABLED := env.bool("CSP_ENABLED", default=False):
    MIDDLEWARE.append("csp.middleware.CSPMiddleware")

    CSP_REPORT_ONLY = env.bool("CSP_REPORT_ONLY", default=True)
    CSP_REPORT_URL = env("CSP_REPORT_URL", default=None) or None

    CSP_DEFAULT_SRC = env.tuple("CSP_DEFAULT_SRC", default=("'none'",))
    CSP_SCRIPT_SRC = env.tuple("CSP_SCRIPT_SRC", default=("'self'",))
    CSP_STYLE_SRC = env.tuple("CSP_STYLE_SRC", default=("'self'",))
    CSP_FONT_SRC = env.tuple("CSP_FONT_SRC", default=("'self'",))
    CSP_IMG_SRC = env.tuple("CSP_IMG_SRC", default=("'self'",))
    CSP_MEDIA_SRC = env.tuple("CSP_MEDIA_SRC", default=("'self'",))
    CSP_OBJECT_SRC = env.tuple("CSP_OBJECT_SRC", default=("'self'",))
    CSP_FRAME_SRC = env.tuple("CSP_FRAME_SRC", default=("'self'",))
    CSP_CONNECT_SRC = env.tuple("CSP_CONNECT_SRC", default=("'self'",))
    CSP_CHILD_SRC = env.tuple("CSP_CHILD_SRC", default=("'self'",))
    CSP_MANIFEST_SRC = env.tuple("CSP_MANIFEST_SRC", default=("'self'",))
    CSP_WORKER_SRC = env.tuple("CSP_WORKER_SRC", default=("'self'",))

    CSP_BLOCK_ALL_MIXED_CONTENT = env.bool("CSP_BLOCK_ALL_MIXED_CONTENT", default=False)
    CSP_EXCLUDE_URL_PREFIXES = env.tuple("CSP_EXCLUDE_URL_PREFIXES", default=tuple())


ROOT_URLCONF = "compute_horde_validator.urls"

TEMPLATES = [
    {
        "BACKEND": "django.template.backends.django.DjangoTemplates",
        "DIRS": [root("compute_horde_validator/templates")],
        "APP_DIRS": True,
        "OPTIONS": {
            "context_processors": [
                "django.template.context_processors.debug",
                "django.template.context_processors.request",
                "django.contrib.auth.context_processors.auth",
                "django.contrib.messages.context_processors.messages",
            ],
        },
    },
]

WSGI_APPLICATION = "compute_horde_validator.wsgi.application"

DATABASES = {}
default_db = f'postgres://postgres:{env("POSTGRES_PASSWORD")}@db:5432/postgres'
if env(
    "DATABASE_POOL_URL", default=""
):  # DB transaction-based connection pool, such as one provided PgBouncer
    DATABASES["default"] = {
        **env.db_url("DATABASE_POOL_URL"),
        "DISABLE_SERVER_SIDE_CURSORS": True,  # prevents random cursor errors with transaction-based connection pool
    }
elif env("DATABASE_URL", default=default_db):
    DATABASES["default"] = env.db_url("DATABASE_URL", default=default_db)

if "default" in DATABASES:
    DATABASES["default"]["NAME"] += env.str("DATABASE_SUFFIX", default="")

DEFAULT_AUTO_FIELD = "django.db.models.BigAutoField"

DEFAULT_DB_ALIAS = (
    "default_alias"  # useful for bypassing transaction while connecting to the same db
)
DATABASES[DEFAULT_DB_ALIAS] = DATABASES["default"]


if new_name := env.str("DEBUG_OVERRIDE_DATABASE_NAME", default=None):
    DATABASES["default"]["NAME"] = new_name
    DATABASES[DEFAULT_DB_ALIAS]["NAME"] = new_name


AUTH_PASSWORD_VALIDATORS = [
    {
        "NAME": "django.contrib.auth.password_validation.UserAttributeSimilarityValidator",
    },
    {
        "NAME": "django.contrib.auth.password_validation.MinimumLengthValidator",
    },
    {
        "NAME": "django.contrib.auth.password_validation.CommonPasswordValidator",
    },
    {
        "NAME": "django.contrib.auth.password_validation.NumericPasswordValidator",
    },
]

# Internationalization
LANGUAGE_CODE = "en-us"
TIME_ZONE = "UTC"
USE_I18N = True
USE_L10N = True
USE_TZ = True

# Static files (CSS, JavaScript, Images)
STATIC_URL = env("STATIC_URL", default="/static/")
STATIC_ROOT = env("STATIC_ROOT", default=root("static"))
MEDIA_URL = env("MEDIA_URL", default="/media/")
MEDIA_ROOT = env("MEDIA_ROOT", default=root("media"))

# Security
# redirect HTTP to HTTPS
if env.bool("HTTPS_REDIRECT", default=False) and not DEBUG:
    SECURE_SSL_REDIRECT = True
    SECURE_REDIRECT_EXEMPT = []  # type: ignore
    SESSION_COOKIE_SECURE = True
    CSRF_COOKIE_SECURE = True
else:
    SECURE_SSL_REDIRECT = False

CELERY_BROKER_URL = env("CELERY_BROKER_URL", default="redis://redis:6379/0")
CELERY_RESULT_BACKEND = env(
    "CELERY_BROKER_URL", default="redis://redis:6379/0"
)  # store results in Redis
CELERY_RESULT_EXPIRES = int(timedelta(days=1).total_seconds())  # time until task result deletion
CELERY_COMPRESSION = "gzip"  # task compression
CELERY_MESSAGE_COMPRESSION = "gzip"  # result compression
CELERY_SEND_EVENTS = True  # needed for worker monitoring
CELERY_BEAT_SCHEDULE = {  # type: ignore
    "schedule_synthetic_jobs": {
        "task": "compute_horde_validator.validator.tasks.schedule_synthetic_jobs",
        "schedule": timedelta(minutes=1),
        "options": {},
    },
    "run_synthetic_jobs": {
        "task": "compute_horde_validator.validator.tasks.run_synthetic_jobs",
        "schedule": timedelta(seconds=30),
        "options": {},
    },
    "check_missed_synthetic_jobs": {
        "task": "compute_horde_validator.validator.tasks.check_missed_synthetic_jobs",
        "schedule": timedelta(minutes=10),
        "options": {},
    },
    "set_scores": {
        "task": "compute_horde_validator.validator.tasks.set_scores",
        "schedule": crontab(
            minute=env("DEBUG_SET_SCORES_MINUTE", default="*/1"),
            hour=env("DEBUG_SET_SCORES_HOUR", default="*"),
        ),
        "options": {},
    },
    # TODO: high CPU usage may impact synthetic jobs - we should profile it and make it less CPU heavy
    # "fetch_receipts": {
    #     "task": "compute_horde_validator.validator.tasks.fetch_receipts",
    #     "schedule": crontab(minute="15,45"),  # try to stay away from set_scores task :)
    #     "options": {},
    # },
    "reveal_scores": {
        "task": "compute_horde_validator.validator.tasks.reveal_scores",
        "schedule": timedelta(minutes=1),
        "options": {},
    },
    "send_events_to_facilitator": {
        "task": "compute_horde_validator.validator.tasks.send_events_to_facilitator",
        "schedule": timedelta(minutes=5),
        "options": {},
    },
    "fetch_dynamic_config": {
        "task": "compute_horde_validator.validator.tasks.fetch_dynamic_config",
        "schedule": timedelta(minutes=5),
        "options": {},
    },
}
if env.bool("DEBUG_RUN_BEAT_VERY_OFTEN", default=False):
    CELERY_BEAT_SCHEDULE["run_synthetic_jobs"]["schedule"] = crontab(minute="*")
    CELERY_BEAT_SCHEDULE["set_scores"]["schedule"] = crontab(minute="*/3")
    CELERY_BEAT_SCHEDULE["fetch_receipts"]["schedule"] = crontab(minute="*/3")

CELERY_TASK_ROUTES = ["compute_horde_validator.celery.route_task"]
CELERY_TASK_TIME_LIMIT = int(timedelta(hours=2, minutes=5).total_seconds())
CELERY_TASK_ALWAYS_EAGER = env.bool("CELERY_TASK_ALWAYS_EAGER", default=False)
CELERY_ACCEPT_CONTENT = ["json"]
CELERY_TASK_SERIALIZER = "json"
CELERY_RESULT_SERIALIZER = "json"
CELERY_WORKER_PREFETCH_MULTIPLIER = env.int("CELERY_WORKER_PREFETCH_MULTIPLIER", default=10)
CELERY_BROKER_POOL_LIMIT = env.int("CELERY_BROKER_POOL_LIMIT", default=50)

EMAIL_BACKEND = env("EMAIL_BACKEND", default="django.core.mail.backends.filebased.EmailBackend")
EMAIL_FILE_PATH = env("EMAIL_FILE_PATH", default="/tmp/email")
EMAIL_HOST = env("EMAIL_HOST", default="smtp.sendgrid.net")
EMAIL_PORT = env.int("EMAIL_PORT", default=587)
EMAIL_HOST_USER = env("EMAIL_HOST_USER", default="apikey")
EMAIL_HOST_PASSWORD = env("EMAIL_HOST_PASSWORD", default="")
EMAIL_USE_TLS = env.bool("EMAIL_USE_TLS", default=True)
DEFAULT_FROM_EMAIL = env("DEFAULT_FROM_EMAIL", default="mail@localhost")

LOGGING = {
    "version": 1,
    "disable_existing_loggers": False,
    "formatters": {
        "main": {
            "format": "{levelname} {asctime} {name} {message}",
            "style": "{",
        },
    },
    "handlers": {
        "console": {
            "class": "logging.StreamHandler",
            "formatter": "main",
        },
    },
    "root": {
        "handlers": ["console"],
        "level": "DEBUG",
    },
    "loggers": {
        "django": {
            "handlers": ["console"],
            "level": "INFO",
            "propagate": True,
        },
        "websockets": {
            "handlers": ["console"],
            "level": "INFO",
            "propagate": True,
        },
    },
}

BITTENSOR_NETUID = env.int("BITTENSOR_NETUID")
BITTENSOR_NETWORK = env.str("BITTENSOR_NETWORK")

BITTENSOR_WALLET_DIRECTORY = env.path(
    "BITTENSOR_WALLET_DIRECTORY",
    default=pathlib.Path("~").expanduser() / ".bittensor" / "wallets",
)
BITTENSOR_WALLET_NAME = env.str("BITTENSOR_WALLET_NAME")
BITTENSOR_WALLET_HOTKEY_NAME = env.str("BITTENSOR_WALLET_HOTKEY_NAME")
SYNTHETIC_JOB_GENERATOR_FACTORY = env.str(
    "SYNTHETIC_JOB_GENERATOR_FACTORY",
    default="compute_horde_validator.validator.synthetic_jobs.generator.factory:DefaultSyntheticJobGeneratorFactory",
)
FACILITATOR_URI = env.str("FACILITATOR_URI", default="wss://facilitator.computehorde.io/ws/v0/")
STATS_COLLECTOR_URL = env.str(
    "STATS_COLLECTOR_URL", default="https://facilitator.computehorde.io/stats_collector/v0/"
)
# if you need to hit a particular miner, without fetching their key, address or port from the blockchain
DEBUG_MINER_KEY = env.str("DEBUG_MINER_KEY", default="")
DEBUG_MINER_ADDRESS = env.str("DEBUG_MINER_ADDRESS", default="")
DEBUG_MINER_PORT = env.int("DEBUG_MINER_PORT", default=0)
# how many distinct miners exists on ports starting at DEBUG_MINER_PORT
DEBUG_MINER_COUNT = env.int("DEBUG_MINER_COUNT", default=1)
# if you don't want to wait for your celery beat job to sleep on staging:
DEBUG_DONT_STAGGER_VALIDATORS = env.bool("DEBUG_DONT_STAGGER_VALIDATORS", default=False)

HORDE_SCORE_AVG_PARAM = 0
HORDE_SCORE_SIZE_PARAM = 0
# horde size for 0.5 value of sigmoid - sigmoid is evaluated for `-(1 / horde_size)`
# so setting this value to 4 would cause sigmoid for horde size of 4 return 0.5
# depending on a sigmoid steepnes it defines how important it is to having larger horde or how
# much penalized it is having smaller hordes; in extreme example step can be discrete and
# having smaller horde will result with 0 weights, having larger will not matter, and having the same value
# as this param would result in half weights; one may setup fully discrete step by setting
# non integer number - like 4.5 (all hordes smaller than 5 will get 0 weights)
HORDE_SCORE_CENTRAL_SIZE_PARAM = 1

DEBUG_OVERRIDE_WEIGHTS_VERSION = env.int("DEBUG_OVERRIDE_WEIGHTS_VERSION", default=None)
DEBUG_OVERRIDE_SYNTHETIC_JOBS_FLOW_VERSION = env.int(
    "DEBUG_OVERRIDE_SYNTHETIC_JOBS_FLOW_VERSION", default=None
)

DYNAMIC_CONFIG_ENV = env.str("DYNAMIC_CONFIG_ENV", default="prod")

# prompt gen sampling
DEBUG_OVERRIDE_DYNAMIC_NUMBER_OF_PROMPTS_IN_SERIES = env.int(
    "DEBUG_OVERRIDE_DYNAMIC_NUMBER_OF_PROMPTS_IN_SERIES", default=None
)
DEBUG_OVERRIDE_DYNAMIC_NUMBER_OF_PROMPTS_TO_VALIDATE_FROM_SERIES = env.int(
    "DEBUG_OVERRIDE_DYNAMIC_NUMBER_OF_PROMPTS_TO_VALIDATE_IN_BATCH", default=None
)
DEBUG_OVERRIDE_DYNAMIC_NUMBER_OF_WORKLOADS_TO_TRIGGER_LOCAL_INFERENCE = env.int(
    "DEBUG_OVERRIDE_DYNAMIC_NUMBER_OF_WORKLOADS_TO_TRIGGER_LOCAL_INFERENCE", default=None
)

# synthetic jobs are evenly distributed through the cycle, however
# we start them from some offset because scheduling takes some time
SYNTHETIC_JOBS_RUN_OFFSET = env.int("SYNTHETIC_JOBS_RUN_OFFSET", default=24)

PROMPT_JOB_GENERATOR = env.str(
    "PROMPT_JOB_GENERATOR",
    default="compute_horde_validator.validator.cross_validation.generator.v0:PromptJobGenerator",
)


def BITTENSOR_WALLET() -> bittensor.wallet:
    if not BITTENSOR_WALLET_NAME or not BITTENSOR_WALLET_HOTKEY_NAME:
        raise RuntimeError("Wallet not configured")
    wallet = bittensor.wallet(
        name=BITTENSOR_WALLET_NAME,
        hotkey=BITTENSOR_WALLET_HOTKEY_NAME,
        path=str(BITTENSOR_WALLET_DIRECTORY),
    )
    wallet.hotkey_file.get_keypair()  # this raises errors if the keys are inaccessible
    return wallet


# Local miner generating prompts
GENERATION_MINER_KEY = env.str("GENERATION_MINER_KEY", default="")
GENERATION_MINER_ADDRESS = env.str("GENERATION_MINER_ADDRESS", default="")
GENERATION_MINER_PORT = env.int("GENERATION_MINER_PORT", default=0)


CHANNEL_LAYERS = {
    "default": {
        "BACKEND": "channels_redis.core.RedisChannelLayer",
        "CONFIG": {
            "hosts": [
                (env.str("REDIS_HOST", default="redis"), env.int("REDIS_PORT", default="6379"))
            ],
            # we need some buffer here to handle synthetic job batch messages
            "capacity": 50_000,
            # machine spec messages can take time to process and pile up, so we need a
            # larger expiration time. One hour should be plenty.
            "expiry": 3600,
        },
    },
}

AWS_ACCESS_KEY_ID = env("AWS_ACCESS_KEY_ID", default=None)
AWS_SECRET_ACCESS_KEY = env("AWS_SECRET_ACCESS_KEY", default=None)
AWS_ENDPOINT_URL = env("AWS_ENDPOINT_URL", default=None)

S3_BUCKET_NAME_PROMPTS = env("S3_BUCKET_NAME_PROMPTS", default=None)
S3_BUCKET_NAME_ANSWERS = env("S3_BUCKET_NAME_ANSWERS", default=None)

# Sentry
if SENTRY_DSN := env("SENTRY_DSN", default=""):
    import sentry_sdk
    from sentry_sdk.integrations.celery import CeleryIntegration
    from sentry_sdk.integrations.django import DjangoIntegration
    from sentry_sdk.integrations.logging import LoggingIntegration
    from sentry_sdk.integrations.redis import RedisIntegration

    sentry_sdk.init(  # type: ignore
        dsn=SENTRY_DSN,
        environment=ENV,
        integrations=[
            DjangoIntegration(),
            CeleryIntegration(),
            RedisIntegration(),
            LoggingIntegration(
                level=logging.INFO,  # Capture info and above as breadcrumbs
                event_level=logging.ERROR,  # Send error events from log messages
            ),
        ],
    )
