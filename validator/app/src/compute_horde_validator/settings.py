"""
Django settings for compute_horde_validator project.
"""

import decimal
import inspect
import logging
import pathlib
from datetime import timedelta
from functools import wraps

import bittensor_wallet
import environ
from bt_ddos_shield.shield_metagraph import ShieldMetagraphOptions
from celery.schedules import crontab
from compute_horde import base  # noqa

# Set decimal precision.
# We need to be able to represent 256-bit unsigned integers.
# Calculated from: math.ceil(math.log10(2**256 - 1))
DECIMAL_PRECISION = 78
decimal.getcontext().prec = DECIMAL_PRECISION

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

PROMPT_GENERATION_MODEL = env("PROMPT_GENERATION_MODEL", default="phi3")

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
    "compute_horde.receipts",
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
CONSTANCE_DATABASE_CACHE_BACKEND = "default"
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
    "DYNAMIC_NON_PEAK_CYCLE_EXECUTOR_MIN_RATIO": (
        0.1,
        "Ratio of the number of executors in peak cycle that needs to be present in non-peak cycle",
        float,
    ),
    "DYNAMIC_NON_PEAK_CYCLE_PENALTY_MULTIPLIER": (
        0.8,  # 0.8 means, 20% score reduction
        "Penalty multiplier for non-peak cycle when a miner does not maintain the DYNAMIC_NON_PEAK_CYCLE_EXECUTOR_MIN_RATIO",
        float,
    ),
    "DYNAMIC_DEFAULT_EXECUTOR_LIMITS_FOR_MISSED_PEAK": (
        "always_on.llm.a6000=2",
        "The limits of executors per class per miner, if the miner or this validator missed the peak batch",
        str,
    ),
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
        722,
        "In blocks. This should be synced with the hyperparam",
        int,
    ),
    "DYNAMIC_COMMIT_REVEAL_COMMIT_START_OFFSET": (
        361,
        "Do not commit weights for this many blocks from the start of the interval",
        int,
    ),
    "DYNAMIC_COMMIT_REVEAL_COMMIT_END_BUFFER": (
        15,
        "Do not commit weights if there are less than this many blocks left in the commit window",
        int,
    ),
    "DYNAMIC_COMMIT_REVEAL_REVEAL_END_BUFFER": (
        15,
        "Do not reveal weights if there are less than this many blocks left in the reveal window",
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
    # llama params
    "DYNAMIC_MAX_PROMPT_SERIES": (
        3500,
        "Maximum number of prompt series upon which the prompt generator will not be triggered",
        int,
    ),
    "DYNAMIC_TARGET_NUMBER_OF_PROMPT_SAMPLES_READY": (
        1536,  # 256 * 2 * 3 - we allow 2 executors per miner and want queue for 3 synthetic job batches
        "how many prompt samples to generate (should be larger than how many prompts series we use per synthetic run)",
        int,
    ),
    "DYNAMIC_NUMBER_OF_PROMPTS_PER_WORKLOAD": (
        240,
        "how many prompts to answer in a single workload",
        int,
    ),
    # prompt generation params
    "DYNAMIC_PROMPTS_SERIES_IN_A_SINGLE_GENERATION": (
        25,
        "Number of batches that prompt generator will process in a single go",
        int,
    ),
    "DYNAMIC_NUMBER_OF_PROMPTS_IN_SERIES": (
        240,
        "Number of prompts to generate in a single series",
        int,
    ),
    # prompts answering params
    "DYNAMIC_NUMBER_OF_PROMPTS_TO_SAMPLE_FROM_SERIES": (
        1,
        "how many prompts to sample and answer from a series",
        int,
    ),
    "DYNAMIC_MINER_MAX_EXECUTORS_PER_CLASS": (
        "always_on.llm.a6000=2",
        (
            "The maximum number of executor for an executor class that miners are allowed to have. "
            "Executor classes not mentioned here have no limits. "
            "The format should be: 'key1=value1,key2=value2', "
            "where the keys are executor class enum values, and the values are integers. "
            "Setting 0 will disable an executor class."
        ),
        str,
    ),
    "DYNAMIC_EXECUTOR_CLASS_WEIGHTS": (
        "spin_up-4min.gpu-24gb=99,always_on.llm.a6000=1",
        (
            "Weights of executor classes that are used to normalize miners scores. "
            "Executor classes not mentioned here are not taken into account when scoring. "
            "The format should be: 'key1=value1,key2=value2', "
            "where the keys are executor class enum values, and the values are floats, "
            "but int values that sum up to 100 are encouraged"
        ),
        str,
    ),
    "DYNAMIC_EXCUSED_SYNTHETIC_JOB_SCORE": (
        1.0,
        "Score for each properly excused synthetic job",
        float,
    ),
    "DYNAMIC_MINIMUM_VALIDATOR_STAKE_FOR_EXCUSE": (
        30_000.0,
        "The minimum effective stake that a validator must have for its job to properly excuse other jobs (denominated in alpha)",
        float,
    ),
    "DYNAMIC_ORGANIC_JOB_SCORE": (
        1.0,
        "Score of each successful organic job",
        float,
    ),
    "DYNAMIC_SCORE_ORGANIC_JOBS_LIMIT": (
        -1,
        "Maximum number of organic jobs each miner can get scores for. Negative value means unlimited.",
        int,
    ),
    "DYNAMIC_EXECUTOR_RESERVATION_TIME_LIMIT": (
        7,
        "Time for miner to accept or decline an organic job in seconds",
        int,
    ),
    "DYNAMIC_EXECUTOR_STARTUP_TIME_LIMIT": (
        5,
        "Time it takes for the executor to perform its startup stage (security checks, docker image check)",
        int,
    ),
    "DYNAMIC_ORGANIC_JOB_ALLOWED_LEEWAY_TIME": (
        5,
        "Additional time granted to the executor for the job-related tasks (download, execute, upload)",
        int,
    ),
    "DYNAMIC_DISABLE_TRUSTED_ORGANIC_JOB_EVENTS": (
        True,
        "Disable system events for organic jobs run on trusted miner",
        bool,
    ),
    "DYNAMIC_SYSTEM_EVENT_LIMITS": (
        "MINER_SYNTHETIC_JOB_FAILURE,LLM_PROMPT_ANSWERS_MISSING,10",
        "Limits of system events produced for each type-subtype pairs in a synthetic job run. Format: TYPE1,SUBTYPE1,100;TYPE2,SUBTYPE2,200",
        str,
    ),
    "DYNAMIC_LLM_ANSWER_S3_DOWNLOAD_TIMEOUT_SECONDS": (
        5.0,
        "Total timeout for downloading answer files from S3.",
        float,
    ),
    "DYNAMIC_RECEIPT_TRANSFER_ENABLED": (
        False,
        "Whether receipt transfer between miners and validators should be enabled",
        bool,
    ),
    "DYNAMIC_RECEIPT_TRANSFER_INTERVAL": (
        2,
        "Seconds between consecutive receipt polling",
        int,
    ),
    "DYNAMIC_SYNTHETIC_STREAMING_JOB_EXECUTOR_CLASSES": (
        "",
        "Comma separated list of classes to run streaming jobs on during synthetic jobs batch runs",
        str,
    ),
    "DYNAMIC_SYNTHETIC_STREAMING_JOB_READY_TIMEOUT": (
        300,
        "Timeout for waiting for a streaming job to be ready to accept connections from the user",
        int,
    ),
    "DYNAMIC_JOB_FAILURE_BLACKLIST_TIME_SECONDS": (
        int(timedelta(hours=4).total_seconds()),
        "Amount of time a miner will be temporarily blacklisted for after failing an organic job.",
        int,
    ),
    "DYNAMIC_JOB_CHEATED_BLACKLIST_TIME_SECONDS": (
        int(timedelta(hours=4).total_seconds()),
        "Amount of time a miner will be temporarily blacklisted for after being reported cheating on an organic job.",
        int,
    ),
    "DYNAMIC_BURN_TARGET_SS58ADDRESSES": (
        "Comma-separated list of ss58 addresses that will receive 'DYNAMIC_BURN_RATE' fraction of all incentives",
        "",
        str,
    ),
    "DYNAMIC_BURN_RATE": (
        0.0,
        "(0.0 - 1.0) fraction of miner incentives that will be directed to 'DYNAMIC_BURN_TARGET_SS58ADDRESSES' ",
        float,
    ),
    "DYNAMIC_BURN_PARTITION": (
        0.0,
        "(0.0 - 1.0) each time miner incentive is burned, if there is more than one hotkey registered from among "
        "'DYNAMIC_BURN_TARGET_SS58ADDRESSES', one will be chosen as the primary at random (but random seed is the same "
        "across all validators) and will receive 'DYNAMIC_BURN_PARTITION' fraction of all the burn.",
        float,
    ),
    "DYNAMIC_ROUTING_PRELIMINARY_RESERVATION_TIME_SECONDS": (
        10.0,
        "How long to initially reserve an executor for during job routing request. This should last only long enough "
        "for the job flow to create and store a job started receipt.",
        float,
    ),
    "ORGANIC_JOB_CELERY_WAIT_TIMEOUT": (
        600,
        "How long to wait for Celery to execute the organic job",
        int,
    ),
    # collateral params
    "DYNAMIC_MINIMUM_COLLATERAL_AMOUNT_WEI": (
        10_000_000_000_000_000,  # 0.01 tao
        "Minimum collateral amount (in Wei) for a miner to be considered for a job",
        int,
    ),
    "DYNAMIC_COLLATERAL_SLASH_AMOUNT_WEI": (
        5_000_000_000_000_000,  # 0.005 tao
        "Amount (in Wei) of collateral to be slashed if a miner cheats on a job",
        int,
    ),
    "DYNAMIC_ALLOW_CROSS_CYCLE_ORGANIC_JOBS": (
        True,
        "Whether organic jobs can be scheduled to run for longer than the current cycle",
        bool,
    ),
    "DYNAMIC_CHECK_ALLOWANCE_WHILE_ROUTING": (
        True,
        "Whether to check for remaining allowance while picking a miner for an organic job. "
        "In both cases allowance will still be deducted.",
        bool,
    ),
}

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
default_db = f"postgres://postgres:{env('POSTGRES_PASSWORD')}@db:5432/postgres"
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

REDIS_HOST = env.str("REDIS_HOST", default="redis")
REDIS_PORT = env.int("REDIS_PORT", default=6379)

CACHES = {
    "default": {
        "BACKEND": "django.core.cache.backends.redis.RedisCache",
        "LOCATION": f"redis://{REDIS_HOST}:{REDIS_PORT}/1",
    },
    "receipts_checkpoints": {
        "BACKEND": "django.core.cache.backends.redis.RedisCache",
        "LOCATION": f"redis://{REDIS_HOST}:{REDIS_PORT}/2",
        "TIMEOUT": 60 * 60 * 24 * 2,  # Remember the checkpoints for 2 days
    },
}

RECEIPT_TRANSFER_CHECKPOINT_CACHE = "receipts_checkpoints"

CELERY_BROKER_URL = env("CELERY_BROKER_URL", default=f"redis://{REDIS_HOST}:{REDIS_PORT}/0")
CELERY_RESULT_BACKEND = env(
    "CELERY_BROKER_URL", default=f"redis://{REDIS_HOST}:{REDIS_PORT}/0"
)  # store results in Redis
CELERY_RESULT_EXPIRES = int(timedelta(days=1).total_seconds())  # time until task result deletion
CELERY_COMPRESSION = "gzip"  # task compression
CELERY_MESSAGE_COMPRESSION = "gzip"  # result compression
CELERY_SEND_EVENTS = True  # needed for worker monitoring
CELERY_BEAT_SCHEDULE = {
    "sync_metagraph": {
        "task": "compute_horde_validator.validator.tasks.sync_metagraph",
        "schedule": timedelta(seconds=10),
        "options": {
            "expires": timedelta(seconds=10).total_seconds(),
        },
    },
    "schedule_synthetic_jobs": {
        "task": "compute_horde_validator.validator.tasks.schedule_synthetic_jobs",
        "schedule": timedelta(minutes=1),
        "options": {
            "expires": timedelta(minutes=1).total_seconds(),
        },
    },
    "run_synthetic_jobs": {
        "task": "compute_horde_validator.validator.tasks.run_synthetic_jobs",
        "schedule": timedelta(seconds=env.int("DEBUG_RUN_SYNTHETIC_JOBS_SECONDS", default=30)),
        "options": {
            "expires": timedelta(
                seconds=env.int("DEBUG_RUN_SYNTHETIC_JOBS_SECONDS", default=30)
            ).total_seconds(),
        },
    },
    "check_missed_synthetic_jobs": {
        "task": "compute_horde_validator.validator.tasks.check_missed_synthetic_jobs",
        "schedule": timedelta(minutes=10),
        "options": {
            "expires": timedelta(minutes=10).total_seconds(),
        },
    },
    "set_scores": {
        "task": "compute_horde_validator.validator.tasks.set_scores",
        "schedule": crontab(
            minute=env("DEBUG_SET_SCORES_MINUTE", default="*/1"),
            hour=env("DEBUG_SET_SCORES_HOUR", default="*"),
        ),
        "options": {
            "expires": timedelta(minutes=1).total_seconds(),
        },
    },
    "reveal_scores": {
        "task": "compute_horde_validator.validator.tasks.reveal_scores",
        "schedule": timedelta(minutes=1),
        "options": {
            "expires": timedelta(minutes=1).total_seconds(),
        },
    },
    "send_events_to_facilitator": {
        "task": "compute_horde_validator.validator.tasks.send_events_to_facilitator",
        "schedule": timedelta(minutes=5),
        "options": {
            "expires": timedelta(minutes=5).total_seconds(),
        },
    },
    "fetch_dynamic_config": {
        "task": "compute_horde_validator.validator.tasks.fetch_dynamic_config",
        "schedule": timedelta(minutes=5),
        "options": {
            "expires": timedelta(minutes=5).total_seconds(),
        },
    },
    "llm_prompt_generation": {
        "task": "compute_horde_validator.validator.tasks.llm_prompt_generation",
        "schedule": timedelta(minutes=5),
        "options": {
            "expires": timedelta(minutes=5).total_seconds(),
        },
    },
    "llm_prompt_sampling": {
        "task": "compute_horde_validator.validator.tasks.llm_prompt_sampling",
        "schedule": timedelta(minutes=30),
        "options": {
            "expires": timedelta(minutes=30).total_seconds(),
        },
    },
    "llm_prompt_answering": {
        "task": "compute_horde_validator.validator.tasks.llm_prompt_answering",
        "schedule": timedelta(minutes=5),
        "options": {
            "expires": timedelta(minutes=5).total_seconds(),
        },
    },
    "evict_old_data": {
        "task": "compute_horde_validator.validator.tasks.evict_old_data",
        "schedule": timedelta(days=1),
        "options": {
            "expires": timedelta(days=1).total_seconds(),
        },
    },
    "set_compute_time_allowances": {
        "task": "compute_horde_validator.validator.tasks.set_compute_time_allowances",
        "schedule": timedelta(minutes=1),
        "options": {
            "expires": timedelta(minutes=1).total_seconds(),
        },
    },
    "poll_miner_manifests": {
        "task": "compute_horde_validator.validator.tasks.poll_miner_manifests",
        "schedule": timedelta(minutes=5),
        "options": {
            "expires": timedelta(minutes=5).total_seconds(),
        },
    },
}
if env.bool("DEBUG_RUN_BEAT_VERY_OFTEN", default=False):
    CELERY_BEAT_SCHEDULE["run_synthetic_jobs"]["schedule"] = crontab(minute="*")
    CELERY_BEAT_SCHEDULE["set_scores"]["schedule"] = crontab(minute="*/3")

CELERY_TASK_ROUTES = ["compute_horde_validator.celery.route_task"]
CELERY_TASK_TIME_LIMIT = int(timedelta(hours=2, minutes=5).total_seconds())
CELERY_TASK_ALWAYS_EAGER = env.bool("CELERY_TASK_ALWAYS_EAGER", default=False)
CELERY_WORKER_SEND_TASK_EVENTS = True
CELERY_TASK_SEND_SENT_EVENT = True
CELERY_ACCEPT_CONTENT = ["json"]
CELERY_TASK_SERIALIZER = "json"
CELERY_RESULT_SERIALIZER = "json"
CELERY_WORKER_PREFETCH_MULTIPLIER = env.int("CELERY_WORKER_PREFETCH_MULTIPLIER", default=10)
CELERY_BROKER_POOL_LIMIT = env.int("CELERY_BROKER_POOL_LIMIT", default=50)

WORKER_HEALTHCHECK_FILE_PATH = env(
    "WORKER_HEALTHCHECK_FILE_PATH", default="/tmp/worker-healthcheck"
)

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
        "compute_horde.receipts.transfer": {
            "handlers": ["console"],
            "level": "WARNING",
            "propagate": True,
        },
        "botocore": {
            "handlers": ["console"],
            "level": "WARNING",
            "propagate": True,
        },
        "httpcore.http11": {
            "handlers": ["console"],
            "level": "WARNING",
            "propagate": True,
        },
        # Fix spamming DEBUG-level logs in manage.py shell and shell_plus.
        "parso": {
            "handlers": ["console"],
            "level": "INFO",
            "propagate": True,
        },
        # This also spams DEBUG messages
        "btdecode": {
            "handlers": ["console"],
            "level": "WARNING",
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

BITTENSOR_SHIELD_CERTIFICATE_PATH = env.path(
    "BITTENSOR_SHIELD_CERTIFICATE_PATH",
    default=BITTENSOR_WALLET_DIRECTORY.path(
        BITTENSOR_WALLET_NAME, "shield", BITTENSOR_WALLET_HOTKEY_NAME, "validator_cert.pem"
    ),
)
BITTENSOR_SHIELD_DISABLE_UPLOADING_CERTIFICATE = env.bool(
    "BITTENSOR_SHIELD_DISABLE_UPLOADING_CERTIFICATE",
    default=False,
)


def BITTENSOR_SHIELD_METAGRAPH_OPTIONS() -> ShieldMetagraphOptions:
    pathlib.Path(BITTENSOR_SHIELD_CERTIFICATE_PATH).parent.mkdir(parents=True, exist_ok=True)
    return ShieldMetagraphOptions(
        disable_uploading_certificate=BITTENSOR_SHIELD_DISABLE_UPLOADING_CERTIFICATE,
        certificate_path=str(BITTENSOR_SHIELD_CERTIFICATE_PATH),
        replace_ip_address_for_axon=False,
    )


SYNTHETIC_JOB_GENERATOR_FACTORY = env.str(
    "SYNTHETIC_JOB_GENERATOR_FACTORY",
    default="compute_horde_validator.validator.synthetic_jobs.generator.factory:DefaultSyntheticJobGeneratorFactory",
)
FACILITATOR_URI = env.str(
    "FACILITATOR_URI", default="wss://facilitator.computehorde.io/ws/v0/"
).strip()
DEBUG_CONNECT_FACILITATOR_WEBHOOK = env.str("DEBUG_CONNECT_FACILITATOR_WEBHOOK", default=None)
DEBUG_USE_MOCK_BLOCK_NUMBER = env.bool("DEBUG_USE_MOCK_BLOCK_NUMBER", default=False)
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

# synthetic jobs are evenly distributed through the cycle, however
# we start them from some offset because scheduling takes some time
SYNTHETIC_JOBS_RUN_OFFSET = env.int("SYNTHETIC_JOBS_RUN_OFFSET", default=24)

PROMPT_JOB_GENERATOR = env.str(
    "PROMPT_JOB_GENERATOR",
    default="compute_horde_validator.validator.cross_validation.generator.v0:PromptJobGenerator",
)


def BITTENSOR_WALLET() -> bittensor_wallet.Wallet:
    if not BITTENSOR_WALLET_NAME or not BITTENSOR_WALLET_HOTKEY_NAME:
        raise RuntimeError("Wallet not configured")
    wallet = bittensor_wallet.Wallet(
        name=BITTENSOR_WALLET_NAME,
        hotkey=BITTENSOR_WALLET_HOTKEY_NAME,
        path=str(BITTENSOR_WALLET_DIRECTORY),
    )
    wallet.hotkey_file.get_keypair()  # this raises errors if the keys are inaccessible
    return wallet


# Local miner generating prompts
TRUSTED_MINER_ADDRESS = env.str("TRUSTED_MINER_ADDRESS", default="")
TRUSTED_MINER_PORT = env.int("TRUSTED_MINER_PORT", default=0)

# This env var is expected to be a list of hotkey:ip:port
DEBUG_FETCH_RECEIPTS_FROM_MINERS: list[tuple[str, str, int]] = []
for miner in env.list("DEBUG_FETCH_RECEIPTS_FROM_MINERS", default=[]):
    hotkey, ip, port = miner.split(":")
    DEBUG_FETCH_RECEIPTS_FROM_MINERS.append((hotkey, ip, port))

CHANNEL_LAYERS = {
    "default": {
        # Apparently pubsub backend is in "beta" state, yet seems more stable than the older redis backend.
        "BACKEND": env.str("CHANNELS_BACKEND", "channels_redis.pubsub.RedisPubSubChannelLayer"),
        "CONFIG": {
            "hosts": [
                (REDIS_HOST, REDIS_PORT),
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
AWS_SIGNATURE_VERSION = env("AWS_SIGNATURE_VERSION", default=None)

S3_BUCKET_NAME_PROMPTS = env("S3_BUCKET_NAME_PROMPTS", default=None)
S3_BUCKET_NAME_ANSWERS = env("S3_BUCKET_NAME_ANSWERS", default=None)

# Sentry
if SENTRY_DSN := env("SENTRY_DSN", default=""):
    import sentry_sdk
    from sentry_sdk.integrations.celery import CeleryIntegration
    from sentry_sdk.integrations.django import DjangoIntegration
    from sentry_sdk.integrations.logging import LoggingIntegration
    from sentry_sdk.integrations.redis import RedisIntegration

    sentry_sdk.init(
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
