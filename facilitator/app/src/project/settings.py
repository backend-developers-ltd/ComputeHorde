"""
Django settings for project project.
"""

import inspect
import logging
from datetime import timedelta
from functools import wraps

import environ
import structlog

root = environ.Path(__file__) - 2

env = environ.Env(DEBUG=(bool, False))

# .env file contents are not passed to docker image during build stage;
# this results in errors if you require some env var to be set, as if in `env("MYVAR")`` -
# obviously it's not set during build stage, but you don't care and want to ignore that.
# To mitigate this, we set ENV_FILL_MISSING_VALUES=1 during build phase, and it activates
# monkey-patching of "environ" module, so that all unset variables are set to None and
# the library is not complaining anymore
if env.bool("ENV_FILL_MISSING_VALUES", default=False):

    def patch(fn):
        @wraps(fn)
        def wrapped(*args, **kwargs):
            if kwargs.get("default") is env.NOTSET:
                kwargs["default"] = {
                    bool: False,
                    int: 0,
                    float: 0.0,
                }.get(kwargs.get("cast"), None)

            return fn(*args, **kwargs)

        return wrapped

    for name, method in inspect.getmembers(env, predicate=inspect.ismethod):
        setattr(env, name, patch(method))

# read from the .env file if hasn"t been sourced already
if env("ENV", default=None) is None:
    env.read_env(root("../../.env"))

ENV = env("ENV")

# SECURITY WARNING: keep the secret key used in production secret!
SECRET_KEY = env("SECRET_KEY")

# SECURITY WARNING: don"t run with debug turned on in production!
DEBUG = env("DEBUG")

ALLOWED_HOSTS = ["*"]

ADDITIONAL_PACKAGES = env("ADDITIONAL_PACKAGES", default="").split("\n")
ADDITIONAL_APPS = [app.split("@")[0].strip() for app in ADDITIONAL_PACKAGES if app]
assert all(ADDITIONAL_APPS), f"ADDITIONAL_PACKAGES contains empty app names: {ADDITIONAL_APPS=}"
INSTALLED_APPS = [
    "daphne",
    "django_prometheus",
    "django.contrib.admin",
    "django.contrib.auth",
    "django.contrib.contenttypes",
    "django.contrib.sessions",
    "django.contrib.messages",
    "django.contrib.staticfiles",
    "allauth",
    "allauth.account",
    "allauth.socialaccount",
    "django_structlog",
    "django_extensions",
    "django_probes",
    "constance",
    "rest_framework",
    "rest_framework.authtoken",
    "django_filters",
    *ADDITIONAL_APPS,
    "project.core",
    "compute_horde.receipts",
    "compute_horde.base",
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
    "django_prometheus.middleware.PrometheusBeforeMiddleware",
    "django.middleware.security.SecurityMiddleware",
    "django.contrib.sessions.middleware.SessionMiddleware",
    "django.middleware.common.CommonMiddleware",
    "django.middleware.csrf.CsrfViewMiddleware",
    "django.contrib.auth.middleware.AuthenticationMiddleware",
    "django.contrib.messages.middleware.MessageMiddleware",
    "django.middleware.clickjacking.XFrameOptionsMiddleware",
    "django_prometheus.middleware.PrometheusAfterMiddleware",
    "django_structlog.middlewares.RequestMiddleware",
    "allauth.account.middleware.AccountMiddleware",
    "project.core.middleware.FacilitatorSignatureMiddleware",
]

AUTHENTICATION_BACKENDS = [
    "django.contrib.auth.backends.ModelBackend",
    "allauth.account.auth_backends.AuthenticationBackend",
]
SOCIALACCOUNT_PROVIDERS = {}
ACCOUNT_EMAIL_VERIFICATION = "mandatory"
ACCOUNT_EMAIL_REQUIRED = True
ACCOUNT_EMAIL_SUBJECT_PREFIX = ""
ACCOUNT_LOGIN_ON_EMAIL_CONFIRMATION = True
LOGIN_REDIRECT_URL = "/"

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

CONSTANCE_BACKEND = "constance.backends.database.DatabaseBackend"
CONSTANCE_CONFIG = {
    "ENABLE_ORGANIC_JOBS": (True, "Whether to allow users to post organic jobss", bool),
    "VALIDATORS_LIMIT": (12, "Maximum number of active validators", int),
    "OUR_VALIDATOR_SS58_ADDRESS": ("", "Our validator's SS58 address", str),
    "JOB_REQUEST_VERSION": (1, "Version used of job request protocol (allows smooth migration)", int),
}

BITTENSOR_NETUID = env("BITTENSOR_NETUID")
BITTENSOR_NETWORK = env("BITTENSOR_NETWORK")
SIGNATURE_EXPIRE_DURATION = env("SIGNATURE_EXPIRE_DURATION", default="300")

R2_ENDPOINT_URL = env("R2_ENDPOINT_URL")
R2_ACCESS_KEY_ID = env("R2_ACCESS_KEY_ID")  # https://developers.cloudflare.com/r2/api/s3/tokens/
R2_SECRET_ACCESS_KEY = env("R2_SECRET_ACCESS_KEY")
R2_REGION_NAME = env("R2_REGION_NAME")  # must be one of: wnam, enam, weur, eeur, apac, auto
R2_BUCKET_NAME = env("R2_BUCKET_NAME")

# how long the signed URLs are valid for in general
OUTPUT_PRESIGNED_URL_LIFETIME = timedelta(seconds=env.int("OUTPUT_PRESIGNED_URL_LIFETIME"))
# how often the signed URLs are valid for downloading; after this timedelta, the download URL
# is invalid and should be regenerated (this is to prevent long-term access to the file)
DOWNLOAD_PRESIGNED_URL_LIFETIME = timedelta(seconds=env.int("DOWNLOAD_PRESIGNED_URL_LIFETIME"))

WANDB_API_KEY = env("WANDB_API_KEY")
COMPUTE_SUBNET_UID = 27

# Content Security Policy
if CSP_ENABLED := env.bool("CSP_ENABLED"):
    MIDDLEWARE.append("csp.middleware.CSPMiddleware")

    CSP_REPORT_ONLY = env.bool("CSP_REPORT_ONLY", default=True)
    CSP_REPORT_URL = env("CSP_REPORT_URL", default=None) or None

    CSP_DEFAULT_SRC = env.tuple("CSP_DEFAULT_SRC")
    CSP_SCRIPT_SRC = env.tuple("CSP_SCRIPT_SRC")
    CSP_STYLE_SRC = env.tuple("CSP_STYLE_SRC")
    CSP_FONT_SRC = env.tuple("CSP_FONT_SRC")
    CSP_IMG_SRC = env.tuple("CSP_IMG_SRC")
    CSP_MEDIA_SRC = env.tuple("CSP_MEDIA_SRC")
    CSP_OBJECT_SRC = env.tuple("CSP_OBJECT_SRC")
    CSP_FRAME_SRC = env.tuple("CSP_FRAME_SRC")
    CSP_CONNECT_SRC = env.tuple("CSP_CONNECT_SRC")
    CSP_CHILD_SRC = env.tuple("CSP_CHILD_SRC")
    CSP_MANIFEST_SRC = env.tuple("CSP_MANIFEST_SRC")
    CSP_WORKER_SRC = env.tuple("CSP_WORKER_SRC")

    CSP_BLOCK_ALL_MIXED_CONTENT = env.bool("CSP_BLOCK_ALL_MIXED_CONTENT", default=False)
    CSP_EXCLUDE_URL_PREFIXES = env.tuple("CSP_EXCLUDE_URL_PREFIXES", default=tuple())


ROOT_URLCONF = "project.urls"

TEMPLATES = [
    {
        "BACKEND": "django.template.backends.django.DjangoTemplates",
        "DIRS": [root("project/templates")],
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

WSGI_APPLICATION = "project.wsgi.application"
ASGI_APPLICATION = "project.asgi.application"
HTTP_ASGI_APPLICATION_PATH = env("HTTP_ASGI_APPLICATION_PATH", default="django.core.asgi.get_asgi_application")

DATABASES = {}
if env("DATABASE_POOL_URL"):  # DB transaction-based connection pool, such as one provided PgBouncer
    DATABASES["default"] = {
        **env.db_url("DATABASE_POOL_URL"),
        "DISABLE_SERVER_SIDE_CURSORS": True,  # prevents random cursor errors with transaction-based connection pool
    }
elif env("DATABASE_URL"):
    DATABASES["default"] = env.db_url("DATABASE_URL")

DEFAULT_AUTO_FIELD = "django.db.models.BigAutoField"

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
STATICFILES_FINDERS = [
    "django.contrib.staticfiles.finders.FileSystemFinder",
    "django.contrib.staticfiles.finders.AppDirectoriesFinder",
]
MEDIA_URL = env("MEDIA_URL", default="/media/")
MEDIA_ROOT = env("MEDIA_ROOT", default=root("media"))

# Security
# redirect HTTP to HTTPS
if env.bool("HTTPS_REDIRECT", default=False) and not DEBUG:
    SECURE_SSL_REDIRECT = True
    SECURE_REDIRECT_EXEMPT = []
    SESSION_COOKIE_SECURE = True
    CSRF_COOKIE_SECURE = True
else:
    SECURE_SSL_REDIRECT = False

CHANNELS_BACKEND_URL = env("CHANNELS_BACKEND_URL")
CHANNEL_LAYERS = {
    "default": {
        "BACKEND": "channels_redis.core.RedisChannelLayer",
        "CONFIG": {
            "hosts": [{"address": CHANNELS_BACKEND_URL}],
        },
    },
}

METAGRAPH_SYNC_PERIOD = timedelta(minutes=5)

CELERY_BROKER_URL = env("CELERY_BROKER_URL", default="")
CELERY_RESULT_BACKEND = env("CELERY_BROKER_URL", default="")  # store results in Redis
CELERY_RESULT_EXPIRES = int(timedelta(days=1).total_seconds())  # time until task result deletion
CELERY_COMPRESSION = "gzip"  # task compression
CELERY_MESSAGE_COMPRESSION = "gzip"  # result compression
CELERY_SEND_EVENTS = True  # needed for worker monitoring
CELERY_BEAT_SCHEDULE = {
    "sync_metagraph": {
        "task": "project.core.tasks.sync_metagraph",
        "schedule": METAGRAPH_SYNC_PERIOD,
        "options": {"time_limit": 60},
    },
    "record_compute_subnet_hardware": {
        "task": "project.core.tasks.record_compute_subnet_hardware",
        "schedule": timedelta(hours=12),
        "options": {"time_limit": 60},
    },
    "fetch_miner_versions": {
        "task": "project.core.tasks.fetch_miner_versions",
        "schedule": timedelta(minutes=30),
        "options": {"time_limit": 60},
    },
    "fetch_receipts": {
        "task": "project.core.tasks.fetch_receipts",
        "schedule": timedelta(minutes=30),
        "options": {"time_limit": 30},
    },
    "refresh_specs_materialized_view": {
        "task": "project.core.tasks.refresh_specs_materialized_view",
        "schedule": timedelta(minutes=60),
        "options": {},
    },
    "evict_old_data": {
        "task": "project.core.tasks.evict_old_data",
        "schedule": timedelta(days=1),
        "options": {},
    },
}
CELERY_TASK_ROUTES = ["project.celery.route_task"]
CELERY_TASK_TIME_LIMIT = int(timedelta(minutes=5).total_seconds())
CELERY_TASK_ALWAYS_EAGER = env.bool("CELERY_TASK_ALWAYS_EAGER", default=False)
CELERY_ACCEPT_CONTENT = ["json"]
CELERY_TASK_SERIALIZER = "json"
CELERY_RESULT_SERIALIZER = "json"
CELERY_WORKER_PREFETCH_MULTIPLIER = env.int("CELERY_WORKER_PREFETCH_MULTIPLIER", default=10)
CELERY_BROKER_POOL_LIMIT = env.int("CELERY_BROKER_POOL_LIMIT", default=50)

EMAIL_BACKEND = env("EMAIL_BACKEND")
EMAIL_FILE_PATH = env("EMAIL_FILE_PATH")
EMAIL_HOST = env("EMAIL_HOST")
EMAIL_PORT = env.int("EMAIL_PORT")
EMAIL_HOST_USER = env("EMAIL_HOST_USER")
EMAIL_HOST_PASSWORD = env("EMAIL_HOST_PASSWORD")
EMAIL_USE_TLS = env.bool("EMAIL_USE_TLS")
DEFAULT_FROM_EMAIL = env("DEFAULT_FROM_EMAIL")

LOGGING = {
    "version": 1,
    "disable_existing_loggers": False,
    "formatters": {
        "console": {
            "()": structlog.stdlib.ProcessorFormatter,
            "processor": structlog.dev.ConsoleRenderer(),
        },
    },
    "handlers": {
        "console": {
            "class": "logging.StreamHandler",
            "formatter": "console",
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
        # Fix spamming DEBUG-level logs in manage.py shell and shell_plus.
        "parso": {
            "handlers": ["console"],
            "level": "INFO",
            "propagate": True,
        },
    },
}

DJANGO_STRUCTLOG_CELERY_ENABLED = True
structlog.configure(
    processors=[
        structlog.contextvars.merge_contextvars,
        structlog.stdlib.filter_by_level,
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.stdlib.add_logger_name,
        structlog.stdlib.add_log_level,
        structlog.stdlib.PositionalArgumentsFormatter(),
        structlog.processors.StackInfoRenderer(),
        structlog.processors.format_exc_info,
        structlog.processors.UnicodeDecoder(),
        structlog.stdlib.ProcessorFormatter.wrap_for_formatter,
    ],
    logger_factory=structlog.stdlib.LoggerFactory(),
    cache_logger_on_first_use=True,
)

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


REST_FRAMEWORK = {
    "DEFAULT_AUTHENTICATION_CLASSES": [
        "rest_framework.authentication.TokenAuthentication",
        "rest_framework.authentication.SessionAuthentication",
    ],
    "DEFAULT_RENDERER_CLASSES": [
        "rest_framework.renderers.JSONRenderer",
    ],
    "DEFAULT_PARSER_CLASSES": [
        "rest_framework.parsers.JSONParser",
    ],
    "TEST_REQUEST_DEFAULT_FORMAT": "json",
    "EXCEPTION_HANDLER": "project.core.exception_handlers.api_exception_handler",
    "DEFAULT_SCHEMA_CLASS": "django_pydantic_field.rest_framework.AutoSchema",
}
