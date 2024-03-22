"""
Django settings for compute_horde_miner project.
"""
from compute_horde import base  # noqa

import inspect
import ipaddress
import logging
import pathlib
from datetime import timedelta
from functools import wraps

import bittensor
import environ


# from celery.schedules import crontab


root = environ.Path(__file__) - 2

env = environ.Env(DEBUG=(bool, False))

# .env file contents are not passed to docker image during build stage;
# this results in errors if you require some env var to be set, as if in "env('MYVAR')" -
# obviously it's not set during build stage, but you don't care and want to ignore that.
# To mitigate this, we set ENV_FILL_MISSING_VALUES=1 during build phase, and it activates
# monkey-patching of "environ" module, so that all unset variables are set to None and
# the library is not complaining anymore
if env.bool('ENV_FILL_MISSING_VALUES', default=False):

    def patch(fn):
        @wraps(fn)
        def wrapped(*args, **kwargs):
            if kwargs.get('default') is env.NOTSET:
                kwargs['default'] = None
            return fn(*args, **kwargs)
        return wrapped

    for name, method in inspect.getmembers(env, predicate=inspect.ismethod):
        setattr(env, name, patch(method))

# read from the .env file if hasn't been sourced already
if env('ENV', default=None) is None:
    env.read_env(root('../../.env'))

ENV = env('ENV', default='prod')

# SECURITY WARNING: keep the secret key used in production secret!
SECRET_KEY = env('SECRET_KEY')

# SECURITY WARNING: don't run with debug turned on in production!
DEBUG = env.bool('DEBUG', default=False)

ALLOWED_HOSTS = ['*']

INSTALLED_APPS = [
    'daphne',
    'channels',
    # 'django_prometheus',
    'django.contrib.admin',
    'django.contrib.auth',
    'django.contrib.contenttypes',
    'django.contrib.sessions',
    'django.contrib.messages',
    'django.contrib.staticfiles',

    'django_extensions',
    'django_probes',
    'compute_horde_miner.miner',
]
PROMETHEUS_EXPORT_MIGRATIONS = True
PROMETHEUS_LATENCY_BUCKETS = (.008, .016, .032, .062, .125, .25, .5, 1.0, 2.0, 4.0, 8.0, 16.0, 32.0, 64.0, float("inf"))


MIDDLEWARE = [
    # 'django_prometheus.middleware.PrometheusBeforeMiddleware',
    'django.middleware.security.SecurityMiddleware',
    'django.contrib.sessions.middleware.SessionMiddleware',
    'django.middleware.common.CommonMiddleware',
    'django.middleware.csrf.CsrfViewMiddleware',
    'django.contrib.auth.middleware.AuthenticationMiddleware',
    'django.contrib.messages.middleware.MessageMiddleware',
    'django.middleware.clickjacking.XFrameOptionsMiddleware',
    # 'django_prometheus.middleware.PrometheusAfterMiddleware',
]


if DEBUG_TOOLBAR := env.bool('DEBUG_TOOLBAR', default=False):
    INTERNAL_IPS = [
        "127.0.0.1",
    ]

    DEBUG_TOOLBAR_CONFIG = {
        'SHOW_TOOLBAR_CALLBACK': lambda _request: True
    }
    INSTALLED_APPS.append('debug_toolbar')
    MIDDLEWARE = ['debug_toolbar.middleware.DebugToolbarMiddleware'] + MIDDLEWARE

if CORS_ENABLED := env.bool('CORS_ENABLED', default=True):
    INSTALLED_APPS.append('corsheaders')
    MIDDLEWARE = ['corsheaders.middleware.CorsMiddleware'] + MIDDLEWARE
    CORS_ALLOWED_ORIGINS = env.list('CORS_ALLOWED_ORIGINS', default=[])
    CORS_ALLOWED_ORIGIN_REGEXES = env.list('CORS_ALLOWED_ORIGIN_REGEXES', default=[])
    CORS_ALLOW_ALL_ORIGINS = env.bool('CORS_ALLOW_ALL_ORIGINS', default=False)

SECURE_PROXY_SSL_HEADER = ('HTTP_X_FORWARDED_PROTO', 'https')

# Content Security Policy
if CSP_ENABLED := env.bool('CSP_ENABLED', default=False):
    MIDDLEWARE.append('csp.middleware.CSPMiddleware')

    CSP_REPORT_ONLY = env.bool('CSP_REPORT_ONLY', default=True)
    CSP_REPORT_URL = env('CSP_REPORT_URL', default=None) or None

    CSP_DEFAULT_SRC = env.tuple('CSP_DEFAULT_SRC', default=("'none'",))
    CSP_SCRIPT_SRC = env.tuple('CSP_SCRIPT_SRC', default=("'self'",))
    CSP_STYLE_SRC = env.tuple('CSP_STYLE_SRC', default=("'self'",))
    CSP_FONT_SRC = env.tuple('CSP_FONT_SRC', default=("'self'",))
    CSP_IMG_SRC = env.tuple('CSP_IMG_SRC', default=("'self'",))
    CSP_MEDIA_SRC = env.tuple('CSP_MEDIA_SRC', default=("'self'",))
    CSP_OBJECT_SRC = env.tuple('CSP_OBJECT_SRC', default=("'self'",))
    CSP_FRAME_SRC = env.tuple('CSP_FRAME_SRC', default=("'self'",))
    CSP_CONNECT_SRC = env.tuple('CSP_CONNECT_SRC', default=("'self'",))
    CSP_CHILD_SRC = env.tuple('CSP_CHILD_SRC', default=("'self'",))
    CSP_MANIFEST_SRC = env.tuple('CSP_MANIFEST_SRC', default=("'self'",))
    CSP_WORKER_SRC = env.tuple('CSP_WORKER_SRC', default=("'self'",))

    CSP_BLOCK_ALL_MIXED_CONTENT = env.bool('CSP_BLOCK_ALL_MIXED_CONTENT', default=False)
    CSP_EXCLUDE_URL_PREFIXES = env.tuple('CSP_EXCLUDE_URL_PREFIXES', default=tuple())


ROOT_URLCONF = 'compute_horde_miner.urls'

TEMPLATES = [
    {
        'BACKEND': 'django.template.backends.django.DjangoTemplates',
        'DIRS': [root('compute_horde_miner/templates')],
        'APP_DIRS': True,
        'OPTIONS': {
            'context_processors': [
                'django.template.context_processors.debug',
                'django.template.context_processors.request',
                'django.contrib.auth.context_processors.auth',
                'django.contrib.messages.context_processors.messages',
            ],
        },
    },
]

ASGI_APPLICATION = 'compute_horde_miner.asgi.application'

DATABASES = {}
default_db = f'postgres://postgres:{env("POSTGRES_PASSWORD")}@db:5432/postgres'
if env('DATABASE_POOL_URL', default=''):  # DB transaction-based connection pool, such as one provided PgBouncer
    DATABASES['default'] = {
        **env.db_url('DATABASE_POOL_URL'),
        'DISABLE_SERVER_SIDE_CURSORS': True,  # prevents random cursor errors with transaction-based connection pool
    }
elif env('DATABASE_URL', default=default_db):
    DATABASES['default'] = env.db_url('DATABASE_URL', default=default_db)

if 'default' in DATABASES:
    DATABASES['default']['NAME'] += env.str('DATABASE_SUFFIX', default='')

DEFAULT_AUTO_FIELD = 'django.db.models.BigAutoField'

AUTH_PASSWORD_VALIDATORS = [
    {
        'NAME': 'django.contrib.auth.password_validation.UserAttributeSimilarityValidator',
    },
    {
        'NAME': 'django.contrib.auth.password_validation.MinimumLengthValidator',
    },
    {
        'NAME': 'django.contrib.auth.password_validation.CommonPasswordValidator',
    },
    {
        'NAME': 'django.contrib.auth.password_validation.NumericPasswordValidator',
    },
]

# Internationalization
LANGUAGE_CODE = 'en-us'
TIME_ZONE = 'UTC'
USE_I18N = True
USE_L10N = True
USE_TZ = True

# Static files (CSS, JavaScript, Images)
STATIC_URL = env('STATIC_URL', default='/static/')
STATIC_ROOT = env('STATIC_ROOT', default=root('static'))
MEDIA_URL = env('MEDIA_URL', default='/media/')
MEDIA_ROOT = env('MEDIA_ROOT', default=root('media'))

# Security
# redirect HTTP to HTTPS
if env.bool('HTTPS_REDIRECT', default=False) and not DEBUG:
    SECURE_SSL_REDIRECT = True
    SECURE_REDIRECT_EXEMPT = []  # type: ignore
    SESSION_COOKIE_SECURE = True
    CSRF_COOKIE_SECURE = True
else:
    SECURE_SSL_REDIRECT = False

CELERY_BROKER_URL = env('CELERY_BROKER_URL', default='redis://redis:6379/0')
CELERY_RESULT_BACKEND = env('CELERY_BROKER_URL', default='redis://redis:6379/0')  # store results in Redis
CELERY_RESULT_EXPIRES = int(timedelta(days=1).total_seconds())  # time until task result deletion
CELERY_COMPRESSION = 'gzip'  # task compression
CELERY_MESSAGE_COMPRESSION = 'gzip'  # result compression
CELERY_SEND_EVENTS = True  # needed for worker monitoring
CELERY_BEAT_SCHEDULE = {  # type: ignore
    'announce_address_and_port': {
        'task': 'compute_horde_miner.miner.tasks.announce_address_and_port',
        'schedule': 60,
        'options': {},
    },
    'fetch_validators': {
        'task': 'compute_horde_miner.miner.tasks.fetch_validators',
        'schedule': 60,
        'options': {},
    },
}
CELERY_TASK_ROUTES = ['compute_horde_miner.celery.route_task']
CELERY_TASK_TIME_LIMIT = int(timedelta(minutes=5).total_seconds())
CELERY_TASK_ALWAYS_EAGER = env.bool('CELERY_TASK_ALWAYS_EAGER', default=False)
CELERY_ACCEPT_CONTENT = ['json']
CELERY_TASK_SERIALIZER = 'json'
CELERY_RESULT_SERIALIZER = 'json'
CELERY_WORKER_PREFETCH_MULTIPLIER = env.int('CELERY_WORKER_PREFETCH_MULTIPLIER', default=10)
CELERY_BROKER_POOL_LIMIT = env.int('CELERY_BROKER_POOL_LIMIT', default=50)

EMAIL_BACKEND = env('EMAIL_BACKEND', default='django.core.mail.backends.filebased.EmailBackend')
EMAIL_FILE_PATH = env('EMAIL_FILE_PATH', default='/tmp/email')
EMAIL_HOST = env('EMAIL_HOST', default='smtp.sendgrid.net')
EMAIL_PORT = env.int('EMAIL_PORT', default=587)
EMAIL_HOST_USER = env('EMAIL_HOST_USER', default='apikey')
EMAIL_HOST_PASSWORD = env('EMAIL_HOST_PASSWORD', default='')
EMAIL_USE_TLS = env.bool('EMAIL_USE_TLS', default=True)
DEFAULT_FROM_EMAIL = env('DEFAULT_FROM_EMAIL', default='mail@localhost')

LOGGING = {
    'version': 1,
    'disable_existing_loggers': False,
    'formatters': {
        'main': {
            'format': '{levelname} {asctime} {name} {message}',
            'style': '{',
        },
    },
    'handlers': {
        'console': {
            'class': 'logging.StreamHandler',
            'formatter': 'main',
        },
    },
    'root': {
        'handlers': ['console'],
        'level': 'DEBUG',
    },
    'loggers': {
        'django': {
            'handlers': ['console'],
            'level': 'INFO',
            'propagate': True,
        },
        'websockets': {
            'handlers': ['console'],
            'level': 'INFO',
            'propagate': True,
        },
        'daphne': {
            'handlers': ['console'],
            'level': 'INFO',
            'propagate': True,
        },
    },
}

EXECUTOR_MANAGER_CLASS_PATH = env.str('EXECUTOR_MANAGER_CLASS_PATH', default='compute_horde_miner.miner.executor_manager.docker:DockerExecutorManager')
EXECUTOR_IMAGE = env.str('EXECUTOR_IMAGE', default='backenddevelopersltd/compute-horde-executor:v0-latest')
DEBUG_SKIP_PULLING_EXECUTOR_IMAGE = env.bool('DEBUG_SKIP_PULLING_EXECUTOR_IMAGE', default=False)
ADDRESS_FOR_EXECUTORS = env.str('ADDRESS_FOR_EXECUTORS', default='')
PORT_FOR_EXECUTORS = env.int('PORT_FOR_EXECUTORS')

BITTENSOR_MINER_PORT = env.int('BITTENSOR_MINER_PORT')

BITTENSOR_MINER_ADDRESS = env.str('BITTENSOR_MINER_ADDRESS', default='auto')
BITTENSOR_MINER_ADDRESS_IS_AUTO = BITTENSOR_MINER_ADDRESS == 'auto'
if not BITTENSOR_MINER_ADDRESS_IS_AUTO:
    try:
        ipaddress.ip_address(BITTENSOR_MINER_ADDRESS)
    except ValueError:
        raise RuntimeError('The BITTENSOR_MINER_ADDRESS is not a valid IP address')

BITTENSOR_NETUID = env.int('BITTENSOR_NETUID')
BITTENSOR_NETWORK = env.str('BITTENSOR_NETWORK')

BITTENSOR_WALLET_DIRECTORY = env.path(
    'BITTENSOR_WALLET_DIRECTORY',
    default=pathlib.Path('~').expanduser() / '.bittensor' / 'wallets',
)
BITTENSOR_WALLET_NAME = env.str('BITTENSOR_WALLET_NAME')
BITTENSOR_WALLET_HOTKEY_NAME = env.str('BITTENSOR_WALLET_HOTKEY_NAME')
DEBUG_TURN_AUTHENTICATION_OFF = env.bool('DEBUG_TURN_AUTHENTICATION_OFF', default=False)


def BITTENSOR_WALLET():
    if not BITTENSOR_WALLET_NAME or not BITTENSOR_WALLET_HOTKEY_NAME:
        raise RuntimeError('Wallet not configured')
    wallet = bittensor.wallet(name=BITTENSOR_WALLET_NAME, hotkey=BITTENSOR_WALLET_HOTKEY_NAME,
                              path=str(BITTENSOR_WALLET_DIRECTORY))
    wallet.hotkey_file.get_keypair()  # this raises errors if the keys are inaccessible
    return wallet


CHANNEL_LAYERS = {
    "default": {
        "BACKEND": "compute_horde_miner.channel_layer.channel_layer.ECRedisChannelLayer",
        "CONFIG": {
            "hosts": [(env.str('REDIS_HOST', default='redis'), env.int('REDIS_PORT', default='6379'))],
        },
    },
}

# Sentry
if SENTRY_DSN := env('SENTRY_DSN', default=''):
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
