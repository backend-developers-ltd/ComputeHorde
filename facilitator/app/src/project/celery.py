import logging
import os

import structlog
from celery import Celery
from celery.signals import setup_logging, worker_process_shutdown
from django.conf import settings
from django_structlog.celery.steps import DjangoStructLogInitStep
from prometheus_client import multiprocess

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "project.settings")

app = Celery("project")

app.config_from_object("django.conf:settings", namespace="CELERY")
app.steps["worker"].add(DjangoStructLogInitStep)
app.autodiscover_tasks(lambda: settings.INSTALLED_APPS)


def route_task(name, args, kwargs, options, task=None, **kw):
    return {"queue": "celery"}


@worker_process_shutdown.connect
def child_exit(pid, **kw):
    multiprocess.mark_process_dead(pid)


@setup_logging.connect
def receiver_setup_logging(loglevel, logfile, format, colorize, **kwargs):  # pragma: no cover
    logging.config.dictConfig(
        {
            "version": 1,
            "disable_existing_loggers": False,
            "formatters": {
                "console": {
                    "()": structlog.stdlib.ProcessorFormatter,
                    "processor": structlog.dev.ConsoleRenderer(),
                },
                "file": {
                    "()": structlog.stdlib.ProcessorFormatter,
                    "processor": structlog.processors.JSONRenderer(),
                },
            },
            "handlers": {
                "console": {
                    "class": "logging.StreamHandler",
                    "formatter": "console",
                },
                "file": {
                    "class": "logging.FileHandler",
                    "filename": logfile,  # Use the logfile parameter here
                    "formatter": "file",
                },
            },
            "loggers": {
                "": {  # Root logger
                    "handlers": ["console", "file"],
                    "level": loglevel,  # Use the loglevel parameter
                },
                "django": {
                    "handlers": ["console", "file"],
                    "level": "INFO",
                    "propagate": False,
                },
                "celery": {
                    "handlers": ["console", "file"],
                    "level": loglevel,  # Use the loglevel parameter
                    "propagate": False,
                },
            },
        }
    )

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
