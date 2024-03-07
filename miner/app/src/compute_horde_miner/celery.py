import os

from celery import Celery
from celery.signals import worker_process_shutdown
from django.conf import settings
from prometheus_client import multiprocess

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "compute_horde_miner.settings")

app = Celery("compute_horde_miner")

app.config_from_object("django.conf:settings", namespace="CELERY")
app.autodiscover_tasks(lambda: settings.INSTALLED_APPS)


def route_task(name, args, kwargs, options, task=None, **kw):
    return {"queue": "celery"}


@worker_process_shutdown.connect
def child_exit(pid, **kw):
    multiprocess.mark_process_dead(pid)
