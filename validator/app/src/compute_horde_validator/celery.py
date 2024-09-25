import importlib
import os

from celery import Celery, signals
from celery.signals import worker_process_shutdown
from django.conf import settings
from prometheus_client import multiprocess

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "compute_horde_validator.settings")

app = Celery("compute_horde_validator")

app.config_from_object("django.conf:settings", namespace="CELERY")
app.autodiscover_tasks(lambda: settings.INSTALLED_APPS)


def route_task(name, args, kwargs, options, task=None, **kw):
    worker_queue_names = {
        "compute_horde_validator.validator.tasks.fetch_receipts_from_miner",
        "compute_horde_validator.validator.tasks.send_events_to_facilitator",
        "compute_horde_validator.validator.tasks.fetch_dynamic_config",
        # TODO: llm tasks should have dedicated workers, but just move them from default queue for now
        "compute_horde_validator.validator.tasks.llm_prompt_generation",
        "compute_horde_validator.validator.tasks.llm_prompt_sampling",
        "compute_horde_validator.validator.tasks.llm_prompt_answering",
    }
    if name in worker_queue_names:
        return {"queue": "worker"}
    return {"queue": "celery"}


@worker_process_shutdown.connect
def child_exit(pid, **kw):
    multiprocess.mark_process_dead(pid)


@signals.worker_process_init.connect
def apply_startup_hook(*args, **kwargs):
    print("Worker is ready. Bootstrapping...")
    hook_script_file = os.environ.get("DEBUG_CELERY_HOOK_SCRIPT_FILE")
    if hook_script_file:
        print("Loading startup hook: ", hook_script_file)
        importlib.import_module(hook_script_file)
    else:
        print("Not loading any startup hook")
