import importlib
import logging
import os
from pathlib import Path

from celery import Celery, bootsteps, signals
from celery.signals import worker_process_shutdown
from django.conf import settings
from prometheus_client import multiprocess

logger = logging.getLogger(__name__)

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "compute_horde_validator.settings")

app = Celery("compute_horde_validator")
app.config_from_object("django.conf:settings", namespace="CELERY")
app.autodiscover_tasks(lambda: settings.INSTALLED_APPS)

DEFAULT_QUEUE = "default"

TASK_QUEUE_MAP = {
    # Jobs
    "compute_horde_validator.validator.tasks.check_missed_synthetic_jobs": "jobs",
    "compute_horde_validator.validator.tasks._run_synthetic_jobs": "jobs",
    "compute_horde_validator.validator.tasks.run_synthetic_jobs": "jobs",
    "compute_horde_validator.validator.tasks.schedule_synthetic_jobs": "jobs",
    # LLM job tasks
    "compute_horde_validator.validator.tasks.llm_prompt_generation": "llm",
    "compute_horde_validator.validator.tasks.llm_prompt_sampling": "llm",
    "compute_horde_validator.validator.tasks.llm_prompt_answering": "llm",
    # Scores/weights
    "compute_horde_validator.validator.tasks.reveal_scores": "weights",
    "compute_horde_validator.validator.tasks.set_scores": "weights",
    "compute_horde_validator.validator.tasks.do_set_weights": "weights",
    # Receipts
    "compute_horde_validator.validator.tasks.fetch_receipts": "receipts",
    "compute_horde_validator.validator.tasks.fetch_receipts_from_miner": "receipts",
    # Misc
    "compute_horde_validator.validator.tasks.send_events_to_facilitator": DEFAULT_QUEUE,
    "compute_horde_validator.validator.tasks.fetch_dynamic_config": DEFAULT_QUEUE,
}

CELERY_TASK_QUEUES = list(set(TASK_QUEUE_MAP.values()))

WORKER_HEALTHCHECK_FILE = Path(settings.WORKER_HEALTHCHECK_FILE_PATH)


def route_task(name, args, kwargs, options, task=None, **kw):
    if name not in TASK_QUEUE_MAP:
        logger.warning("Celery task %s is not mapped to any queue", name)
    return {"queue": TASK_QUEUE_MAP.get(name, DEFAULT_QUEUE)}


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


def get_num_tasks_in_queue(queue_name: str) -> int:
    with app.pool.acquire(block=True) as conn:  # type: ignore
        try:
            return int(conn.default_channel.client.llen(queue_name))
        except (TypeError, ValueError, ConnectionError):
            return 0


# Worker healthcheck
# Taken from https://github.com/celery/celery/issues/4079#issuecomment-1128954283
class LivenessProbe(bootsteps.StartStopStep):
    requires = ("celery.worker.components:Timer",)

    def __init__(self, worker, **kwargs):
        super().__init__(worker, **kwargs)
        self.tref = None

    def start(self, worker):
        # Create the parent directory if it doesn't exist
        WORKER_HEALTHCHECK_FILE.parent.mkdir(parents=True, exist_ok=True)
        # Ensure the file exists
        WORKER_HEALTHCHECK_FILE.touch()
        self.tref = worker.timer.call_repeatedly(
            10.0,
            self.update_heartbeat_file,
            (worker,),
            priority=10,
        )

    def stop(self, worker):
        WORKER_HEALTHCHECK_FILE.unlink(missing_ok=True)

    def update_heartbeat_file(self, worker):
        WORKER_HEALTHCHECK_FILE.touch()


app.steps["worker"].add(LivenessProbe)
