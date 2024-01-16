
from celery.utils.log import get_task_logger

from compute_horde_executor.celery import app

logger = get_task_logger(__name__)


@app.task
def demo_task(x, y):
    return x + y
