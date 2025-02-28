# import multiprocessing
import os

from prometheus_client import multiprocess

workers = 4  # 2 * multiprocessing.cpu_count() + 1
bind = "0.0.0.0:8000"
wsgi_app = "project.asgi:application"
access_logfile = "-"
# Bittensor library uses nest_asyncio which requires loop
# to be of type `asyncio`; see following issue as reference:
#  https://github.com/guidance-ai/guidance/issues/184)
worker_class = "project.workers.UvicornAsyncioWorker"
# worker_class = "uvicorn.workers.UvicornWorker"
timeout = os.environ.get("GUNICORN_TIMEOUT", 300)


def child_exit(server, worker):
    multiprocess.mark_process_dead(worker.pid)
