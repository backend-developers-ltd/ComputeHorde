import multiprocessing

from prometheus_client import multiprocess

workers = 2 * multiprocessing.cpu_count() + 1
bind = "0.0.0.0:8000"
wsgi_app = "compute_horde_executor.wsgi:application"
access_logfile = "-"


def child_exit(server, worker):
    multiprocess.mark_process_dead(worker.pid)
