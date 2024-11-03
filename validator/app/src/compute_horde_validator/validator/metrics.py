import glob
import os
from collections.abc import Iterator

import prometheus_client
from django.http import HttpResponse
from django_prometheus.exports import ExportToDjangoView
from prometheus_client import multiprocess
from prometheus_client.core import REGISTRY, GaugeMetricFamily, Metric
from prometheus_client.registry import Collector

from ..celery import get_num_tasks_in_queue, CELERY_TASK_QUEUES


class RecursiveMultiProcessCollector(multiprocess.MultiProcessCollector):
    """A multiprocess collector that scans the directory recursively"""

    def collect(self):
        files = glob.glob(os.path.join(self._path, "**/*.db"), recursive=True)
        return self.merge(files, accumulate=True)


ENV_VAR_NAME = "PROMETHEUS_MULTIPROC_DIR"


def metrics_view(request):
    """Exports metrics as a Django view"""
    if os.environ.get(ENV_VAR_NAME):
        registry = prometheus_client.CollectorRegistry()
        RecursiveMultiProcessCollector(registry)
        return HttpResponse(
            prometheus_client.generate_latest(registry),
            content_type=prometheus_client.CONTENT_TYPE_LATEST,
        )
    else:
        return ExportToDjangoView(request)


class CustomCeleryCollector(Collector):
    def collect(self) -> Iterator[Metric]:
        num_tasks_in_queue = GaugeMetricFamily(
            "celery_queue_len",
            "How many tasks are there in a queue",
            labels=("queue",),
        )
        for queue in CELERY_TASK_QUEUES:
            num_tasks_in_queue.add_metric([queue], get_num_tasks_in_queue(queue))
        yield num_tasks_in_queue


REGISTRY.register(CustomCeleryCollector())
