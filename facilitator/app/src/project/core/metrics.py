import glob
import os

import prometheus_client
from django.http import HttpResponse
from django_prometheus.exports import ExportToDjangoView
from prometheus_client import multiprocess


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
