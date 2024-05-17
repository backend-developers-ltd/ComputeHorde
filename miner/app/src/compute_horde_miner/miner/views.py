import os

from django.http import JsonResponse


def get_version(request):
    miner_version = os.environ.get("MINER_VERSION", "unknown")
    runner_version = os.environ.get("MINER_RUNNER_VERSION", "unknown")
    return JsonResponse({"miner_version": miner_version, "runner_version": runner_version})
