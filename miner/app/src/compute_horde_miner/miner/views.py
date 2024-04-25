import os

from django.http import JsonResponse


def get_version(request):
    version = os.environ.get('MINER_VERSION', 'unknown')
    return JsonResponse({'version': version})
