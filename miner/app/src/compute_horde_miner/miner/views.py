import os

from django.http import HttpResponseRedirect, JsonResponse

from compute_horde_miner.miner.receipt_store.current import receipts_store


def get_version(request):
    miner_version = os.environ.get("MINER_VERSION", "unknown")
    runner_version = os.environ.get("MINER_RUNNER_VERSION", "unknown")
    return JsonResponse({"miner_version": miner_version, "runner_version": runner_version})


def get_receipts(request):
    url = receipts_store.get_url()
    return HttpResponseRedirect(redirect_to=url)
