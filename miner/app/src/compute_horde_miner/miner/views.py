import datetime
import os

from django.http import JsonResponse, HttpResponseRedirect, HttpResponseBadRequest
from django.utils.timezone import now

from compute_horde_miner.miner.receipt_store.current import receipts_store


def get_version(request):
    miner_version = os.environ.get('MINER_VERSION', 'unknown')
    runner_version = os.environ.get('MINER_RUNNER_VERSION', 'unknown')
    return JsonResponse({'miner_version': miner_version, 'runner_version': runner_version})


async def get_receipts(request):
    if date_str := request.GET.get('date'):
        try:
            date = datetime.date.fromisoformat(date_str)
        except ValueError:
            return HttpResponseBadRequest()
    else:
        # yesterday
        date = now().date() - datetime.timedelta(days=1)

    url = await receipts_store.get_url(date)
    return HttpResponseRedirect(redirect_to=url)
