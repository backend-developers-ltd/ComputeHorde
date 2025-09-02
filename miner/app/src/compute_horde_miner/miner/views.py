import os

from django.http import JsonResponse

from compute_horde_miner.miner.executor_manager import current


def get_version(request):
    miner_version = os.environ.get("MINER_VERSION", "unknown")
    runner_version = os.environ.get("MINER_RUNNER_VERSION", "unknown")
    return JsonResponse({"miner_version": miner_version, "runner_version": runner_version})


async def get_main_hotkey(request):
    """HTTP endpoint to get the miner's main hotkey."""
    try:
        main_hotkey = await current.executor_manager.get_main_hotkey()
        return JsonResponse({"main_hotkey": main_hotkey})
    except Exception:
        return JsonResponse({"error": "Could not get main hotkey"}, status=500)


async def get_manifest(request):
    """HTTP endpoint to get the miner's executor manifest."""
    try:
        manifest = await current.executor_manager.get_manifest()
        return JsonResponse({"manifest": manifest})
    except Exception:
        return JsonResponse({"error": "Could not get manifest"}, status=500)
