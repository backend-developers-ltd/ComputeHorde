import asyncio
from concurrent.futures import ThreadPoolExecutor
from typing import TYPE_CHECKING

import wandb
from asgiref.sync import AsyncToSync, SyncToAsync
from constance import config
from django.conf import settings
from structlog import get_logger

if TYPE_CHECKING:
    from bittensor.chain_data import NeuronInfo


log = get_logger(__name__)


def get_thread_sensitive_executor(loop):
    """Returns executor to run sync code in as if sync code were wrapped with sync_to_async

    This is exactly copied implementation from SyncToAsync but instead of running code in thread
    and awaiting in the loop we return executor in which SyncToAsync would run sync code
    and we allow to pass function directly allowing for blocking waiting for future result.

    TODO: handling deadlock is not implemented so it can be unsafe in some conditions
    """
    current_thread_executor = getattr(AsyncToSync.executors, "current", None)
    if current_thread_executor:
        # If we have a parent sync thread above somewhere, use that
        executor = current_thread_executor
    elif SyncToAsync.thread_sensitive_context.get(None):
        # If we have a way of retrieving the current context, attempt
        # to use a per-context thread pool executor
        thread_sensitive_context = SyncToAsync.thread_sensitive_context.get()

        if thread_sensitive_context in SyncToAsync.context_to_thread_executor:
            # Re-use thread executor in current context
            executor = SyncToAsync.context_to_thread_executor[thread_sensitive_context]
        else:
            # Create new thread executor in current context
            executor = ThreadPoolExecutor(max_workers=1)
    elif loop in AsyncToSync.loop_thread_executors:
        # Re-use thread executor for running loop
        executor = AsyncToSync.loop_thread_executors[loop]
    elif SyncToAsync.deadlock_context.get(False):
        raise RuntimeError("Single thread executor already being used, would deadlock")
    else:
        # Otherwise, we run it in a fixed single thread
        executor = SyncToAsync.single_thread_executor
        # TODO: we don't set deadlock here - maybe we should clean it all up
        # SyncToAsync.deadlock_context.set(True)
    return executor


def safe_sync(fun, *args, **kwargs):
    """Runs sensitive sync function in sync or async context

    When running in sync context it is the same as if it was called directly.
    In async context function is run as if it would be run called using sync_to_async
    but it blocks async context when waiting for future result. This allows writing
    sync sensitive functions that can be run in async context without need to write
    async version of a function that allows await, eg. istead of writing two versions
    one using obj.save() and second await obj.asave() we can write one function
    with safe_sync(obj.save)
    """
    try:
        # Try to get the event loop. If we're in an async context, this will succeed.
        loop = asyncio.get_running_loop()
    except RuntimeError:
        loop = None
    if loop is not None:
        executor = get_thread_sensitive_executor(loop)
        future = executor.submit(fun, *args, **kwargs)
        return future.result()
    else:
        # We're in a synchronous context without an event loop
        return fun(*args, **kwargs)


class SafeConfig:
    def __getattr__(self, name):
        return safe_sync(getattr, config, name)


safe_config = SafeConfig()


def is_validator(neuron: "NeuronInfo") -> bool:
    if our_validator_address := safe_config.OUR_VALIDATOR_SS58_ADDRESS:
        if neuron.hotkey == our_validator_address:
            return True

    return neuron.stake > 0


def fetch_compute_subnet_hardware() -> dict:
    """
    Retrieve hardware specs for the compute subnet.

    This info is also displayed here: https://opencompute.streamlit.app
    """

    wandb.login(key=settings.WANDB_API_KEY)
    api = wandb.Api()

    # https://github.com/nauttiilus/opencompute/blob/main/main.py
    db_specs_dict = {}
    project_path = "neuralinternet/opencompute"
    runs = api.runs(project_path)
    for run in runs:
        run_config = run.config
        hotkey = run_config.get("hotkey")
        details = run_config.get("specs")
        role = run_config.get("role")
        if hotkey and details and role == "miner":
            db_specs_dict[hotkey] = details

    return db_specs_dict
