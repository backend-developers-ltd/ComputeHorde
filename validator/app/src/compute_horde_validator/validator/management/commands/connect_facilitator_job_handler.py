import asyncio
import logging
import signal

from asgiref.sync import async_to_sync
from django.core.management.base import BaseCommand

from compute_horde_validator.validator.organic_jobs.facilitator_client.job_request_manager import (
    JobRequestManager,
)

logger = logging.getLogger(__name__)


class Command(BaseCommand):
    JOB_HANDLER_CLASS = JobRequestManager
    STOP_EVENT = asyncio.Event()

    def __init__(self):
        for sig in (signal.SIGINT, signal.SIGTERM):
            signal.signal(sig, self.shutdown)

    @async_to_sync
    async def handle(self, *args, **options):
        logger.info("Starting facilitator client job handler")

        job_handler = self.JOB_HANDLER_CLASS()

        async def lifecycle():
            self.STOP_EVENT.clear()
            await job_handler.start()
            await self.STOP_EVENT.wait()
            await job_handler.stop()

        task = asyncio.create_task(lifecycle())
        await task

    def shutdown(self, *args):
        """Set global stop event to trigger the shutdown of the components."""
        if not self.STOP_EVENT.is_set():
            self.STOP_EVENT.set()
