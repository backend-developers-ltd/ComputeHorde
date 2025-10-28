import asyncio
import logging
import signal

from asgiref.sync import async_to_sync
from compute_horde.transport import WSTransport
from django.conf import settings
from django.core.management.base import BaseCommand

from compute_horde_validator.validator.organic_jobs.facilitator_client.facilitator_connector import (
    FacilitatorClient,
)

logger = logging.getLogger(__name__)


class Command(BaseCommand):
    FACILITATOR_CLIENT_CLASS = FacilitatorClient
    TRANSPORT_LAYER_CLASS = WSTransport
    STOP_EVENT = asyncio.Event()

    def __init__(self):
        for sig in (signal.SIGINT, signal.SIGTERM):
            signal.signal(sig, self.shutdown)

    @async_to_sync
    async def handle(self, *args, **options):
        keypair = settings.BITTENSOR_WALLET().get_hotkey()
        logger.info(
            f"Connecting to facilitator at {settings.FACILITATOR_URI} and "
            "starting connection manager and message manager"
        )

        transport_layer = self.TRANSPORT_LAYER_CLASS(
            name="facilitator", url=settings.FACILITATOR_URI
        )
        facilitator_client = self.FACILITATOR_CLIENT_CLASS(
            keypair=keypair, transport_layer=transport_layer
        )

        async def lifecycle():
            self.STOP_EVENT.clear()
            await facilitator_client.start()
            await self.STOP_EVENT.wait()
            await facilitator_client.stop()

        task = asyncio.create_task(lifecycle())
        await task

    def shutdown(self, *args):
        """Set global stop event to trigger the shutdown of the components."""
        if not self.STOP_EVENT.is_set():
            self.STOP_EVENT.set()
