import logging

from asgiref.sync import async_to_sync
from django.conf import settings
from django.core.management.base import BaseCommand

from compute_horde_validator.validator.organic_jobs.facilitator_client import FacilitatorClient

logger = logging.getLogger(__name__)


class Command(BaseCommand):
    FACILITATOR_CLIENT_CLASS = FacilitatorClient

    @async_to_sync
    async def handle(self, *args, **options):
        keypair = settings.BITTENSOR_WALLET().get_hotkey()
        facilitator_client = self.FACILITATOR_CLIENT_CLASS(keypair, settings.FACILITATOR_URI)
        async with facilitator_client:
            await facilitator_client.run_forever()
