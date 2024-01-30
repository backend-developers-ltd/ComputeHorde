import asyncio
import logging

from django.conf import settings
from django.core.management.base import BaseCommand

from compute_horde_validator.validator.synthetic_jobs.utils import initiate_jobs, execute_jobs

logger = logging.getLogger(__name__)


class Command(BaseCommand):
    """
    For running in dev environment, not in production
    """

    def handle(self, *args, **options):
        jobs = initiate_jobs(settings.BITTENSOR_NETUID, settings.BITTENSOR_NETWORK)
        if not jobs:
            logger.info('Nothing to do')
            return
        asyncio.run(execute_jobs(jobs))
