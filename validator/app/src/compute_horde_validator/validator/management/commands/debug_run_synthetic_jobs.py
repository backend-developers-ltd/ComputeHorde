import sys

from django.conf import settings
from django.core.management.base import BaseCommand

from compute_horde_validator.validator.synthetic_jobs.utils import (
    create_and_run_sythethic_job_batch,
)


class Command(BaseCommand):
    """
    For running in dev environment, not in production
    """

    def handle(self, *args, **options):
        try:
            create_and_run_sythethic_job_batch(
                settings.BITTENSOR_NETUID, settings.BITTENSOR_NETWORK
            )
        except KeyboardInterrupt:
            print("Interrupted by user")
            sys.exit(1)
