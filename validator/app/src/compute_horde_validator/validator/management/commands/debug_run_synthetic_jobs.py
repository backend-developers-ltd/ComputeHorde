import logging
import sys

from django.conf import settings
from django.core.management.base import BaseCommand

from compute_horde_validator.validator.models import SyntheticJobBatch
from compute_horde_validator.validator.synthetic_jobs.utils import (
    run_synthethic_job_batch,
)

logger = logging.getLogger(__name__)


class Command(BaseCommand):
    """
    For running in dev environment, not in production
    """

    def handle(self, *args, **options):
        batch = SyntheticJobBatch.objects.create()
        try:
            run_synthethic_job_batch(
                batch=batch,
                netuid=settings.BITTENSOR_NETUID,
                network=settings.BITTENSOR_NETWORK,
            )
        except KeyboardInterrupt:
            print("Interrupted by user")
            sys.exit(1)
        except Exception:
            logger.warning("command failed with exception", exc_info=True)
