import logging
import sys

import bittensor
from django.conf import settings
from django.core.management.base import BaseCommand

from compute_horde_validator.validator.models import Cycle, SyntheticJobBatch
from compute_horde_validator.validator.synthetic_jobs.utils import (
    create_and_run_synthetic_job_batch,
)

logger = logging.getLogger(__name__)


class Command(BaseCommand):
    """
    For running in dev environment, not in production
    """

    def handle(self, *args, **options):
        try:
            subtensor = bittensor.subtensor(network=settings.BITTENSOR_NETWORK)
            current_block = subtensor.get_current_block()
            cycle = Cycle.from_block(current_block, settings.BITTENSOR_NETUID)
            batch = SyntheticJobBatch.objects.create(
                block=current_block,
                cycle=cycle,
                should_be_scored=False,
            )
            create_and_run_synthetic_job_batch(
                settings.BITTENSOR_NETUID, settings.BITTENSOR_NETWORK, batch.id
            )
        except KeyboardInterrupt:
            print("Interrupted by user")
            sys.exit(1)
        except Exception:
            logger.warning("command failed with exception", exc_info=True)
