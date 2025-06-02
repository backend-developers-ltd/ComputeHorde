import logging
import sys

import turbobt
from asgiref.sync import async_to_sync
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
            current_block = async_to_sync(self._get_block_number)()
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

    async def _get_block_number(self):
        async with turbobt.Bittensor(settings.BITTENSOR_NETWORK) as bittensor:
            block = await bittensor.blocks.head()
            return block.number
