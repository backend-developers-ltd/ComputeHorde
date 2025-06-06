import logging
import sys

import turbobt
import uvloop
from asgiref.sync import async_to_sync
from django.conf import settings
from django.core.management.base import BaseCommand

from compute_horde_validator.validator.models import Cycle, Miner, SyntheticJobBatch
from compute_horde_validator.validator.synthetic_jobs.batch_run import execute_synthetic_batch_run

logger = logging.getLogger(__name__)


class Command(BaseCommand):
    def add_arguments(self, parser):
        parser.add_argument("--miner_hotkey", type=str, help="Miner hotkey", required=True)
        # TODO: mock miner with address, port, ip_type; use executor_class
        # parser.add_argument(
        #     "--miner_address", type=str, help="Miner IPv4 address", default="127.0.0.1"
        # )
        # parser.add_argument("--miner_port", type=int, help="Miner port", default=8000)
        # parser.add_argument(
        #     "--executor_class", type=str, help="Executor class", default=DEFAULT_EXECUTOR_CLASS
        # )

    def handle(self, *args, **options):
        uvloop.install()
        hotkey = options["miner_hotkey"]

        try:
            miner = Miner.objects.get(hotkey=hotkey)
        except Miner.DoesNotExist:
            print(f"Miner with hotkey {hotkey} not found")
            sys.exit(1)

        try:
            current_block = async_to_sync(self._get_block_number)()
            cycle = Cycle.from_block(current_block, settings.BITTENSOR_NETUID)
            batch = SyntheticJobBatch.objects.create(
                block=current_block,
                cycle=cycle,
                should_be_scored=False,
            )
            async_to_sync(execute_synthetic_batch_run)([miner], [], batch.id)
        except KeyboardInterrupt:
            print("Interrupted by user")
            sys.exit(1)
        except Exception:
            logger.warning("command failed with exception", exc_info=True)
            sys.exit(1)

    async def _get_block_number(self):
        async with turbobt.Bittensor(settings.BITTENSOR_NETWORK) as bittensor:
            block = await bittensor.blocks.head()
            return block.number
