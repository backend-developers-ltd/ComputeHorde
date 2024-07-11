import logging
import sys

import bittensor
import uvloop
from compute_horde.executor_class import DEFAULT_EXECUTOR_CLASS
from django.core.management.base import BaseCommand

from compute_horde_validator.validator.models import Miner
from compute_horde_validator.validator.synthetic_jobs.utils import execute_synthetic_batch

logger = logging.getLogger(__name__)


class Command(BaseCommand):
    def add_arguments(self, parser):
        parser.add_argument("--miner_hotkey", type=str, help="Miner hotkey", required=True)
        parser.add_argument(
            "--miner_address", type=str, help="Miner IPv4 address", default="127.0.0.1"
        )
        parser.add_argument("--miner_port", type=int, help="Miner port", default=8000)
        parser.add_argument(
            "--executor_class", type=str, help="Executor class", default=DEFAULT_EXECUTOR_CLASS
        )

    def handle(self, *args, **options):
        uvloop.install()
        miner_hotkey = options["miner_hotkey"]
        miner_address = options["miner_address"]
        miner_port = options["miner_port"]
        # TODO
        # executor_class = options["executor_class"]
        miners = [Miner.objects.get_or_create(hotkey=miner_hotkey)[0]]
        axons_by_key = {
            miner_hotkey: bittensor.AxonInfo(
                version=4,
                ip=miner_address,
                ip_type=4,
                port=miner_port,
                hotkey=miner_hotkey,
                coldkey=miner_hotkey,
            )
        }
        try:
            execute_synthetic_batch(axons_by_key, miners)
        except KeyboardInterrupt:
            print("Interrupted by user")
            sys.exit(1)
        except Exception:
            logger.warning("command failed with exception", exc_info=True)
            sys.exit(1)
