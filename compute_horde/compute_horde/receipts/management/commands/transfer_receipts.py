import logging
import time
from typing import cast

import bittensor
from asgiref.sync import async_to_sync
from django.conf import settings
from django.core.management import BaseCommand

from compute_horde.receipts.transfer import ReceiptsTransfer

logger = logging.getLogger(__name__)


class Command(BaseCommand):
    help = "Fetch receipts from miners"

    def add_arguments(self, parser):
        parser.add_argument("--daemon", "-d", default=False, action='store_true')
        parser.add_argument("--miner-hotkey", type=str)
        parser.add_argument("--miner-ip", type=str)
        parser.add_argument("--miner-port", type=int)

    def handle(self, daemon: bool, miner_hotkey: str, miner_ip: str, miner_port: int, **kwargs):
        if (miner_hotkey, miner_ip, miner_port) != (None, None, None):
            miners = [(miner_hotkey, miner_ip, miner_port)]
            logger.info(f"Will fetch receipts from explicit miner: {miners[0]}")

        elif settings.DEBUG_FETCH_RECEIPTS_FROM_MINERS:
            # 1st, if debug miners are specified, they take precedence.
            miners = settings.DEBUG_FETCH_RECEIPTS_FROM_MINERS
            logger.info(f"Will fetch receipts from {len(miners)} debug miners")

        else:
            # 2nd, if no specific miners were specified, get from metagraph.
            metagraph = bittensor.metagraph(
                netuid=settings.BITTENSOR_NETUID, network=settings.BITTENSOR_NETWORK
            )
            miners = [
                (
                    cast(str, neuron.hotkey),
                    cast(str, neuron.axon_info.ip),
                    cast(int, neuron.axon_info.port),
                )
                for neuron in metagraph.neurons
                if neuron.axon_info.is_serving
            ]
            logger.info(f"Will fetch receipts from {len(miners)} metagraph miners")

        while True:
            # TODO: Miners on metagraph may change
            async_to_sync(ReceiptsTransfer.transfer_from)(miners)
            if not daemon:
                break
            time.sleep(1)
