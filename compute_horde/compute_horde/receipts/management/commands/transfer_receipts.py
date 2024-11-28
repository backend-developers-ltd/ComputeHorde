import logging
import time
from statistics import mean
from typing import cast

import aiohttp
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

    @async_to_sync
    async def handle(self, daemon: bool, miner_hotkey: str, miner_ip: str, miner_port: int, **kwargs):
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

        samples_time = []
        samples_rps = []

        async with aiohttp.ClientSession() as session:
            while True:
                # TODO: Miners on metagraph may change
                start_time = time.monotonic()
                total = await ReceiptsTransfer.transfer(miners, session)
                elapsed = time.monotonic() - start_time
                rps = total / elapsed
                logger.info(f"Transferred {total} receipts in {elapsed:.3f}s at {rps:.0f} RPS")
                if not daemon:
                    break

                if total:
                    samples_time.append(elapsed)
                    samples_rps.append(rps)
                    mean_elapsed = mean(samples_time[1:]) if len(samples_time) > 1 else 0
                    mean_rps = mean(samples_rps[1:]) if len(samples_rps) > 1 else 0
                    logger.info(f"{mean_elapsed=:.3f} {mean_rps=:.0f}")
                    samples_time = samples_time[:100]
                    samples_rps = samples_rps[:100]

                time.sleep((1 - elapsed) if elapsed < 1 else 0)
