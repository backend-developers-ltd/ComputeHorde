import logging
import time
from typing import cast, Callable, Awaitable

import aiohttp
import bittensor
from asgiref.sync import async_to_sync
from django.conf import settings
from django.core.management import BaseCommand

from compute_horde.receipts.transfer import ReceiptsTransfer, MinerInfo

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
            # 1st, use explicitly specified miner if available
            if None in {miner_hotkey, miner_ip, miner_port}:
                raise ValueError("Either none or all of explicit miner details must be provided")
            miner = [miner_hotkey, miner_ip, miner_port]
            logger.info(f"Will fetch receipts from explicit miner: {miner}")

            async def miners():
                return [miner]

        elif settings.DEBUG_FETCH_RECEIPTS_FROM_MINERS:
            # 2nd, if debug miners are specified, they take precedence.
            debug_miners = settings.DEBUG_FETCH_RECEIPTS_FROM_MINERS
            logger.info(f"Will fetch receipts from {len(debug_miners)} debug miners")

            async def miners():
                return debug_miners

        else:
            # 3rd, if no specific miners were specified, get from metagraph.
            logger.info(f"Will fetch receipts from metagraph miners")

            async def miners():
                metagraph = bittensor.metagraph(
                    netuid=settings.BITTENSOR_NETUID, network=settings.BITTENSOR_NETWORK
                )
                return [
                    (
                        cast(str, neuron.hotkey),
                        cast(str, neuron.axon_info.ip),
                        cast(int, neuron.axon_info.port),
                    )
                    for neuron in metagraph.neurons
                    if neuron.axon_info.is_serving
                ]

        await self.do_transfer(daemon, miners)

    async def do_transfer(self, daemon: bool, miners: Callable[[], Awaitable[list[MinerInfo]]]):
        async with aiohttp.ClientSession() as session:
            while True:
                start_time = time.monotonic()
                """
                Considerations:
                - page request timeout may be influenced by some heavy async task in between like db write
                """
                total_receipts, total_exceptions = await ReceiptsTransfer.transfer(
                    miners=await miners(),
                    session=session,
                    concurrency=1000,
                    max_time_per_miner_page=3.0,
                    batch_insert_size=100,
                )
                elapsed = time.monotonic() - start_time
                rps = total_receipts / elapsed

                logger.info(
                    f"Transferred {total_receipts} receipts "
                    f"in {elapsed:.3f}s "
                    f"at {rps:.0f} RPS "
                    f"and got {total_exceptions} exceptions"
                )

                if not daemon:
                    break

                time.sleep((1 - elapsed) if elapsed < 1 else 0)
