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
        parser.add_argument("--miner-hotkey", type=str)
        parser.add_argument("--miner-ip", type=str)
        parser.add_argument("--miner-port", type=int)
        parser.add_argument("--interval", type=float)

    @async_to_sync
    async def handle(
        self,
        interval: float | None,
        miner_hotkey: str | None,
        miner_ip: str | None,
        miner_port: int | None,
        **kwargs,
    ):
        if interval is not None and interval < 1:
            logger.warning("Running with interval < 1 may significantly impact performance.")

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

        # TODO: Catch up last 2 cycles of receipts
        await self.do_transfer(interval, miners)

    async def do_transfer(self, interval: float | None, miners: Callable[[], Awaitable[list[MinerInfo]]]):
        async with aiohttp.ClientSession() as session:
            while True:
                start_time = time.monotonic()
                """
                Considerations:
                - page request timeout may be influenced by some heavy async task
                - too many concurrent downloads may take a lot of bandwidth
                """
                total_receipts, total_exceptions = await ReceiptsTransfer.transfer(
                    miners=await miners(),
                    session=session,
                    concurrency=50,
                    max_time_per_miner_page=1.0,
                    batch_insert_size=1000,
                )
                elapsed = time.monotonic() - start_time
                rps = total_receipts / elapsed

                logger.info(
                    f"{total_receipts=} "
                    f"{elapsed=:.3f} "
                    f"{rps=:.0f} "
                    f"{total_exceptions=} "
                )

                if interval is None:
                    break

                # Sleep for the remainder of the time if any
                if elapsed < interval:
                    time.sleep(interval - elapsed)
