import asyncio
import logging
import time
from collections.abc import Awaitable, Callable, Sequence
from datetime import datetime, timedelta
from typing import cast

import aiohttp
import bittensor
from asgiref.sync import async_to_sync
from compute_horde.receipts.store.local import LocalFilesystemPagedReceiptStore
from compute_horde.receipts.transfer import MinerInfo, ReceiptsTransfer
from django.conf import settings
from django.core.management import BaseCommand
from django.utils import timezone

logger = logging.getLogger(__name__)


class Command(BaseCommand):
    help = "Fetch receipts from miners"

    def add_arguments(self, parser):
        parser.add_argument("--miner-hotkey", type=str)
        parser.add_argument("--miner-ip", type=str)
        parser.add_argument("--miner-port", type=int)
        parser.add_argument(
            "--interval",
            type=float,
            help="If provided, runs in daemon mode and polls for changes every `interval` seconds.",
        )

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
            logger.info("Will fetch receipts from metagraph miners")

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

        # This encompasses at least current and previous cycle.
        cutoff = timezone.now() - timedelta(hours=5)

        if interval is None:
            await self.run_once(cutoff, miners)
        else:
            await self.run_in_loop(interval, cutoff, miners)

    async def run_once(self, cutoff: datetime, miners: Callable[[], Awaitable[list[MinerInfo]]]):
        """
        Do a one time fetch of all pages
        """
        catchup_cutoff_page = LocalFilesystemPagedReceiptStore.current_page_at(cutoff)
        current_page = LocalFilesystemPagedReceiptStore.current_page()
        async with aiohttp.ClientSession() as session:
            await self.catch_up(
                # Pull all pages from latest to oldest
                pages=list(reversed(range(catchup_cutoff_page, current_page + 1))),
                miners=miners,
                session=session,
                semaphore=asyncio.Semaphore(50),
            )

    async def run_in_loop(
        self, interval: float, cutoff: datetime, miners: Callable[[], Awaitable[list[MinerInfo]]]
    ):
        """
        Do a full catch-up while listening for changes in latest 2 pages.
        """
        catchup_cutoff_page = LocalFilesystemPagedReceiptStore.current_page_at(cutoff)
        current_page = LocalFilesystemPagedReceiptStore.current_page()

        # Reuse the session between all tasks so that we can keep the TCP connections up
        async with aiohttp.ClientSession() as session:
            # First, quickly catch-up with the 2 latest pages so that the "keep up" loop has easier time later
            await self.catch_up(
                pages=[current_page, current_page - 1],
                miners=miners,
                session=session,
                semaphore=asyncio.Semaphore(50),
            )
            await asyncio.gather(
                # Slowly catch up with pages older than 2, latest page first
                self.catch_up(
                    pages=list(reversed(range(catchup_cutoff_page, current_page - 1))),
                    miners=miners,
                    session=session,
                    # Throttle this lower so that it doesn't choke the keep up loop
                    semaphore=asyncio.Semaphore(10),
                ),
                # ... and keep up with latest 2 pages continuously in parallel
                self.keep_up(
                    n_pages=2,
                    interval=interval,
                    miners=miners,
                    session=session,
                    semaphore=asyncio.Semaphore(50),
                ),
            )

    async def catch_up(
        self,
        pages: Sequence[int],
        miners: Callable[[], Awaitable[list[MinerInfo]]],
        session: aiohttp.ClientSession,
        semaphore: asyncio.Semaphore,
    ):
        """
        Fetches changes in given pages one by one.
        """
        for idx, page in enumerate(pages):
            start_time = time.monotonic()
            receipts, exceptions = await ReceiptsTransfer.transfer(
                miners=await miners(),
                pages=[page],
                session=session,
                semaphore=semaphore,
                request_timeout=3.0,
            )
            elapsed = time.monotonic() - start_time
            rps = receipts / elapsed

            logger.info(
                f"Catching up: "
                f"{page=} ({idx + 1}/{len(pages)}) "
                f"{elapsed=:.3f} "
                f"{rps=:.0f} "
                f"{receipts=} "
                f"{exceptions=} "
            )

    async def keep_up(
        self,
        n_pages: int,
        interval: float | None,
        miners: Callable[[], Awaitable[list[MinerInfo]]],
        session: aiohttp.ClientSession,
        semaphore: asyncio.Semaphore,
    ):
        """
        Runs indefinitely and polls for changes in last `n_pages` every `interval`.
        """
        while True:
            start_time = time.monotonic()
            """
            Considerations:
            - page request timeout may be influenced by some heavy async task
            - too many concurrent downloads may take a lot of bandwidth
            """
            latest_page = LocalFilesystemPagedReceiptStore.current_page()
            pages = list(reversed(range(latest_page - n_pages + 1, latest_page + 1)))
            receipts, exceptions = await ReceiptsTransfer.transfer(
                miners=await miners(),
                pages=pages,
                session=session,
                semaphore=semaphore,
                request_timeout=1.0,
            )
            elapsed = time.monotonic() - start_time
            rps = receipts / elapsed

            logger.info(
                f"Keeping up: "
                f"{pages=} "
                f"{elapsed=:.3f} "
                f"{rps=:.0f} "
                f"{receipts=} "
                f"{exceptions=} "
            )

            # Sleep for the remainder of the time if any
            if elapsed < interval:
                time.sleep(interval - elapsed)
