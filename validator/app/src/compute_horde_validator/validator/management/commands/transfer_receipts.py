import asyncio
import logging
import time
from collections import defaultdict
from collections.abc import Awaitable, Callable, Sequence
from datetime import datetime, timedelta
from typing import cast

import aiohttp
import bittensor
from asgiref.sync import async_to_sync
from compute_horde.receipts.store.local import N_ACTIVE_PAGES, LocalFilesystemPagedReceiptStore
from compute_horde.receipts.transfer import (
    MinerInfo,
    ReceiptsTransfer,
    TransferMustBeEnabledException,
    TransferResult,
)
from django.conf import settings
from django.core.management import BaseCommand
from django.utils import timezone
from prometheus_client import Counter, Gauge, Histogram

logger = logging.getLogger(__name__)


class Command(BaseCommand):
    help = "Fetch receipts from miners"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.m_receipts = Counter(
            "receipts",
            documentation="Number of transferred receipts",
        )
        self.m_miners = Gauge(
            "miners",
            documentation="Number of miners to transfer from",
        )
        self.m_line_errors = Counter(
            "line_errors",
            labelnames=["exc_type"],
            documentation="Number of invalid lines in received pages",
        )
        self.m_transfer_errors = Counter(
            "transfer_errors",
            labelnames=["exc_type"],
            documentation="Number of completely failed page transfers",
        )
        self.m_transfer_duration = Histogram(
            "transfer_duration",
            documentation="Total time to transfer all required pages",
        )
        self.m_catchup_pages_left = Gauge(
            "catchup_pages_left",
            documentation="Pages waiting for catch-up",
        )

    def add_arguments(self, parser):
        parser.add_argument(
            "--debug-miner-hotkey",
            type=str,
            help="Only for debug: hotkey of miner to fetch receipts from",
        )
        parser.add_argument(
            "--debug-miner-ip",
            type=str,
            help="Only for debug: IP address of miner to fetch receipts from",
        )
        parser.add_argument(
            "--debug-miner-port",
            type=int,
            help="Only for debug: IP port of miner to fetch receipts from",
        )
        parser.add_argument(
            "--interval",
            type=float,
            help="If provided, runs in daemon mode and polls for changes every `interval` seconds.",
        )

    @async_to_sync
    async def handle(
        self,
        interval: float | None,
        debug_miner_hotkey: str | None,
        debug_miner_ip: str | None,
        debug_miner_port: int | None,
        **kwargs,
    ):
        if (debug_miner_hotkey, debug_miner_ip, debug_miner_port) != (None, None, None):
            # 1st, use explicitly specified miner if available
            if None in {debug_miner_hotkey, debug_miner_ip, debug_miner_port}:
                raise ValueError("Either none or all of explicit miner details must be provided")
            miner = [debug_miner_hotkey, debug_miner_ip, debug_miner_port]
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

        # IMPORTANT: This encompasses at least the current and the previous cycle.
        cutoff = timezone.now() - timedelta(hours=5)

        """
        General considerations:
        - higher concurrency:
            - higher bandwidth use
            - more parallel CPU-heavy signature check tasks -> steal CPU time from asyncio thread (GIL) 
        - lower concurrency:
            - slows down the process due to higher influence of network latency 
        - higher allowed request timeout:
            - one slow miner may stall the whole process for longer
            - less timeouts due to CPU time being stolen by CPU heavy tasks
        """

        if interval is None:
            await self.run_once(cutoff, miners)
        else:
            while True:
                try:
                    await self.run_in_loop(interval, cutoff, miners)
                except TransferMustBeEnabledException:
                    # Sleep instead of exiting in case the transfer is dynamically re-enabled.
                    logger.info("Transfer is currently disabled. Sleeping for a minute.")
                    await asyncio.sleep(60)

    async def run_once(
        self, cutoff: datetime, miners: Callable[[], Awaitable[list[MinerInfo]]]
    ) -> None:
        catchup_cutoff_page = LocalFilesystemPagedReceiptStore.current_page_at(cutoff)
        current_page = LocalFilesystemPagedReceiptStore.current_page()
        async with aiohttp.ClientSession() as session:
            await self.catch_up(
                # Pull all pages from newest to oldest
                pages=list(reversed(range(catchup_cutoff_page, current_page + 1))),
                miners=miners,
                session=session,
                semaphore=asyncio.Semaphore(50),
            )

    async def run_in_loop(
        self, interval: float, cutoff: datetime, miners: Callable[[], Awaitable[list[MinerInfo]]]
    ) -> None:
        """
        Do a full catch-up + listen for changes in latest 2 pages indefinitely
        """
        catchup_cutoff_page = LocalFilesystemPagedReceiptStore.current_page_at(cutoff)
        current_page = LocalFilesystemPagedReceiptStore.current_page()

        # TCP adds significant overhead - it's important to reuse connections.
        async with aiohttp.ClientSession() as session:
            # Catch-up with the latest pages so that the "keep up" loop has easier time later
            await self.catch_up(
                pages=list(reversed(range(current_page - N_ACTIVE_PAGES + 1, current_page + 1))),
                miners=miners,
                session=session,
                semaphore=asyncio.Semaphore(50),
            )
            await asyncio.gather(
                # Slowly catch up with non-active pages, newest first
                self.catch_up(
                    pages=list(
                        reversed(range(catchup_cutoff_page, current_page - N_ACTIVE_PAGES + 1))
                    ),
                    miners=miners,
                    session=session,
                    # Throttle this lower so that it doesn't choke the "keep up" loop
                    semaphore=asyncio.Semaphore(10),
                ),
                # Keep up with latest pages continuously in parallel
                self.keep_up(
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
    ) -> None:
        """
        Fetches new receipts on given pages one by one.
        """
        for idx, page in enumerate(pages):
            self.m_catchup_pages_left.set(len(pages) - idx)
            start_time = time.monotonic()
            current_loop_miners = await miners()
            result = await ReceiptsTransfer.transfer(
                miners=current_loop_miners,
                pages=[page],
                session=session,
                semaphore=semaphore,
                # We may need to download a lot of full pages, so the timeout is higher.
                request_timeout=3.0,
            )
            elapsed = time.monotonic() - start_time

            logger.info(
                f"Catching up: "
                f"{page=} ({idx + 1}/{len(pages)}) "
                f"receipts={result.n_receipts} "
                f"{elapsed=:.3f} "
                f"transfer_errors={len(result.transfer_errors)} "
                f"line_errors={len(result.line_errors)} "
            )

            self._push_common_metrics(result)
        self.m_catchup_pages_left.set(0)

    async def keep_up(
        self,
        interval: float,
        miners: Callable[[], Awaitable[list[MinerInfo]]],
        session: aiohttp.ClientSession,
        semaphore: asyncio.Semaphore,
    ) -> None:
        """
        Runs indefinitely and polls for changes in active pages every `interval`.
        """
        while True:
            start_time = time.monotonic()
            current_page = LocalFilesystemPagedReceiptStore.current_page()
            pages = list(reversed(range(current_page - N_ACTIVE_PAGES + 1, current_page + 1)))
            current_loop_miners = await miners()
            result = await ReceiptsTransfer.transfer(
                miners=current_loop_miners,
                pages=pages,
                session=session,
                semaphore=semaphore,
                request_timeout=1.0,
            )
            elapsed = time.monotonic() - start_time

            logger.info(
                f"Keeping up: "
                f"{pages=} "
                f"receipts={result.n_receipts} "
                f"{elapsed=:.3f} "
                f"transfer_errors={len(result.transfer_errors)} "
                f"line_errors={len(result.line_errors)} "
            )

            self._push_common_metrics(result)
            self.m_miners.set(len(current_loop_miners))
            self.m_transfer_duration.observe(elapsed)

            # Sleep for the remainder of the time if any
            if elapsed < interval:
                time.sleep(interval - elapsed)

    def _push_common_metrics(self, result: TransferResult) -> None:
        n_line_errors: defaultdict[type[Exception], int] = defaultdict(int)
        for line_error in result.line_errors:
            n_line_errors[type(line_error)] += 1
        for exc_type, exc_count in n_line_errors.items():
            self.m_line_errors.labels(exc_type=exc_type.__name__).inc(exc_count)

        n_transfer_errors: defaultdict[type[Exception], int] = defaultdict(int)
        for transfer_error in result.transfer_errors:
            n_transfer_errors[type(transfer_error)] += 1
        for exc_type, exc_count in n_transfer_errors.items():
            self.m_transfer_errors.labels(exc_type=exc_type.__name__).inc(exc_count)

        self.m_receipts.inc(result.n_receipts)
