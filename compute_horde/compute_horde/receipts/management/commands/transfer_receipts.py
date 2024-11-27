import asyncio
import logging
import time
from typing import Sequence, TypeAlias, cast

import bittensor
from asgiref.sync import async_to_sync
from django.conf import settings
from django.core.management import BaseCommand

from compute_horde.receipts.store.local import LocalFilesystemPagedReceiptStore
from compute_horde.receipts.transfer import DjangoCacheCheckpointBackend, ReceiptsTransfer

MinerInfo: TypeAlias = tuple[str, str, int]

logger = logging.getLogger(__name__)


class Command(BaseCommand):
    help = "Fetch receipts from miners"

    def add_arguments(self, parser):
        parser.add_argument("--daemon", "-d", default=False, action='store_true')
        parser.add_argument("--miner-hotkey", type=str)
        parser.add_argument("--miner-ip", type=str)
        parser.add_argument("--miner-port", type=int)

    def handle(self, daemon: bool, miner_hotkey: str, miner_ip: str, miner_port: int, **kwargs):
        logger.info(f"asd")
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
            self.do_transfer_once(miners)
            if not daemon:
                break
            time.sleep(1)

    @async_to_sync
    async def do_transfer_once(self, miners: Sequence[MinerInfo]):
        # TODO: Be smart about catching up when we have no receipts at all?
        n_pages_to_fetch = 2
        latest_page = LocalFilesystemPagedReceiptStore.active_page_id()
        pages = [latest_page - offset for offset in reversed(range(n_pages_to_fetch))]

        # TODO: cache name should be coming from settings
        # TODO: inject it automatically
        checkpoint_backend = DjangoCacheCheckpointBackend("receipts_checkpoints")

        async def transfer_from_miner(miner: MinerInfo, semaphore: asyncio.Semaphore):
            hotkey, ip, port = miner
            transfer = ReceiptsTransfer(
                server_url=f"http://{ip}:{port}/receipts",
                checkpoint_backend=checkpoint_backend,
            )

            async with semaphore:
                fetched_receipts = await transfer.transfer_new_receipts(pages=pages)

            total = sum(len(receipts) for receipts in fetched_receipts.values())
            logger.info(f"Fetched {total} receipts from {hotkey} at {ip}:{port}")

        async def transfer_from_all_miners():
            # TODO: Configurable, but default to 100 or so
            semaphore = asyncio.Semaphore(50)
            tasks = []
            for miner in miners:
                tasks.append(asyncio.create_task(transfer_from_miner(miner, semaphore)))
            await asyncio.gather(*tasks)

        await transfer_from_all_miners()
