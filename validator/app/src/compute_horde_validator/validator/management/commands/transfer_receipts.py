from asgiref.sync import async_to_sync
from django.core.management import BaseCommand

from compute_horde_validator.validator.receipts.default import receipts


class Command(BaseCommand):
    help = "Fetch receipts from miners"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

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
            "--daemon",
            action="store_true",
            default=False,
            help="Run indefinitely. (otherwise performs a single transfer)",
        )

    @async_to_sync
    async def handle(
        self,
        daemon: bool,
        debug_miner_hotkey: str | None,
        debug_miner_ip: str | None,
        debug_miner_port: int | None,
        **kwargs,
    ):
        await receipts().run_receipts_transfer(
            daemon=daemon,
            debug_miner_hotkey=debug_miner_hotkey,
            debug_miner_ip=debug_miner_ip,
            debug_miner_port=debug_miner_port,
        )
