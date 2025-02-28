from django.core.management import BaseCommand

from ...tasks import fetch_receipts, fetch_receipts_from_miner


class Command(BaseCommand):
    help = "Fetch receipts from miner"

    def add_arguments(self, parser):
        parser.add_argument("--miner-hotkey", type=str)
        parser.add_argument("--miner-ip", type=str)
        parser.add_argument("--miner-port", type=int)

    def handle(self, *args, miner_hotkey, miner_ip, miner_port, **options):
        if (miner_hotkey, miner_ip, miner_port) == (None, None, None):
            fetch_receipts()
        else:
            fetch_receipts_from_miner(miner_hotkey, miner_ip, miner_port)
