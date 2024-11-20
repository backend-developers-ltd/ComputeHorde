from django.core.management import BaseCommand

from compute_horde_validator.validator.tasks import fetch_receipts


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
            fetch_receipts(
                miners=[
                    (miner_hotkey, miner_ip, miner_port),
                ]
            )
