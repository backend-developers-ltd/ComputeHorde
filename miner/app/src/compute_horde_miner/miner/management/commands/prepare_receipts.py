from django.core.management import BaseCommand

from compute_horde_miner.miner.tasks import prepare_receipts


class Command(BaseCommand):
    def handle(self, *args, **options):
        prepare_receipts()
