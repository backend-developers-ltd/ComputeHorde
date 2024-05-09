from asgiref.sync import async_to_sync
from django.core.management import BaseCommand

from compute_horde_miner.miner.utils import prepare_receipts


class Command(BaseCommand):
    def handle(self, *args, **options):
        async_to_sync(prepare_receipts)()
