import datetime

from django.core.management import BaseCommand

from compute_horde_miner.miner.tasks import prepare_receipts


class Command(BaseCommand):
    def add_arguments(self, parser):
        parser.add_argument('--date', type=datetime.date.fromisoformat)

    def handle(self, *args, date, **options):
        prepare_receipts(date)
