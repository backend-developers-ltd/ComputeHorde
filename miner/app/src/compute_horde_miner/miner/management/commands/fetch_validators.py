from django.core.management import BaseCommand

from compute_horde_miner.miner.tasks import fetch_validators


class Command(BaseCommand):
    def handle(self, *args, **options):
        fetch_validators()
