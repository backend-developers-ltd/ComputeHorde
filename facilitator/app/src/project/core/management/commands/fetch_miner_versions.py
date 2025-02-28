from django.core.management import BaseCommand

from ...tasks import fetch_miner_versions


class Command(BaseCommand):
    help = "Fetch miner & miner runner versions"

    def handle(self, *args, **options):
        fetch_miner_versions()
