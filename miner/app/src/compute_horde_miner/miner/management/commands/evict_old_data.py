from django.core.management import BaseCommand

from compute_horde_miner.miner import eviction


class Command(BaseCommand):
    def handle(self, *args, **options):
        eviction.evict_all()
