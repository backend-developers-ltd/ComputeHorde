from asgiref.sync import async_to_sync
from django.core.management import BaseCommand

from compute_horde_miner.miner import eviction


class Command(BaseCommand):
    @async_to_sync
    async def handle(self, *args, **options):
        await eviction.evict_all()
