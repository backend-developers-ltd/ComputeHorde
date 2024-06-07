from asgiref.sync import async_to_sync
from django.core.management import BaseCommand, CommandError

from compute_horde_miner.miner.liveness_check import CheckError, check_all


class Command(BaseCommand):
    def handle(self, *args, **options):
        try:
            async_to_sync(check_all)()
        except CheckError as e:
            raise CommandError(repr(e)) from e
