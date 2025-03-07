from django.core.management import BaseCommand

from ...tasks import sync_metagraph


class Command(BaseCommand):
    def handle(self, *args, **options):
        sync_metagraph()
