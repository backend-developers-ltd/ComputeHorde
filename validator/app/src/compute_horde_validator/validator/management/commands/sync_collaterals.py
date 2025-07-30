from django.core.management.base import BaseCommand

from compute_horde_validator.validator.tasks import sync_collaterals


class Command(BaseCommand):
    def handle(self, *args, **options):
        sync_collaterals()
