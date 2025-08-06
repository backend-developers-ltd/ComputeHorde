from django.core.management import BaseCommand

from compute_horde_validator.validator.allowance.utils.manifests import sync_manifests


class Command(BaseCommand):
    def handle(self, *args, **options):
        sync_manifests()
