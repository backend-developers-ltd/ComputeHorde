from django.core.management import BaseCommand

from compute_horde_validator.validator import eviction


class Command(BaseCommand):
    def handle(self, *args, **options):
        eviction.evict_all()
