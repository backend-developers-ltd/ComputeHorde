from django.core.management import BaseCommand

from project.core import eviction


class Command(BaseCommand):
    def handle(self, *args, **options):
        eviction.evict_all()
