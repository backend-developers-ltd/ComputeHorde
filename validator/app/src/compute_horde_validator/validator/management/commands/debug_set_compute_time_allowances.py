from django.core.management import BaseCommand

from ...tasks import set_compute_time_allowances


class Command(BaseCommand):
    def handle(self, *args, **options):
        set_compute_time_allowances()
