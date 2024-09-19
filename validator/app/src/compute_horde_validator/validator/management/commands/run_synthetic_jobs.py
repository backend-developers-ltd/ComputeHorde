from django.core.management import BaseCommand

from ...tasks import run_synthetic_jobs


class Command(BaseCommand):
    help = "Store the time to run a synthetic job batch in db"

    def handle(self, *args, **options):
        run_synthetic_jobs()
