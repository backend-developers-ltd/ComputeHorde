from django.core.management import BaseCommand

from ...tasks import schedule_synthetic_jobs


class Command(BaseCommand):
    help = "Store the time to run a synthetic job batch in db"

    def handle(self, *args, **options):
        schedule_synthetic_jobs()
