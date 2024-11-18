from django.core.management import BaseCommand

from ...tasks import fetch_dynamic_config


class Command(BaseCommand):
    help = "Fetch environment's dynamic config from github"

    def handle(self, *args, **options):
        fetch_dynamic_config()
