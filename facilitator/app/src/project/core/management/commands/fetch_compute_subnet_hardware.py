from pprint import pprint

from django.core.management import BaseCommand

from ...tasks import record_compute_subnet_hardware
from ...utils import fetch_compute_subnet_hardware


class Command(BaseCommand):
    help = "Fetch hardware info from compute subnet"

    def add_arguments(self, parser):
        parser.add_argument("--store", action="store_true", help="Store fetched data in the database")

    def handle(self, *args, **options):
        if options["store"]:
            record_compute_subnet_hardware()
        else:
            specs = fetch_compute_subnet_hardware()
            pprint(specs)
