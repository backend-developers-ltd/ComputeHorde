from django.core.management import BaseCommand

from compute_horde_miner.miner.quasi_axon import announce_address_and_port


class Command(BaseCommand):
    def handle(self, *args, **options):
        announce_address_and_port()
