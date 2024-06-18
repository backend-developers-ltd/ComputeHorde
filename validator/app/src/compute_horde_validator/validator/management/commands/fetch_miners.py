import bittensor
from django.conf import settings
from django.core.management import BaseCommand

from compute_horde_validator.validator.synthetic_jobs.utils import get_miners


class Command(BaseCommand):
    def handle(self, *args, **options):
        metagraph = bittensor.metagraph(netuid=settings.BITTENSOR_NETUID, network=settings.BITTENSOR_NETWORK)
        miners = get_miners(metagraph)
        print("Total miners:", len(miners))
