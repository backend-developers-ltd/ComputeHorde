from django.core.management import BaseCommand

from compute_horde_miner.miner.models import Validator


class Command(BaseCommand):

    def add_arguments(self, parser):
        parser.add_argument('validator_public_key', type=str, help='public key of the validator to be inserted')

    def handle(self, *args, **options):
        Validator.objects.create(
            public_key=options['validator_public_key'],
            active=True,
        )
