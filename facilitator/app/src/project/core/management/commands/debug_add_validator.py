from django.core.management import BaseCommand

from project.core.models import Validator


class Command(BaseCommand):
    def add_arguments(self, parser):
        parser.add_argument("validator_public_key", type=str, help="public key of the validator to be inserted")

    def handle(self, *args, **options):
        Validator.objects.get_or_create(
            ss58_address=options["validator_public_key"],
            defaults=dict(
                is_active=True,
            ),
        )
