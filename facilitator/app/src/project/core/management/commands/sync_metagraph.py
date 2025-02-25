from django.core.management import BaseCommand

from ...models import Validator
from ...tasks import sync_metagraph


class Command(BaseCommand):
    help = "Fetch validators & miners from the network"

    def handle(self, *args, **options):
        sync_metagraph()

        num_active_validators = Validator.objects.filter(is_active=True).count()
        self.stdout.write(self.style.SUCCESS(f"Active validators: {num_active_validators}"))
