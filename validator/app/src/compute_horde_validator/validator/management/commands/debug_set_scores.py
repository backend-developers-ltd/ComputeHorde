from django.core.management.base import BaseCommand

from compute_horde_validator.validator.tasks import set_scores


class Command(BaseCommand):
    def handle(self, *args, **options):
        set_scores()
