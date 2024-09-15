import logging

from django.core.management.base import BaseCommand

from compute_horde_validator.validator.tasks import llama_prompt_generation

logger = logging.getLogger(__name__)


class Command(BaseCommand):
    def add_arguments(self, parser):
        pass

    def handle(self, *args, **options):
        llama_prompt_generation()
