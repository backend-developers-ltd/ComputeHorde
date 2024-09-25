import logging
import sys

from django.core.management.base import BaseCommand

from compute_horde_validator.validator.tasks import (
    llm_prompt_answering,
    llm_prompt_generation,
    llm_prompt_sampling,
)

logger = logging.getLogger(__name__)


class Command(BaseCommand):
    def add_arguments(self, parser):
        parser.add_argument(
            "--action", type=str, help="generation | sampling | answering", required=True
        )

    def handle(self, *args, **options):
        action = options["action"]
        logger.info(f"Running LLM prompt task with action: {action}")
        if action == "generation":
            llm_prompt_generation()
        elif action == "sampling":
            llm_prompt_sampling()
        elif action == "answering":
            llm_prompt_answering()
        else:
            logger.warning("Invalid action")
            sys.exit(1)
