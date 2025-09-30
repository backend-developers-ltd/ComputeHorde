import logging

from compute_horde.subtensor import get_cycle_containing_block
from django.conf import settings
from django.core.management.base import BaseCommand

from compute_horde_validator.validator.allowance.default import allowance
from compute_horde_validator.validator.scoring.calculations import (
    calculate_allowance_paid_job_scores,
)


class Command(BaseCommand):
    help = "Calculate allowance paid job scores and print results without committing changes"

    def add_arguments(self, parser):
        parser.add_argument(
            "start_block",
            type=int,
            nargs="?",
            help="Start block number (defaults to current cycle start)",
        )
        parser.add_argument(
            "end_block",
            type=int,
            nargs="?",
            help="End block number (defaults to current cycle end)",
        )
        parser.add_argument("--verbose", action="store_true", help="Enable debug logging")

    def handle(self, *args, **options):
        """
        Calls calculate_allowance_paid_job_scores and prints the calculated values.
        Does not modify any data - purely for inspection.
        """
        start_block = options["start_block"]
        end_block = options["end_block"]

        if options["verbose"]:
            logging.getLogger().setLevel(logging.DEBUG)

        if start_block is None or end_block is None:
            metagraph = allowance().get_metagraph()
            current_cycle = get_cycle_containing_block(
                block=metagraph.block, netuid=settings.BITTENSOR_NETUID
            )
            start_block = start_block or current_cycle.start
            end_block = end_block or current_cycle.stop
            self.stdout.write(f"Using current cycle: blocks {start_block} to {end_block}")

        scores = calculate_allowance_paid_job_scores(start_block, end_block)

        for executor_class, miner_scores in scores.items():
            self.stdout.write(f"{executor_class}:")
            if miner_scores:
                self.stdout.write(f" Miners: {len(miner_scores)}")
                for miner, score in sorted(miner_scores.items(), key=lambda x: x[1], reverse=True):
                    self.stdout.write(f"    {miner}: {score}")
            else:
                self.stdout.write("  No scores")
            self.stdout.write("")
