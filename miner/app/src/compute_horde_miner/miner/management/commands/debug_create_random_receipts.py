import logging
import random
import time
from collections import defaultdict
from collections.abc import Callable
from functools import cached_property
from typing import TypeAlias
from uuid import uuid4

import bittensor
from compute_horde.executor_class import DEFAULT_EXECUTOR_CLASS
from compute_horde.receipts.models import (
    JobAcceptedReceipt,
    JobFinishedReceipt,
    JobStartedReceipt,
    ReceiptModel,
)
from compute_horde.receipts.schemas import (
    JobAcceptedReceiptPayload,
    JobFinishedReceiptPayload,
    JobStartedReceiptPayload,
)
from compute_horde.utils import sign_blob
from django.core.management.base import BaseCommand
from django.utils import timezone

from compute_horde_miner.miner.receipts import current_store

logger = logging.getLogger(__name__)

ReceiptFactory: TypeAlias = Callable[[bittensor.Keypair, bittensor.Keypair], ReceiptModel]


class Command(BaseCommand):
    help = "Generate random receipts"
    # Command args
    n: int
    interval: int | None

    def handle(self, *args, **kwargs):
        self.n = kwargs["n"]
        self.interval = kwargs["interval"]

        if self.interval is None:
            added_so_far = 0
            # Insert in chunks
            while added_so_far < self.n:
                current_chunk_size = min(self.n - added_so_far, 1000)
                receipts = [self.generate_one() for _ in range(current_chunk_size)]
                by_type: defaultdict[type[ReceiptModel], list[ReceiptModel]] = defaultdict(list)
                for receipt in receipts:
                    by_type[type(receipt)].append(receipt)
                for cls, receipts in by_type.items():
                    cls.objects.bulk_create(receipts)  # type: ignore
                    current_store().store([r.to_receipt() for r in receipts])
                    logger.info(f"Inserted {len(receipts)} {cls.__name__}")
                added_so_far += current_chunk_size

        else:
            # dividing by 5 makes the insertions a bit more "batchy"
            time_per_receipt = self.interval / self.n / 5

            while True:
                time_started = time.monotonic()
                receipts_this_loop = []

                for _ in range(self.n):
                    receipt = self.generate_one()
                    receipt.save()
                    current_store().store([receipt.to_receipt()])
                    receipts_this_loop.append(receipt)
                    time.sleep(time_per_receipt)

                # Sleep for the rest of the interval
                time.sleep(max(time_started + self.interval - time.monotonic(), 0))
                logger.info(f"Created {len(receipts_this_loop)} receipts")

    def add_arguments(self, parser):
        parser.add_argument("n", type=int, help="Number of receipts")
        parser.add_argument(
            "--interval",
            type=float,
            default=None,
            help="Keep on adding `n` receipts every `interval` seconds. (default: add only once)",
        )

    def generate_one(self) -> ReceiptModel:
        return random.choice(
            (
                _generate_job_accepted_receipt,
                _generate_job_started_receipt,
                _generate_job_finished_receipt,
            )
        )(self.validator_keys, self.miner_keys)

    @cached_property
    def miner_keys(self) -> bittensor.Keypair:
        return bittensor.Keypair.create_from_seed("2" * 64)

    @cached_property
    def validator_keys(self) -> bittensor.Keypair:
        return bittensor.Keypair.create_from_seed("1" * 64)


def _generate_job_accepted_receipt(
    validator_keys: bittensor.Keypair,
    miner_keys: bittensor.Keypair,
) -> JobAcceptedReceipt:
    payload = JobAcceptedReceiptPayload(
        job_uuid=str(uuid4()),
        miner_hotkey=miner_keys.ss58_address,
        validator_hotkey=validator_keys.ss58_address,
        timestamp=timezone.now(),
        time_accepted=timezone.now(),
        ttl=123,
    )
    return JobAcceptedReceipt(
        job_uuid=payload.job_uuid,
        validator_hotkey=payload.validator_hotkey,
        miner_hotkey=payload.miner_hotkey,
        validator_signature=sign_blob(validator_keys, payload.blob_for_signing()),
        miner_signature=sign_blob(miner_keys, payload.blob_for_signing()),
        timestamp=payload.timestamp,
        time_accepted=payload.time_accepted,
        ttl=payload.ttl,
    )


def _generate_job_started_receipt(
    validator_keys: bittensor.Keypair,
    miner_keys: bittensor.Keypair,
) -> JobStartedReceipt:
    payload = JobStartedReceiptPayload(
        job_uuid=str(uuid4()),
        miner_hotkey=miner_keys.ss58_address,
        validator_hotkey=validator_keys.ss58_address,
        timestamp=timezone.now(),
        executor_class=DEFAULT_EXECUTOR_CLASS,
        max_timeout=123,
        is_organic=random.choice((True, False)),
        ttl=123,
    )
    return JobStartedReceipt(
        job_uuid=payload.job_uuid,
        validator_hotkey=payload.validator_hotkey,
        miner_hotkey=payload.miner_hotkey,
        validator_signature=sign_blob(validator_keys, payload.blob_for_signing()),
        miner_signature=sign_blob(miner_keys, payload.blob_for_signing()),
        timestamp=payload.timestamp,
        executor_class=payload.executor_class,
        max_timeout=payload.max_timeout,
        is_organic=payload.is_organic,
        ttl=payload.ttl,
    )


def _generate_job_finished_receipt(
    validator_keys: bittensor.Keypair,
    miner_keys: bittensor.Keypair,
) -> JobFinishedReceipt:
    payload = JobFinishedReceiptPayload(
        job_uuid=str(uuid4()),
        miner_hotkey=miner_keys.ss58_address,
        validator_hotkey=validator_keys.ss58_address,
        timestamp=timezone.now(),
        time_started=timezone.now(),
        time_took_us=12345,
        score_str="1.23",
    )
    return JobFinishedReceipt(
        job_uuid=payload.job_uuid,
        validator_hotkey=payload.validator_hotkey,
        miner_hotkey=payload.miner_hotkey,
        validator_signature=sign_blob(validator_keys, payload.blob_for_signing()),
        miner_signature=sign_blob(miner_keys, payload.blob_for_signing()),
        timestamp=payload.timestamp,
        time_started=payload.time_started,
        time_took_us=payload.time_took_us,
        score_str=payload.score_str,
    )
