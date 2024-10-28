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
    AbstractReceipt,
    JobAcceptedReceipt,
    JobFinishedReceipt,
    JobStartedReceipt,
)
from compute_horde.receipts.schemas import (
    JobAcceptedReceiptPayload,
    JobFinishedReceiptPayload,
    JobStartedReceiptPayload,
)
from django.conf import settings
from django.core.management.base import BaseCommand
from django.utils import timezone

logger = logging.getLogger(__name__)


def _sign(kp: bittensor.Keypair, blob: str):
    return f"0x{kp.sign(blob).hex()}"


ReceiptFactory: TypeAlias = Callable[[bittensor.Keypair, bittensor.Keypair], AbstractReceipt]


def _generate_job_accepted_receipt(
        validator_keys: bittensor.Keypair,
        miner_keys: bittensor.Keypair,
) -> JobAcceptedReceipt:
    payload = JobAcceptedReceiptPayload(
        job_uuid=str(uuid4()),
        miner_hotkey=validator_keys.ss58_address,
        validator_hotkey=miner_keys.ss58_address,
        timestamp=timezone.now(),
        time_accepted=timezone.now(),
        ttl=123,
    )
    return JobAcceptedReceipt(
        job_uuid=payload.job_uuid,
        validator_hotkey=payload.validator_hotkey,
        miner_hotkey=payload.miner_hotkey,
        validator_signature=_sign(validator_keys, payload.blob_for_signing()),
        miner_signature=_sign(miner_keys, payload.blob_for_signing()),
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
        miner_hotkey=validator_keys.ss58_address,
        validator_hotkey=miner_keys.ss58_address,
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
        validator_signature=_sign(validator_keys, payload.blob_for_signing()),
        miner_signature=_sign(miner_keys, payload.blob_for_signing()),
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
        validator_hotkey=validator_keys.ss58_address,
        miner_hotkey=miner_keys.ss58_address,
        timestamp=timezone.now(),
        time_started=timezone.now(),
        time_took_us=12345,
        score_str="1.23",
    )
    return JobFinishedReceipt(
        job_uuid=payload.job_uuid,
        validator_hotkey=payload.validator_hotkey,
        miner_hotkey=payload.miner_hotkey,
        validator_signature=_sign(validator_keys, payload.blob_for_signing()),
        miner_signature=_sign(miner_keys, payload.blob_for_signing()),
        timestamp=payload.timestamp,
        time_started=payload.time_started,
        time_took_us=payload.time_took_us,
        score_str=payload.score_str,
    )


class Command(BaseCommand):
    help = "Generate random receipts"

    @cached_property
    def validator_keys(self) -> bittensor.Keypair:
        return bittensor.Keypair.create_from_seed("1" * 64)

    @cached_property
    def miner_keys(self) -> bittensor.Keypair:
        return settings.BITTENSOR_WALLET().hotkey

    def add_arguments(self, parser):
        parser.add_argument("n", type=int, help="Number of receipts")
        parser.add_argument("--interval", type=int, default=None)
        parser.add_argument("--interval-condense", type=int, default=2)

    def handle(self, *args, **kwargs):
        n = kwargs["n"]
        interval = kwargs["interval"]
        condense = kwargs["interval_condense"]

        def chunkinate(total: int, size: int):
            so_far = 0
            while so_far < total:
                next_chunk = min(size, total - so_far)
                yield next_chunk, so_far
                so_far += next_chunk

        if interval is None:
            # Insert in chunks of 1000
            for chunk, so_far in chunkinate(n, 1000):
                logger.info(f"Done {so_far} out of {n}")
                receipts = (self.generate() for _ in range(chunk))
                by_type: defaultdict[type[AbstractReceipt], list[AbstractReceipt]] = defaultdict(list)
                for receipt in receipts:
                    by_type[receipt.__class__].append(receipt)
                for cls, receipts in by_type.items():
                    cls.objects.bulk_create(receipts)

        else:
            time_per_receipt = interval / n / condense
            while True:
                time_started = time.time()
                for _ in range(n):
                    self.generate()
                    # wait for time_per_receipt +- 20% of the time
                    time.sleep(random.uniform(time_per_receipt * 0.8, time_per_receipt * 1.2))
                # Sleep for the rest of the cycle
                time.sleep(max(time_started + interval - time.time(), 0))

    def generate(self) -> AbstractReceipt:
        return random.choice(
            (
                _generate_job_accepted_receipt,
                _generate_job_started_receipt,
                _generate_job_finished_receipt,
            )
        )(self.validator_keys, self.miner_keys)
