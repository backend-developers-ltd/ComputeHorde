import logging
import random
import time
from collections import defaultdict
from collections.abc import Callable
from datetime import timedelta
from functools import cached_property
from typing import TypeAlias
from uuid import uuid4

import bittensor
from django.conf import settings
from django.core.management.base import BaseCommand
from django.utils import timezone

from compute_horde.executor_class import DEFAULT_EXECUTOR_CLASS
from compute_horde.receipts.models import (
    JobAcceptedReceipt,
    JobFinishedReceipt,
    JobStartedReceipt,
)
from compute_horde.receipts.schemas import (
    JobAcceptedReceiptPayload,
    JobFinishedReceiptPayload,
    JobStartedReceiptPayload,
)
from compute_horde.receipts.store.current import receipts_store

logger = logging.getLogger(__name__)


def _sign(kp: bittensor.Keypair, blob: str):
    return f"0x{kp.sign(blob).hex()}"


ReceiptModel: TypeAlias = JobAcceptedReceipt | JobStartedReceipt | JobFinishedReceipt
ReceiptFactory: TypeAlias = Callable[[bittensor.Keypair, bittensor.Keypair], ReceiptModel]


class Command(BaseCommand):
    help = "Generate random receipts"

    # Command args
    n: int
    interval: int | None
    condense: int

    @cached_property
    def validator_keys(self) -> bittensor.Keypair:
        return bittensor.Keypair.create_from_seed("1" * 64)

    @cached_property
    def miner_keys(self) -> bittensor.Keypair:
        return settings.BITTENSOR_WALLET().hotkey

    def add_arguments(self, parser):
        parser.add_argument("n", type=int, help="Number of receipts")
        parser.add_argument(
            "--interval",
            type=float,
            default=None,
            help="Keep on adding `n` receipts every `interval` seconds. (default: add only once)",
        )
        parser.add_argument(
            "--interval-condense",
            type=int,
            default=2,
            help="Larger value condenses the creation of receipts closer to the start of interval.",
        )

    def handle(self, *args, **kwargs):
        self.n = kwargs["n"]
        self.interval = kwargs["interval"]
        self.condense = kwargs["interval_condense"]

        if self.interval is None:
            so_far = 0
            # Insert in chunks of up to 1000
            while so_far < self.n:
                chunk = min(self.n - so_far, 1000)
                receipts = [self.generate_one() for _ in range(chunk)]
                by_type: defaultdict[type[ReceiptModel], list[ReceiptModel]] = defaultdict(list)
                for receipt in receipts:
                    by_type[receipt.__class__].append(receipt)
                for cls, receipts in by_type.items():
                    cls.objects.bulk_create(receipts)
                    logger.info(f"Inserted {len(receipts)} receipts")
                    receipts_store.store([r.to_receipt() for r in receipts])
                so_far += chunk

        else:
            time_per_receipt = self.interval / self.n / self.condense
            while True:
                time_started = time.time()
                receipts_this_loop = []
                for _ in range(self.n):
                    receipt = self.generate_one()
                    receipts_store.store([receipt.to_receipt()])
                    # wait for time_per_receipt +- 20% of the time
                    receipts_this_loop.append(receipt)
                    time.sleep(random.uniform(time_per_receipt * 0.8, time_per_receipt * 1.2))
                # Sleep for the rest of the cycle
                time.sleep(max(time_started + self.interval - time.time(), 0))
                logger.info(f"Created {len(receipts_this_loop)} receipts")

    def generate_one(self) -> ReceiptModel:
        return random.choice(
            (
                self._generate_job_accepted_receipt,
                self._generate_job_started_receipt,
                self._generate_job_finished_receipt,
            )
        )(self.validator_keys, self.miner_keys)

    def _receipt_timestamp(self):
        if self.interval:
            return timezone.now()
        else:
            # For one-time generation, spread the receipts over last 10 hours
            return timezone.now() - timedelta(seconds=int(60 * 60 * 10 * random.random()))

    def _generate_job_accepted_receipt(
        self,
        validator_keys: bittensor.Keypair,
        miner_keys: bittensor.Keypair,
    ) -> JobAcceptedReceipt:
        payload = JobAcceptedReceiptPayload(
            job_uuid=str(uuid4()),
            miner_hotkey=miner_keys.ss58_address,
            validator_hotkey=validator_keys.ss58_address,
            timestamp=self._receipt_timestamp(),
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
        self,
        validator_keys: bittensor.Keypair,
        miner_keys: bittensor.Keypair,
    ) -> JobStartedReceipt:
        payload = JobStartedReceiptPayload(
            job_uuid=str(uuid4()),
            miner_hotkey=miner_keys.ss58_address,
            validator_hotkey=validator_keys.ss58_address,
            timestamp=self._receipt_timestamp(),
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
        self,
        validator_keys: bittensor.Keypair,
        miner_keys: bittensor.Keypair,
    ) -> JobFinishedReceipt:
        payload = JobFinishedReceiptPayload(
            job_uuid=str(uuid4()),
            miner_hotkey=miner_keys.ss58_address,
            validator_hotkey=validator_keys.ss58_address,
            timestamp=self._receipt_timestamp(),
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
