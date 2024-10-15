from datetime import timedelta

from django.db import models

from compute_horde.executor_class import DEFAULT_EXECUTOR_CLASS, ExecutorClass
from compute_horde.mv_protocol.validator_requests import (
    JobFinishedReceiptPayload,
    JobStartedReceiptPayload,
)
from compute_horde.receipts.schemas import Receipt


class ReceiptNotSigned(Exception):
    pass


class AbstractReceipt(models.Model):
    job_uuid = models.UUIDField()
    validator_hotkey = models.CharField(max_length=256)
    miner_hotkey = models.CharField(max_length=256)
    validator_signature = models.CharField(max_length=256)
    miner_signature = models.CharField(max_length=256, null=True, blank=True)

    class Meta:
        abstract = True
        constraints = [
            models.UniqueConstraint(fields=["job_uuid"], name="receipts_unique_%(class)s_job_uuid"),
        ]

    def __str__(self):
        return f"job_uuid: {self.job_uuid}"


class JobStartedReceipt(AbstractReceipt):
    executor_class = models.CharField(max_length=255, default=DEFAULT_EXECUTOR_CLASS)
    time_accepted = models.DateTimeField()
    max_timeout = models.IntegerField()
    is_organic = models.BooleanField()

    # https://github.com/typeddjango/django-stubs/issues/1684#issuecomment-1706446344
    objects: models.Manager["JobStartedReceipt"]

    def to_receipt(self) -> Receipt:
        if self.miner_signature is None:
            raise ReceiptNotSigned("Miner signature is required")

        return Receipt(
            payload=JobStartedReceiptPayload(
                job_uuid=str(self.job_uuid),
                miner_hotkey=self.miner_hotkey,
                validator_hotkey=self.validator_hotkey,
                executor_class=ExecutorClass(self.executor_class),
                time_accepted=self.time_accepted,
                max_timeout=self.max_timeout,
                is_organic=self.is_organic,
            ),
            validator_signature=self.validator_signature,
            miner_signature=self.miner_signature,
        )


class JobFinishedReceipt(AbstractReceipt):
    time_started = models.DateTimeField()
    time_took_us = models.BigIntegerField()
    score_str = models.CharField(max_length=256)

    # https://github.com/typeddjango/django-stubs/issues/1684#issuecomment-1706446344
    objects: models.Manager["JobFinishedReceipt"]

    def time_took(self):
        return timedelta(microseconds=self.time_took_us)

    def score(self):
        return float(self.score_str)

    def to_receipt(self) -> Receipt:
        if self.miner_signature is None:
            raise ReceiptNotSigned("Miner signature is required")

        return Receipt(
            payload=JobFinishedReceiptPayload(
                job_uuid=str(self.job_uuid),
                miner_hotkey=self.miner_hotkey,
                validator_hotkey=self.validator_hotkey,
                time_started=self.time_started,
                time_took_us=self.time_took_us,
                score_str=self.score_str,
            ),
            validator_signature=self.validator_signature,
            miner_signature=self.miner_signature,
        )
