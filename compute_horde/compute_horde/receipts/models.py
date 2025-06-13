import datetime
from datetime import timedelta
from typing import ClassVar, Self, TypeAlias, assert_never

from compute_horde_core.executor_class import ExecutorClass
from django.db import models

from compute_horde.executor_class import DEFAULT_EXECUTOR_CLASS
from compute_horde.receipts import ReceiptType
from compute_horde.receipts.schemas import (
    JobAcceptedReceiptPayload,
    JobFinishedReceiptPayload,
    JobStartedReceiptPayload,
    Receipt,
)


class ReceiptNotSigned(Exception):
    pass


class AbstractReceipt(models.Model):
    job_uuid = models.UUIDField()
    validator_hotkey = models.CharField(max_length=256)
    miner_hotkey = models.CharField(max_length=256)
    validator_signature = models.CharField(max_length=256)
    miner_signature = models.CharField(max_length=256, null=True, blank=True)
    timestamp = models.DateTimeField()

    # https://github.com/typeddjango/django-stubs/issues/1684#issuecomment-1706446344
    objects: ClassVar[models.Manager[Self]]

    class Meta:
        abstract = True
        constraints = [
            models.UniqueConstraint(fields=["job_uuid"], name="receipts_unique_%(class)s_job_uuid"),
        ]
        indexes = [
            models.Index(fields=["timestamp"], name="%(class)s_ts_idx"),
        ]

    def __str__(self):
        return f"job_uuid: {self.job_uuid}"


class JobStartedReceiptQuerySet(models.QuerySet["JobStartedReceipt"]):
    def valid_at(self, dt: datetime.datetime):
        return self.annotate(
            valid_until=models.ExpressionWrapper(
                models.F("timestamp") + models.F("ttl") * timedelta(seconds=1),
                output_field=models.DateTimeField(),
            ),
        ).filter(
            timestamp__lte=dt,
            valid_until__gte=dt,
        )


class JobStartedReceipt(AbstractReceipt):
    executor_class = models.CharField(max_length=255, default=DEFAULT_EXECUTOR_CLASS)
    is_organic = models.BooleanField()
    ttl = models.IntegerField()

    objects = JobStartedReceiptQuerySet.as_manager()  # type: ignore

    def to_receipt(self) -> Receipt:
        if self.miner_signature is None:
            raise ReceiptNotSigned("Miner signature is required")

        return Receipt(
            payload=JobStartedReceiptPayload(
                job_uuid=str(self.job_uuid),
                miner_hotkey=self.miner_hotkey,
                validator_hotkey=self.validator_hotkey,
                timestamp=self.timestamp,
                executor_class=ExecutorClass(self.executor_class),
                is_organic=self.is_organic,
                ttl=self.ttl,
            ),
            validator_signature=self.validator_signature,
            miner_signature=self.miner_signature,
        )

    @classmethod
    def from_receipt(cls, receipt: Receipt) -> "JobStartedReceipt":
        if not isinstance(receipt.payload, JobStartedReceiptPayload):
            raise ValueError(
                f"Incompatible receipt payload type. "
                f"Got: {type(receipt.payload).__name__} "
                f"Expected: {JobStartedReceiptPayload.__name__}"
            )

        return cls.from_payload(
            receipt.payload, receipt.validator_signature, receipt.miner_signature
        )

    @classmethod
    def from_payload(
        cls,
        payload: JobStartedReceiptPayload,
        validator_signature: str,
        miner_signature: str | None = None,
    ) -> "JobStartedReceipt":
        return JobStartedReceipt(
            job_uuid=payload.job_uuid,
            miner_hotkey=payload.miner_hotkey,
            validator_signature=validator_signature,
            miner_signature=miner_signature,
            timestamp=payload.timestamp,
            executor_class=payload.executor_class,
            is_organic=payload.is_organic,
            ttl=payload.ttl,
        )


class JobAcceptedReceipt(AbstractReceipt):
    time_accepted = models.DateTimeField()
    ttl = models.IntegerField()

    def to_receipt(self) -> Receipt:
        if self.miner_signature is None:
            raise ReceiptNotSigned("Miner signature is required")

        return Receipt(
            payload=JobAcceptedReceiptPayload(
                job_uuid=str(self.job_uuid),
                miner_hotkey=self.miner_hotkey,
                validator_hotkey=self.validator_hotkey,
                timestamp=self.timestamp,
                time_accepted=self.time_accepted,
                ttl=self.ttl,
            ),
            validator_signature=self.validator_signature,
            miner_signature=self.miner_signature,
        )

    @classmethod
    def from_receipt(cls, receipt: Receipt) -> "JobAcceptedReceipt":
        if not isinstance(receipt.payload, JobAcceptedReceiptPayload):
            raise ValueError(
                f"Incompatible receipt payload type. "
                f"Got: {type(receipt.payload).__name__} "
                f"Expected: {JobAcceptedReceiptPayload.__name__}"
            )

        return cls.from_payload(
            receipt.payload, receipt.validator_signature, receipt.miner_signature
        )

    @classmethod
    def from_payload(
        cls,
        payload: JobAcceptedReceiptPayload,
        validator_signature: str,
        miner_signature: str | None = None,
    ) -> "JobAcceptedReceipt":
        return JobAcceptedReceipt(
            job_uuid=payload.job_uuid,
            miner_hotkey=payload.miner_hotkey,
            validator_signature=validator_signature,
            miner_signature=miner_signature,
            timestamp=payload.timestamp,
            time_accepted=payload.time_accepted,
            ttl=payload.ttl,
        )


class JobFinishedReceipt(AbstractReceipt):
    time_started = models.DateTimeField()
    time_took_us = models.BigIntegerField()
    score_str = models.CharField(max_length=256)

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
                timestamp=self.timestamp,
                time_started=self.time_started,
                time_took_us=self.time_took_us,
                score_str=self.score_str,
            ),
            validator_signature=self.validator_signature,
            miner_signature=self.miner_signature,
        )

    @classmethod
    def from_receipt(cls, receipt: Receipt) -> "JobFinishedReceipt":
        if not isinstance(receipt.payload, JobFinishedReceiptPayload):
            raise ValueError(
                f"Incompatible receipt payload type. "
                f"Got: {receipt.payload.__class__.__name__} "
                f"Expected: {JobFinishedReceiptPayload.__name__}"
            )

        return cls.from_payload(
            receipt.payload, receipt.validator_signature, receipt.miner_signature
        )

    @classmethod
    def from_payload(
        cls,
        payload: JobFinishedReceiptPayload,
        validator_signature: str,
        miner_signature: str | None = None,
    ) -> "JobFinishedReceipt":
        return JobFinishedReceipt(
            job_uuid=payload.job_uuid,
            miner_hotkey=payload.miner_hotkey,
            validator_signature=validator_signature,
            miner_signature=miner_signature,
            timestamp=payload.timestamp,
            time_started=payload.time_started,
            time_took_us=payload.time_took_us,
            score_str=payload.score_str,
        )


ReceiptModel: TypeAlias = JobAcceptedReceipt | JobStartedReceipt | JobFinishedReceipt


def receipt_to_django_model(receipt: Receipt) -> ReceiptModel:
    match receipt.payload.receipt_type:
        case ReceiptType.JobAcceptedReceipt:
            return JobAcceptedReceipt.from_receipt(receipt)
        case ReceiptType.JobStartedReceipt:
            return JobStartedReceipt.from_receipt(receipt)
        case ReceiptType.JobFinishedReceipt:
            return JobFinishedReceipt.from_receipt(receipt)
        case _:
            assert_never(receipt.payload.receipt_type)
