from collections.abc import Iterable
from datetime import timedelta
from enum import Enum
from typing import Self

from compute_horde.mv_protocol.validator_requests import ReceiptPayload
from compute_horde.receipts import Receipt
from django.core.serializers.json import DjangoJSONEncoder
from django.db import models


class EnumEncoder(DjangoJSONEncoder):
    def default(self, obj):
        if isinstance(obj, Enum):
            return obj.value
        return super().default(obj)


class Validator(models.Model):
    public_key = models.TextField(unique=True)
    active = models.BooleanField()

    def __str__(self):
        return f"hotkey: {self.public_key}"


class ValidatorBlacklist(models.Model):
    validator = models.OneToOneField(Validator, on_delete=models.CASCADE)

    class Meta:
        verbose_name = "Blacklisted Validator"
        verbose_name_plural = "Blacklisted Validators"

    def __str__(self):
        return f"hotkey: {self.validator.public_key}"


class AcceptedJob(models.Model):
    class Status(models.TextChoices):
        WAITING_FOR_EXECUTOR = "WAITING_FOR_EXECUTOR"
        WAITING_FOR_PAYLOAD = "WAITING_FOR_PAYLOAD"
        RUNNING = "RUNNING"
        FINISHED = "FINISHED"
        FAILED = "FAILED"

    validator = models.ForeignKey(Validator, on_delete=models.CASCADE)
    job_uuid = models.UUIDField()
    executor_token = models.CharField(max_length=73)
    status = models.CharField(choices=Status.choices, max_length=255)
    initial_job_details = models.JSONField(encoder=EnumEncoder)
    full_job_details = models.JSONField(encoder=EnumEncoder, null=True)
    exit_status = models.PositiveSmallIntegerField(null=True)
    stdout = models.TextField(blank=True, default="")
    stderr = models.TextField(blank=True, default="")
    result_reported_to_validator = models.DateTimeField(null=True)
    time_took = models.DurationField(null=True)
    score = models.FloatField(null=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    def __str__(self):
        return (
            f"uuid: {self.job_uuid} - validator hotkey: {self.validator.public_key} - {self.status}"
        )

    @classmethod
    async def get_for_validator(cls, validator: Validator) -> dict[str, Self]:
        return {
            str(job.job_uuid): job
            async for job in cls.objects.filter(
                validator=validator,
                status__in=[
                    cls.Status.WAITING_FOR_EXECUTOR.value,
                    cls.Status.WAITING_FOR_PAYLOAD.value,
                    cls.Status.RUNNING.value,
                ],
            )
        }

    @classmethod
    async def get_not_reported(cls, validator: Validator) -> Iterable[Self]:
        return [
            job
            async for job in cls.objects.filter(
                validator=validator,
                status__in=[cls.Status.FINISHED.value, cls.Status.FAILED.value],
                result_reported_to_validator__isnull=True,
            )
        ]


class JobReceipt(models.Model):
    validator_signature = models.CharField(max_length=256)
    miner_signature = models.CharField(max_length=256)

    # payload fields
    job_uuid = models.UUIDField()
    miner_hotkey = models.CharField(max_length=256)
    validator_hotkey = models.CharField(max_length=256)
    time_started = models.DateTimeField()
    time_took_us = models.BigIntegerField()
    score_str = models.CharField(max_length=256)

    def __str__(self):
        return f"uuid: {self.job_uuid}"

    def time_took(self):
        return timedelta(microseconds=self.time_took_us)

    def score(self):
        return float(self.score_str)

    def to_receipt(self):
        return Receipt(
            payload=ReceiptPayload(
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
