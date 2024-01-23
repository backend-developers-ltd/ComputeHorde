from collections.abc import Iterable
from enum import Enum
from typing import Self

from django.core.serializers.json import DjangoJSONEncoder
from django.db import models


class EnumEncoder(DjangoJSONEncoder):
    def default(self, obj):
        if isinstance(obj, Enum):
            return obj.value
        return super().default(obj)


class Validator(models.Model):
    public_key = models.TextField(unique=True)


class AcceptedJob(models.Model):
    class Status(models.TextChoices):
        WAITING_FOR_EXECUTOR = 'WAITING_FOR_EXECUTOR'
        WAITING_FOR_PAYLOAD = 'WAITING_FOR_PAYLOAD'
        RUNNING = 'RUNNING'
        FINISHED = 'FINISHED'
        FAILED = 'FAILED'

    validator = models.ForeignKey(Validator, on_delete=models.CASCADE)
    job_uuid = models.UUIDField()
    executor_token = models.CharField(max_length=73)
    status = models.CharField(choices=Status.choices, max_length=255)
    initial_job_details = models.JSONField(encoder=EnumEncoder)
    full_job_details = models.JSONField(encoder=EnumEncoder, null=True)
    exit_status = models.PositiveSmallIntegerField(null=True)
    stdout = models.TextField(blank=True, default='')
    stderr = models.TextField(blank=True, default='')
    result_reported_to_validator = models.DateTimeField(null=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    def __str__(self):
        return f'{self.job_uuid} - {self.status.value}'

    @classmethod
    async def get_for_validator(cls, validator: Validator) -> dict[str, Self]:
        return {
            str(job.job_uuid): job
            async for job in cls.objects.filter(
                validator=validator,
                status__in=[cls.Status.WAITING_FOR_EXECUTOR.value, cls.Status.WAITING_FOR_PAYLOAD.value, cls.Status.RUNNING.value]
            )
        }

    @classmethod
    async def get_not_reported(cls, validator: Validator) -> Iterable[Self]:
        return [job async for job in cls.objects.filter(
            validator=validator,
            status__in=[cls.Status.FINISHED.value, cls.Status.FAILED.value],
            result_reported_to_validator__isnull=True,
        )]
