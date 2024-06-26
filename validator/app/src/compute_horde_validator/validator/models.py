import logging
import shlex
import uuid
from datetime import timedelta

from django.db import models
from django.db.models import UniqueConstraint

logger = logging.getLogger(__name__)


class SystemEvent(models.Model):
    """
    System Events that need to be reported to the stats collector
    """

    class EventType(models.TextChoices):
        WEIGHT_SETTING_SUCCESS = "WEIGHT_SETTING_SUCCESS"
        WEIGHT_SETTING_FAILURE = "WEIGHT_SETTING_FAILURE"
        MINER_ORGANIC_JOB_FAILURE = "MINER_ORGANIC_JOB_FAILURE"
        MINER_ORGANIC_JOB_SUCCESS = "MINER_ORGANIC_JOB_SUCCESS"
        MINER_SYNTHETIC_JOB_SUCCESS = "MINER_SYNTHETIC_JOB_SUCCESS"
        MINER_SYNTHETIC_JOB_FAILURE = "MINER_SYNTHETIC_JOB_FAILURE"
        RECEIPT_FAILURE = "RECEIPT_FAILURE"

    class EventSubType(models.TextChoices):
        SUCCESS = "SUCCESS"
        FAILURE = "FAILURE"
        SUBTENSOR_CONNECTIVITY_ERROR = "SUBTENSOR_CONNECTIVITY_ERROR"
        GENERIC_ERROR = "GENERIC_ERROR"
        WRITING_TO_CHAIN_TIMEOUT = "WRITING_TO_CHAIN_TIMEOUT"
        WRITING_TO_CHAIN_FAILED = "WRITING_TO_CHAIN_FAILED"
        WRITING_TO_CHAIN_GENERIC_ERROR = "WRITING_TO_CHAIN_GENERIC_ERROR"
        GIVING_UP = "GIVING_UP"
        MANIFEST_ERROR = "MANIFEST_ERROR"
        MINER_CONNECTION_ERROR = "MINER_CONNECTION_ERROR"
        JOB_NOT_STARTED = "JOB_NOT_STARTED"
        JOB_EXECUTION_TIMEOUT = "JOB_EXECUTION_TIMEOUT"
        RECEIPT_FETCH_ERROR = "RECEIPT_FETCH_ERROR"
        RECEIPT_SEND_ERROR = "RECEIPT_SEND_ERROR"

    type = models.CharField(max_length=255, choices=EventType.choices)
    subtype = models.CharField(max_length=255, choices=EventSubType.choices)
    timestamp = models.DateTimeField(auto_now_add=True)
    long_description = models.TextField(
        blank=True,
        help_text="Verbose description of the event, not sent to the stats collector",
    )
    data = models.JSONField(blank=True)
    sent = models.BooleanField(default=False)

    def to_dict(self):
        return {
            "type": self.type,
            "subtype": self.subtype,
            "timestamp": self.timestamp.isoformat(),
            "data": self.data,
        }

    def __str__(self):
        return f"SystemEvent({self.id}, {self.type}, {self.subtype})"


class Miner(models.Model):
    hotkey = models.CharField(max_length=255, unique=True)
    created_at = models.DateTimeField(auto_now_add=True)

    def __str__(self):
        return f"hotkey: {self.hotkey}"


class MinerBlacklist(models.Model):
    miner = models.OneToOneField(Miner, on_delete=models.CASCADE)

    class Meta:
        verbose_name = "Blacklisted Miner"
        verbose_name_plural = "Blacklisted Miners"

    def __str__(self):
        return f"hotkey: {self.miner.hotkey}"


class SyntheticJobBatch(models.Model):
    started_at = models.DateTimeField(auto_now_add=True)
    accepting_results_until = models.DateTimeField()
    scored = models.BooleanField(default=False)


class MinerManifest(models.Model):
    miner = models.ForeignKey(Miner, on_delete=models.CASCADE)
    batch = models.ForeignKey(SyntheticJobBatch, on_delete=models.CASCADE)
    created_at = models.DateTimeField(auto_now_add=True)
    executor_count = models.IntegerField(default=0)

    class Meta:
        constraints = [
            UniqueConstraint(fields=["miner", "batch"], name="unique_miner_manifest"),
        ]


class JobBase(models.Model):
    class Meta:
        abstract = True

    class Status(models.TextChoices):
        PENDING = "PENDING"
        COMPLETED = "COMPLETED"
        FAILED = "FAILED"

    job_uuid = models.UUIDField(default=uuid.uuid4, unique=True)
    miner = models.ForeignKey(Miner, on_delete=models.CASCADE)
    miner_address = models.CharField(max_length=255)
    miner_address_ip_version = models.IntegerField()
    miner_port = models.IntegerField()
    status = models.TextField(choices=Status.choices, default=Status.PENDING)
    updated_at = models.DateTimeField(auto_now=True)
    comment = models.TextField(blank=True, default="")
    job_description = models.TextField(blank=True)

    def __str__(self):
        return f"uuid: {self.job_uuid} - miner hotkey: {self.miner.hotkey} - {self.status}"


class SyntheticJob(JobBase):
    batch = models.ForeignKey(
        SyntheticJobBatch, on_delete=models.CASCADE, related_name="synthetic_jobs"
    )
    score = models.FloatField(default=0)


class OrganicJob(JobBase):
    stdout = models.TextField(blank=True, default="")
    stderr = models.TextField(blank=True, default="")

    def get_absolute_url(self):
        return f"/admin/validator/organicjob/{self.pk}/change/"


class AdminJobRequest(models.Model):
    uuid = models.UUIDField(default=uuid.uuid4, unique=True)
    miner = models.ForeignKey(Miner, on_delete=models.PROTECT)
    timeout = models.PositiveIntegerField(default=300, help_text="timeout in seconds")

    docker_image = models.CharField(max_length=255, help_text="docker image for job execution")
    raw_script = models.TextField(blank=True, help_text="raw script to be executed")

    args = models.TextField(blank=True, help_text="arguments passed to the script or docker image")
    env = models.JSONField(blank=True, default=dict, help_text="environment variables for the job")

    use_gpu = models.BooleanField(default=False, help_text="Whether to use GPU for the job")
    input_url = models.URLField(
        blank=True, help_text="URL to the input data source", max_length=1000
    )
    output_url = models.TextField(blank=True, help_text="URL for uploading output")

    created_at = models.DateTimeField(auto_now_add=True)

    status_message = models.TextField(blank=True, default="")

    def get_args(self):
        return shlex.split(self.args)

    def __str__(self):
        return f"uuid: {self.uuid} - miner hotkey: {self.miner.hotkey}"


class JobReceipt(models.Model):
    job_uuid = models.UUIDField()
    miner_hotkey = models.CharField(max_length=256)
    validator_hotkey = models.CharField(max_length=256)
    time_started = models.DateTimeField()
    time_took_us = models.BigIntegerField()
    score_str = models.CharField(max_length=256)

    class Meta:
        constraints = [
            UniqueConstraint(fields=["job_uuid"], name="unique_job_receipt_job_uuid"),
        ]

    def __str__(self):
        return f"job_uuid: {self.job_uuid}"

    def time_took(self):
        return timedelta(microseconds=self.time_took_us)

    def score(self):
        return float(self.score_str)
