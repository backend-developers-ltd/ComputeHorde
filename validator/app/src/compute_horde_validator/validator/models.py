import logging
import shlex
import uuid
from datetime import timedelta
from os import urandom

from compute_horde.executor_class import DEFAULT_EXECUTOR_CLASS
from django.contrib.postgres.fields import ArrayField
from django.db import models
from django.db.models import UniqueConstraint
from django.utils.timezone import now

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
        FACILITATOR_CLIENT_ERROR = "FACILITATOR_CLIENT_ERROR"
        VALIDATOR_MINERS_REFRESH = "VALIDATOR_MINERS_REFRESH"
        VALIDATOR_SYNTHETIC_JOBS_FAILURE = "VALIDATOR_SYNTHETIC_JOBS_FAILURE"
        VALIDATOR_FAILURE = "VALIDATOR_FAILURE"
        VALIDATOR_TELEMETRY = "VALIDATOR_TELEMETRY"
        VALIDATOR_CHANNEL_LAYER_ERROR = "VALIDATOR_CHANNEL_LAYER_ERROR"
        VALIDATOR_SYNTHETIC_JOB_SCHEDULED = "VALIDATOR_SYNTHETIC_JOB_SCHEDULED"

    class EventSubType(models.TextChoices):
        SUCCESS = "SUCCESS"
        FAILURE = "FAILURE"
        SUBTENSOR_CONNECTIVITY_ERROR = "SUBTENSOR_CONNECTIVITY_ERROR"
        COMMIT_WEIGHTS_SUCCESS = "COMMIT_WEIGHTS_SUCCESS"
        COMMIT_WEIGHTS_ERROR = "COMMIT_WEIGHTS_ERROR"
        COMMIT_WEIGHTS_UNREVEALED_ERROR = "COMMIT_WEIGHTS_UNREVEALED_ERROR"
        REVEAL_WEIGHTS_ERROR = "REVEAL_WEIGHTS_ERROR"
        REVEAL_WEIGHTS_SUCCESS = "REVEAL_WEIGHTS_SUCCESS"
        SET_WEIGHTS_SUCCESS = "SET_WEIGHTS_SUCCESS"
        SET_WEIGHTS_ERROR = "SET_WEIGHTS_ERROR"
        GENERIC_ERROR = "GENERIC_ERROR"
        WRITING_TO_CHAIN_TIMEOUT = "WRITING_TO_CHAIN_TIMEOUT"
        WRITING_TO_CHAIN_FAILED = "WRITING_TO_CHAIN_FAILED"
        WRITING_TO_CHAIN_GENERIC_ERROR = "WRITING_TO_CHAIN_GENERIC_ERROR"
        GIVING_UP = "GIVING_UP"
        MANIFEST_ERROR = "MANIFEST_ERROR"
        MANIFEST_TIMEOUT = "MANIFEST_TIMEOUT"
        MINER_CONNECTION_ERROR = "MINER_CONNECTION_ERROR"
        MINER_SEND_ERROR = "MINER_SEND_ERROR"
        MINER_SCORING_ERROR = "MINER_SCORING_ERROR"
        JOB_NOT_STARTED = "JOB_NOT_STARTED"
        JOB_REJECTED = "JOB_REJECTED"
        JOB_EXECUTION_TIMEOUT = "JOB_EXECUTION_TIMEOUT"
        RECEIPT_FETCH_ERROR = "RECEIPT_FETCH_ERROR"
        RECEIPT_SEND_ERROR = "RECEIPT_SEND_ERROR"
        SPECS_SEND_ERROR = "SPECS_SENDING_ERROR"
        HEARTBEAT_ERROR = "HEARTBEAT_ERROR"
        UNEXPECTED_MESSAGE = "UNEXPECTED_MESSAGE"
        UNAUTHORIZED = "UNAUTHORIZED"
        SYNTHETIC_BATCH = "SYNTHETIC_BATCH"
        SYNTHETIC_JOB = "SYNTHETIC_JOB"
        CHECKPOINT = "CHECKPOINT"
        OVERSLEPT = "OVERSLEPT"

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
            "data": {
                "description": self.long_description,
                **self.data,
            },
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


class Cycle(models.Model):
    start = models.BigIntegerField()
    stop = models.BigIntegerField()

    class Meta:
        constraints = [
            UniqueConstraint(fields=["start", "stop"], name="unique_cycle"),
        ]

    def __str__(self):
        return f"Cycle [{self.start};{self.stop})"


class SyntheticJobBatch(models.Model):
    """
    Scheduled running of synthetic jobs for a specific block.
    """

    block = models.BigIntegerField(
        null=True, unique=True, help_text="Block number for which this batch is scheduled"
    )
    cycle = models.ForeignKey(
        Cycle, blank=True, null=True, related_name="batches", on_delete=models.CASCADE
    )
    created_at = models.DateTimeField(default=now)
    started_at = models.DateTimeField(null=True)
    accepting_results_until = models.DateTimeField(null=True)
    scored = models.BooleanField(default=False)
    is_missed = models.BooleanField(
        default=False, help_text="Whether the batch was missed (not run)"
    )

    def __str__(self) -> str:
        return f"Scheduled validation #{self.pk} at block #{self.block}"


class MinerManifest(models.Model):
    miner = models.ForeignKey(Miner, on_delete=models.CASCADE)
    batch = models.ForeignKey(SyntheticJobBatch, on_delete=models.CASCADE)
    created_at = models.DateTimeField(auto_now_add=True)
    executor_count = models.IntegerField(default=0)
    online_executor_count = models.IntegerField(default=0)

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
    executor_class = models.CharField(max_length=255, default=DEFAULT_EXECUTOR_CLASS)
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
    executor_class = models.CharField(
        max_length=255, default=DEFAULT_EXECUTOR_CLASS, help_text="executor hardware class"
    )
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


class AbstractReceipt(models.Model):
    job_uuid = models.UUIDField()
    miner_hotkey = models.CharField(max_length=256)
    validator_hotkey = models.CharField(max_length=256)

    class Meta:
        abstract = True
        constraints = [
            UniqueConstraint(fields=["job_uuid"], name="unique_%(class)s_job_uuid"),
        ]

    def __str__(self):
        return f"job_uuid: {self.job_uuid}"


class JobFinishedReceipt(AbstractReceipt):
    time_started = models.DateTimeField()
    time_took_us = models.BigIntegerField()
    score_str = models.CharField(max_length=256)

    def time_took(self):
        return timedelta(microseconds=self.time_took_us)

    def score(self):
        return float(self.score_str)


class JobStartedReceipt(AbstractReceipt):
    executor_class = models.CharField(max_length=255, default=DEFAULT_EXECUTOR_CLASS)
    time_accepted = models.DateTimeField()
    max_timeout = models.IntegerField()


def get_random_salt() -> list[int]:
    return list(urandom(8))


class Weights(models.Model):
    """
    Weights set by validator at specific block.
    This is used to verify the weights revealed by the validator later.
    """

    uids = ArrayField(models.IntegerField())
    weights = ArrayField(models.IntegerField())
    salt = ArrayField(models.IntegerField(), default=get_random_salt)
    version_key = models.IntegerField()
    block = models.BigIntegerField()
    created_at = models.DateTimeField(auto_now_add=True)
    revealed_at = models.DateTimeField(null=True, default=None)

    class Meta:
        constraints = [
            UniqueConstraint(fields=["block"], name="unique_block"),
        ]
        indexes = [
            models.Index(fields=["created_at", "revealed_at"]),
        ]

    def save(self, *args, **kwargs) -> None:
        assert len(self.uids) == len(self.weights), "Length of uids and weights should be the same"
        super().save(*args, **kwargs)

    def __str__(self) -> str:
        return str(self.weights)
