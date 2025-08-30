import logging
import shlex
import uuid
from datetime import datetime, timedelta
from decimal import Decimal
from enum import IntEnum
from os import urandom
from typing import Self

from asgiref.sync import sync_to_async
from compute_horde.executor_class import DEFAULT_EXECUTOR_CLASS
from compute_horde.subtensor import get_cycle_containing_block
from compute_horde.utils import MIN_VALIDATOR_STAKE
from compute_horde_core.output_upload import OutputUpload, ZipAndHttpPutUpload
from compute_horde_core.volume import Volume, ZipUrlVolume
from django.conf import settings
from django.contrib.postgres.fields import ArrayField
from django.db import models
from django.db.models import Exists, OuterRef, UniqueConstraint
from django.utils import timezone
from django.utils.timezone import now

logger = logging.getLogger(__name__)


class SystemEvent(models.Model):
    """
    System Events that need to be reported to the stats collector
    """

    class EventType(models.TextChoices):
        WEIGHT_SETTING_SUCCESS = "WEIGHT_SETTING_SUCCESS"
        WEIGHT_SETTING_FAILURE = "WEIGHT_SETTING_FAILURE"
        # the two above are blankets for setting, committing and revealing
        MINER_ORGANIC_JOB_INFO = "MINER_ORGANIC_JOB_INFO"
        MINER_ORGANIC_JOB_FAILURE = "MINER_ORGANIC_JOB_FAILURE"
        MINER_ORGANIC_JOB_SUCCESS = "MINER_ORGANIC_JOB_SUCCESS"
        MINER_SYNTHETIC_JOB_SUCCESS = "MINER_SYNTHETIC_JOB_SUCCESS"
        MINER_SYNTHETIC_JOB_FAILURE = "MINER_SYNTHETIC_JOB_FAILURE"
        MINER_EXECUTOR_COUNT_CLIPPED = "MINER_EXECUTOR_COUNT_CLIPPED"
        RECEIPT_FAILURE = "RECEIPT_FAILURE"
        FACILITATOR_CLIENT_ERROR = "FACILITATOR_CLIENT_ERROR"
        VALIDATOR_MINERS_REFRESH = "VALIDATOR_MINERS_REFRESH"
        VALIDATOR_SYNTHETIC_JOBS_FAILURE = "VALIDATOR_SYNTHETIC_JOBS_FAILURE"
        VALIDATOR_FAILURE = "VALIDATOR_FAILURE"
        VALIDATOR_TELEMETRY = "VALIDATOR_TELEMETRY"
        VALIDATOR_CHANNEL_LAYER_ERROR = "VALIDATOR_CHANNEL_LAYER_ERROR"
        VALIDATOR_SYNTHETIC_JOB_SCHEDULED = "VALIDATOR_SYNTHETIC_JOB_SCHEDULED"
        VALIDATOR_OVERSLEPT_SCHEDULED_JOB_WARNING = "VALIDATOR_OVERSLEPT_SCHEDULED_JOB_WARNING"
        LLM_PROMPT_GENERATION = "LLM_PROMPT_GENERATION"
        LLM_PROMPT_ANSWERING = "LLM_PROMPT_ANSWERING"
        LLM_PROMPT_SAMPLING = "LLM_PROMPT_SAMPLING"
        BURNING_INCENTIVE = "BURNING_INCENTIVE"
        COMPUTE_TIME_ALLOWANCE = "COMPUTE_TIME_ALLOWANCE"
        METAGRAPH_SYNCING = "METAGRAPH_SYNCING"
        COLLATERAL_SYNCING = "COLLATERAL_SYNCING"

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
        WRITING_TO_CHAIN_GENERIC_ERROR = "WRITING_TO_CHAIN_GENERIC_ERROR"
        GIVING_UP = "GIVING_UP"
        MANIFEST_ERROR = "MANIFEST_ERROR"
        MANIFEST_TIMEOUT = "MANIFEST_TIMEOUT"
        MINER_CONNECTION_ERROR = "MINER_CONNECTION_ERROR"
        MINER_SEND_ERROR = "MINER_SEND_ERROR"
        MINER_SCORING_ERROR = "MINER_SCORING_ERROR"
        GENERIC_JOB_FAILURE = "GENERIC_JOB_FAILURE"
        JOB_NOT_STARTED = "JOB_NOT_STARTED"
        JOB_REJECTED = "JOB_REJECTED"
        JOB_EXCUSED = "JOB_EXCUSED"
        JOB_CHEATED = "JOB_CHEATED"
        MINER_BLACKLISTED = "MINER_BLACKLISTED"
        JOB_TIMEOUT = "JOB_TIMEOUT"
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
        WARNING = "WARNING"
        FAILED_TO_WAIT = "FAILED_TO_WAIT"
        TRUSTED_MINER_NOT_CONFIGURED = "TRUSTED_MINER_NOT_CONFIGURED"
        LLM_PROMPT_ANSWERS_MISSING = "LLM_PROMPT_ANSWERS_MISSING"
        INSUFFICIENT_PROMPTS = "INSUFFICIENT_PROMPTS"
        UNPROCESSED_WORKLOADS = "UNPROCESSED_WORKLOADS"
        PROMPT_GENERATION_STARTED = "PROMPT_GENERATION_STARTED"
        PROMPT_SAMPLING_SKIPPED = "PROMPT_SAMPLING_SKIPPED"
        NEW_WORKLOADS_CREATED = "NEW_WORKLOADS_CREATED"
        ERROR_UPLOADING_TO_S3 = "ERROR_UPLOADING_TO_S3"
        ERROR_DOWNLOADING_FROM_S3 = "ERROR_DOWNLOADING_FROM_S3"
        ERROR_DOWNLOADING_FROM_HUGGINGFACE = "ERROR_DOWNLOADING_FROM_HUGGINGFACE"
        ERROR_FAILED_SECURITY_CHECK = "ERROR_DURING_SECURITY_CHECK"
        ERROR_EXECUTOR_REPORTED_TIMEOUT = "ERROR_EXECUTOR_REPORTED_TIMEOUT"
        ERROR_VALIDATOR_REPORTED_TIMEOUT = "ERROR_VALIDATOR_REPORTED_TIMEOUT"
        JOB_PROCESS_NONZERO_EXIT_CODE = "JOB_PROCESS_NONZERO_EXIT_CODE"
        JOB_VOLUME_DOWNLOAD_FAILED = "JOB_VOLUME_DOWNLOAD_FAILED"
        JOB_RESULT_UPLOAD_FAILED = "JOB_RESULT_UPLOAD_FAILED"
        LLM_PROMPT_ANSWERS_DOWNLOAD_WORKER_FAILED = "LLM_PROMPT_ANSWERS_DOWNLOAD_WORKER_FAILED"
        APPLIED_BURNING = "APPLIED_BURNING"
        NO_BURNING = "NO_BURNING"
        GETTING_MINER_COLLATERAL_FAILED = "GETTING_MINER_COLLATERAL_FAILED"
        BLOCK_ALREADY_INSERTED_PROBABLY_BLOCK_CACHE_JITTER_IGNORE_ME_FOR_NOW = "BLOCK_ALREADY_INSERTED_PROBABLY_BLOCK_CACHE_JITTER_IGNORE_ME_FOR_NOW"

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


class MinerQueryset(models.QuerySet["Miner"]):
    def non_blacklisted(self):
        active_blacklist = MinerBlacklist.objects.active()

        return self.annotate(
            is_blacklisted=Exists(active_blacklist.filter(miner=OuterRef("id"))),
        ).filter(is_blacklisted=False)


class MetagraphSnapshot(models.Model):
    """
    Snapshot of the metagraph at a specific block.
    """

    block = models.BigIntegerField()
    updated_at = models.DateTimeField(auto_now_add=True)

    alpha_stake = ArrayField(models.FloatField())
    tao_stake = ArrayField(models.FloatField())
    stake = ArrayField(models.FloatField())

    uids = ArrayField(models.IntegerField())
    hotkeys = ArrayField(models.CharField(max_length=255))
    coldkeys = ArrayField(models.CharField(max_length=255), null=True, blank=True)

    # current active miners
    serving_hotkeys = ArrayField(models.CharField(max_length=255))

    class SnapshotType(IntEnum):
        LATEST = 0
        CYCLE_START = 1

    @classmethod
    def get_latest(cls) -> "MetagraphSnapshot":
        metagraph = MetagraphSnapshot.objects.get(id=cls.SnapshotType.LATEST)
        if metagraph.updated_at < now() - timedelta(minutes=1):
            msg = f"Tried to fetch stale metagraph last updated at: {metagraph.updated_at}"
            logger.error(msg)
            SystemEvent.objects.using(settings.DEFAULT_DB_ALIAS).create(
                type=SystemEvent.EventType.METAGRAPH_SYNCING,
                subtype=SystemEvent.EventSubType.GENERIC_ERROR,
                long_description=msg,
                data={"block": metagraph.block},
            )
            raise Exception(msg)
        return metagraph

    @classmethod
    async def aget_latest(cls) -> "MetagraphSnapshot":
        return await sync_to_async(cls.get_latest)()

    @classmethod
    def get_cycle_start(cls) -> "MetagraphSnapshot":
        return MetagraphSnapshot.objects.get(id=cls.SnapshotType.CYCLE_START)

    @classmethod
    async def aget_cycle_start(cls) -> "MetagraphSnapshot":
        return await sync_to_async(cls.get_cycle_start)()

    def get_serving_hotkeys(self) -> list[str]:
        """
        Get the list of serving hotkeys.
        :return: List of serving hotkeys.
        """
        return self.serving_hotkeys or []

    def get_total_validator_stake(self) -> float:
        """
        Get the total stake for all hotkeys.
        :return: The total stake.
        """
        return sum([s for s in self.stake if s > MIN_VALIDATOR_STAKE])


# contains all neurons not only miners
# TODO: rename to Neuron
class Miner(models.Model):
    objects = MinerQueryset.as_manager()

    hotkey = models.CharField(max_length=255, unique=True)
    coldkey = models.CharField(max_length=255, null=True, blank=True)
    created_at = models.DateTimeField(auto_now_add=True)

    uid = models.IntegerField(null=True)
    address = models.CharField(max_length=255, default="0.0.0.0")
    ip_version = models.IntegerField(default=4)
    port = models.IntegerField(default=0)

    evm_address = models.CharField(max_length=42, null=True)

    # This is a 256-bit integer, which is too big to store in any of postgres's int types.
    # So we are using the NUMERIC(78) here.
    collateral_wei = models.DecimalField(
        max_digits=settings.DECIMAL_PRECISION,
        decimal_places=0,
        default=Decimal(0),
    )

    def __str__(self):
        return f"hotkey: {self.hotkey}"


class MinerBlacklistQueryset(models.QuerySet["MinerBlacklist"]):
    def active(self):
        return self.filter(
            models.Q(expires_at__gt=timezone.now()) | models.Q(expires_at__isnull=True)
        )

    def expired(self):
        return self.filter(expires_at__lte=timezone.now())


class MinerBlacklist(models.Model):
    objects = MinerBlacklistQueryset.as_manager()

    class BlacklistReason(models.TextChoices):
        MANUAL = "MANUAL", "Manual"
        JOB_FAILED = "JOB_FAILED", "Job Failed"
        JOB_CHEATED = "JOB_CHEATED", "Job Cheated"

    miner = models.ForeignKey(Miner, on_delete=models.CASCADE)
    blacklisted_at = models.DateTimeField(auto_now_add=True)
    expires_at = models.DateTimeField(null=True, blank=True)
    reason = models.TextField(choices=BlacklistReason.choices, default=BlacklistReason.MANUAL)
    reason_details = models.TextField(blank=True, default="")

    class Meta:
        verbose_name = "Blacklisted Miner"
        verbose_name_plural = "Blacklisted Miners"


class ValidatorWhitelist(models.Model):
    hotkey = models.CharField(max_length=255, unique=True)
    # root_consumer = models.BooleanField(default=True)
    created_at = models.DateTimeField(auto_now_add=True)

    def __str__(self):
        return f"hotkey: {self.hotkey}"


class Cycle(models.Model):
    start = models.BigIntegerField()
    stop = models.BigIntegerField()

    class Meta:
        constraints = [
            UniqueConstraint(fields=["start", "stop"], name="unique_cycle"),
        ]

    def __str__(self):
        return f"Cycle [{self.start};{self.stop})"

    @classmethod
    def from_block(cls, block: int, netuid: int) -> Self:
        r = get_cycle_containing_block(block=block, netuid=netuid)
        c, _ = cls.objects.get_or_create(start=r.start, stop=r.stop)
        return c


class SyntheticJobBatch(models.Model):
    """
    Scheduled running of synthetic jobs for a specific block.
    """

    block = models.BigIntegerField(
        unique=True, help_text="Block number for which this batch is scheduled"
    )
    cycle = models.ForeignKey(Cycle, related_name="batches", on_delete=models.CASCADE)
    created_at = models.DateTimeField(default=now)
    started_at = models.DateTimeField(null=True)
    accepting_results_until = models.DateTimeField(null=True)
    scored = models.BooleanField(default=False)
    is_missed = models.BooleanField(
        default=False, help_text="Whether the batch was missed (not run)"
    )
    should_be_scored = models.BooleanField(default=True)

    def __str__(self) -> str:
        return f"Scheduled validation #{self.pk} at block #{self.block}"


class MinerManifest(models.Model):
    miner = models.ForeignKey(Miner, on_delete=models.CASCADE)
    batch = models.ForeignKey(SyntheticJobBatch, on_delete=models.CASCADE, null=True, blank=True)
    created_at = models.DateTimeField(auto_now_add=True)
    executor_class = models.CharField(max_length=255)
    executor_count = models.IntegerField(
        default=0,
        help_text="The total number of available executors of this class as reported by the miner",
    )
    online_executor_count = models.IntegerField(
        default=0,
        help_text="Number of executors that finished or properly declined a job request during the batch",
    )

    class Meta:
        constraints = [
            UniqueConstraint(
                fields=["miner", "batch", "executor_class"], name="unique_miner_manifest"
            ),
        ]
        indexes = [
            models.Index(fields=["created_at"]),
            models.Index(fields=["miner", "created_at"]),
            models.Index(fields=["batch", "created_at"]),
        ]


class JobBase(models.Model):
    class Meta:
        abstract = True

    class Status(models.TextChoices):
        PENDING = "PENDING"
        COMPLETED = "COMPLETED"
        FAILED = "FAILED"
        EXCUSED = "EXCUSED"

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
    error_type = models.TextField(null=True, default=None)
    error_detail = models.TextField(null=True, default=None)
    artifacts = models.JSONField(blank=True, default=dict)
    upload_results = models.JSONField(blank=True, default=dict)
    cheated = models.BooleanField(default=False)
    slashed = models.BooleanField(default=False)
    block = models.BigIntegerField(
        null=True, help_text="Block number on which this job is scheduled"
    )
    on_trusted_miner = models.BooleanField(default=False)
    streaming_details = models.JSONField(null=True, default=None)
    allowance_reservation_id = models.BigIntegerField(
        null=True, blank=True, help_text="ID of the allowance reservation for this job"
    )

    class Meta:
        indexes = [
            models.Index(fields=["block"]),
        ]

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

    @property
    def volume(self) -> Volume | None:
        if self.input_url:
            return ZipUrlVolume(contents=self.input_url)
        return None

    @property
    def output_upload(self) -> OutputUpload | None:
        if self.output_url:
            return ZipAndHttpPutUpload(url=self.output_url)
        return None


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


class PromptSeries(models.Model):
    """
    A series of prompts generated in a single run of the prompt generator.
    """

    series_uuid = models.UUIDField(default=uuid.uuid4, unique=True)
    s3_url = models.URLField(max_length=1000)
    created_at = models.DateTimeField(default=now)
    generator_version = models.PositiveSmallIntegerField()


class SolveWorkload(models.Model):
    """
    A collective workload of prompt samples to be solved together.
    """

    workload_uuid = models.UUIDField(default=uuid.uuid4, unique=True)
    seed = models.BigIntegerField()
    s3_url = models.URLField(max_length=1000)
    created_at = models.DateTimeField(default=now)
    finished_at = models.DateTimeField(null=True, default=None, db_index=True)

    def __str__(self):
        return f"uuid: {self.workload_uuid} - seed: {self.seed}"


class PromptSample(models.Model):
    """
    A sample of prompts to be solved from a particular series.
    Each sample is used to generate a single synthetic job after being solved.
    """

    series = models.ForeignKey(PromptSeries, on_delete=models.CASCADE, related_name="samples")
    workload = models.ForeignKey(SolveWorkload, on_delete=models.CASCADE, related_name="samples")
    synthetic_job = models.ForeignKey(SyntheticJob, on_delete=models.CASCADE, null=True)
    created_at = models.DateTimeField(default=now)

    class Meta:
        constraints = [
            UniqueConstraint(
                fields=["series", "workload"],
                name="unique_series_workload",
            ),
        ]


class Prompt(models.Model):
    sample = models.ForeignKey(PromptSample, on_delete=models.CASCADE, related_name="prompts")
    content = models.TextField()
    answer = models.TextField(null=True)


class MinerPreliminaryReservationQueryset(models.QuerySet["MinerPreliminaryReservation"]):
    def active(self, at: datetime | None = None) -> Self:
        at = at or now()
        return self.filter(expires_at__gt=at)


class MinerPreliminaryReservation(models.Model):
    miner = models.ForeignKey(Miner, on_delete=models.CASCADE)
    executor_class = models.CharField(max_length=255)
    job_uuid = models.UUIDField(default=uuid.uuid4, unique=True)
    created_at = models.DateTimeField(auto_now_add=True)
    expires_at = models.DateTimeField()

    objects = MinerPreliminaryReservationQueryset.as_manager()
