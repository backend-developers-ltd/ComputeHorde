import shlex
import uuid
from datetime import timedelta

from django.db import models
from django.db.models import UniqueConstraint


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

    def set_manifest(self, manifest):
        self._manifest = manifest

    def get_manifest(self):
        return getattr(self, "_manifest", None)


class SyntheticJobBatch(models.Model):
    started_at = models.DateTimeField(auto_now_add=True)
    accepting_results_until = models.DateTimeField()
    scored = models.BooleanField(default=False)


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
