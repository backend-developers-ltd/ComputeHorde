import uuid

from django.db import models


class Miner(models.Model):
    hotkey = models.CharField(max_length=255, unique=True)
    created_at = models.DateTimeField(auto_now_add=True)


class SyntheticJobBatch(models.Model):
    started_at = models.DateTimeField(auto_now_add=True)
    accepting_results_until = models.DateTimeField()


class SyntheticJob(models.Model):
    class Status(models.TextChoices):
        PENDING = 'PENDING'
        COMPLETED = 'COMPLETED'
        FAILED = 'FAILED'
    job_uuid = models.UUIDField(default=uuid.uuid4, unique=True)
    batch = models.ForeignKey(SyntheticJobBatch, on_delete=models.CASCADE)
    miner = models.ForeignKey(Miner, on_delete=models.CASCADE)
    miner_address = models.CharField(max_length=255)
    miner_address_ip_version = models.IntegerField()
    miner_port = models.IntegerField()
    status = models.TextField(choices=Status.choices, default=Status.PENDING)

    class Meta:
        unique_together = ('batch', 'miner')


