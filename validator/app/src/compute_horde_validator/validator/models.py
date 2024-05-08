import uuid

from django.db import models


class Miner(models.Model):
    hotkey = models.CharField(max_length=255, unique=True)
    created_at = models.DateTimeField(auto_now_add=True)

    def __str__(self):
        return f'hotkey: {self.hotkey}'


class SyntheticJobBatch(models.Model):
    started_at = models.DateTimeField(auto_now_add=True)
    accepting_results_until = models.DateTimeField()
    scored = models.BooleanField(default=False)


class JobBase(models.Model):
    class Meta:
        abstract = True

    class Status(models.TextChoices):
        PENDING = 'PENDING'
        COMPLETED = 'COMPLETED'
        FAILED = 'FAILED'
    job_uuid = models.UUIDField(default=uuid.uuid4, unique=True)
    miner = models.ForeignKey(Miner, on_delete=models.CASCADE)
    miner_address = models.CharField(max_length=255)
    miner_address_ip_version = models.IntegerField()
    miner_port = models.IntegerField()
    status = models.TextField(choices=Status.choices, default=Status.PENDING)
    updated_at = models.DateTimeField(auto_now=True)
    comment = models.TextField(blank=True, default='')
    job_description = models.TextField(blank=True)

    def __str__(self):
        return f'uuid: {self.job_uuid} - miner hotkey: {self.miner.hotkey} - {self.status}'


class SyntheticJob(JobBase):
    batch = models.ForeignKey(SyntheticJobBatch, on_delete=models.CASCADE, related_name='synthetic_jobs')
    score = models.FloatField(default=0)

    class Meta:
        # unique_together = ('batch', 'miner')
        constraints = [
            models.UniqueConstraint(fields=['batch', 'miner'], name='one_job_per_batch_per_miner'),
        ]


class OrganicJob(JobBase):
    pass


class SyntheticJobContents(models.Model):
    created_at = models.DateTimeField(auto_now_add=True)
    weights_version = models.IntegerField(db_index=True)
    answer = models.TextField()
    contents = models.BinaryField()
