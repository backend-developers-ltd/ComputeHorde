"""Models internal for the routing module, not to be used by other modules."""

import uuid

from compute_horde_core.executor_class import ExecutorClass
from django.db import models
from django.db.models import UniqueConstraint


class MinerIncident(models.Model):
    class IncidentType(models.TextChoices):
        MINER_JOB_REJECTED = "MINER_JOB_REJECTED"
        MINER_JOB_FAILED = "MINER_JOB_FAILED"
        MINER_HORDE_FAILED = "MINER_HORDE_FAILED"

    type = models.CharField(max_length=255, choices=IncidentType.choices)
    timestamp = models.DateTimeField(auto_now_add=True)
    hotkey_ss58address = models.TextField()
    job_uuid = models.UUIDField(default=uuid.uuid4, unique=True)
    executor_class = models.CharField(
        max_length=255, choices=[(choice.value, choice.value) for choice in ExecutorClass]
    )

    class Meta:
        constraints = [
            UniqueConstraint(
                fields=["hotkey_ss58address", "job_uuid"],
                name="unique_miner_incident",
            ),
        ]
