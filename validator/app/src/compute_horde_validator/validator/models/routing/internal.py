"""Models internal for the routing module, not to be used by other modules."""

import uuid

from compute_horde_core.executor_class import ExecutorClass
from django.db import models
from django.db.models import UniqueConstraint

from compute_horde_validator.validator.routing.types import MinerIncidentType


class MinerIncident(models.Model):
    type = models.CharField(max_length=255, choices=MinerIncidentType.choices)
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
