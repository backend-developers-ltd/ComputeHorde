"""Models internal for the scoring module, not to be used by other modules."""

from dataclasses import dataclass
from os import urandom
from typing import Self

from compute_horde.subtensor import get_cycle_containing_block
from django.contrib.postgres.fields import ArrayField
from django.db import models
from django.db.models.constraints import UniqueConstraint
from django.utils.timezone import now


def get_random_salt() -> list[int]:
    return list(urandom(8))


class WeightSettingFinishedEvent(models.Model):
    """
    A record in this table indicates a successful weight setting event. Weight setting in cycle N does calculations
    based on work done in cycle N-1 and this is reflected in columns `block_from` and `block_to`.
    """

    created_at = models.DateTimeField(default=now)
    block_from = models.BigIntegerField()
    block_to = models.BigIntegerField()

    class Meta:
        constraints = [
            UniqueConstraint(fields=["block_from", "block_to"], name="unique_cycle_in_wsfe"),
        ]

    def __str__(self):
        return f"WeightSettingEvent [{self.block_from};{self.block_to})"

    @classmethod
    def from_block(cls, block: int, netuid: int) -> tuple[Self, bool]:
        r = get_cycle_containing_block(block=block, netuid=netuid)
        return cls.objects.get_or_create(
            block_from=r.start - (r.stop - r.start), block_to=r.stop - (r.stop - r.start)
        )


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


@dataclass
class MainHotkeyInfo:
    coldkey: str
    cycle_start: int
    main_hotkey: str | None


class MinerMainHotkey(models.Model):
    """
    Stores main hotkey information for a coldkey in a cycle.
    """

    coldkey = models.CharField(max_length=255, db_index=True)
    cycle_start = models.BigIntegerField()
    main_hotkey = models.CharField(max_length=255, null=True, blank=True)
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        unique_together = ("coldkey", "cycle_start")
        indexes = [
            models.Index(fields=["coldkey", "cycle_start"]),
        ]

    def __str__(self):
        return f"Main hotkey for {self.coldkey} -> {self.main_hotkey} (cycle {self.cycle_start})"
