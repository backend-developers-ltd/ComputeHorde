"""
Internal models for split storage and management.
"""

from dataclasses import dataclass

from django.db import models


@dataclass
class SplitInfo:
    """Internal representation of a miner's split distribution."""

    coldkey: str
    cycle_start: int
    validator_hotkey: str
    distributions: dict[str, float]  # hotkey -> percentage


class MinerSplit(models.Model):
    """
    Stores split information for miners in a group (same coldkey).
    """

    coldkey = models.CharField(max_length=255, db_index=True)
    cycle_start = models.BigIntegerField()
    validator_hotkey = models.CharField(max_length=255)
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        unique_together = ("coldkey", "cycle_start", "validator_hotkey")
        indexes = [
            models.Index(fields=["coldkey", "cycle_start"]),
            models.Index(fields=["validator_hotkey"]),
        ]

    def __str__(self):
        return f"Split for {self.coldkey} -> validator {self.validator_hotkey} (cycle {self.cycle_start})"


class MinerSplitDistribution(models.Model):
    """
    Stores the distribution percentages for each hotkey in a split.
    """

    split = models.ForeignKey(MinerSplit, on_delete=models.CASCADE, related_name="distributions")
    hotkey = models.CharField(max_length=255)
    percentage = models.DecimalField(max_digits=5, decimal_places=4)  # 0.0000 to 1.0000

    class Meta:
        unique_together = ("split", "hotkey")
        constraints = [
            models.CheckConstraint(
                check=models.Q(percentage__gte=0) & models.Q(percentage__lte=1),
                name="valid_percentage_range",
            )
        ]

    def __str__(self):
        return f"{self.hotkey}: {self.percentage}%"
