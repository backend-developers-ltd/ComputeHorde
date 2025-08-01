"""
Internal models for main hotkey storage.
"""

from dataclasses import dataclass

from django.db import models


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
