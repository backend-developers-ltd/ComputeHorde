"""
Internal models for split storage and management.
"""

from dataclasses import dataclass
from typing import Dict, Optional
from django.db import models


@dataclass
class SplitInfo:
    """Internal representation of a miner's split distribution."""
    coldkey: str
    cycle_start: int
    validator_hotkey: str
    distributions: Dict[str, float]  # hotkey -> percentage


class SplitStorage:
    """Internal storage for split information."""
    
    def __init__(self):
        self._splits: Dict[str, SplitInfo] = {}
    
    def _get_key(self, coldkey: str, cycle_start: int, validator_hotkey: str) -> str:
        """Generate a unique key for split storage."""
        return f"{coldkey}:{cycle_start}:{validator_hotkey}"
    
    def save_split(self, split: SplitInfo) -> None:
        """Save a split information."""
        key = self._get_key(split.coldkey, split.cycle_start, split.validator_hotkey)
        self._splits[key] = split
    
    def get_split(self, coldkey: str, cycle_start: int, validator_hotkey: str) -> Optional[SplitInfo]:
        """Get split information for a specific coldkey, cycle, and validator."""
        key = self._get_key(coldkey, cycle_start, validator_hotkey)
        return self._splits.get(key)
    
    def has_split_change(
        self, 
        coldkey: str, 
        current_cycle: int, 
        previous_cycle: int, 
        validator_hotkey: str
    ) -> bool:
        """Check if there was a split change between cycles."""
        current_split = self.get_split(coldkey, current_cycle, validator_hotkey)
        previous_split = self.get_split(coldkey, previous_cycle, validator_hotkey)
        
        if current_split is None or previous_split is None:
            return False
        
        return current_split.distributions != previous_split.distributions


class MinerSplit(models.Model):
    """
    Stores split information for miners in a group (same coldkey).
    """
    coldkey = models.CharField(max_length=255, db_index=True)
    cycle_start = models.BigIntegerField()
    cycle_end = models.BigIntegerField()
    validator_hotkey = models.CharField(max_length=255)
    created_at = models.DateTimeField(auto_now_add=True)
    
    class Meta:
        unique_together = ('coldkey', 'cycle_start', 'validator_hotkey')
        indexes = [
            models.Index(fields=['coldkey', 'cycle_start']),
            models.Index(fields=['validator_hotkey']),
        ]
    
    def __str__(self):
        return f"Split for {self.coldkey} -> validator {self.validator_hotkey} (cycle {self.cycle_start})"


class MinerSplitDistribution(models.Model):
    """
    Stores the distribution percentages for each hotkey in a split.
    """
    split = models.ForeignKey(MinerSplit, on_delete=models.CASCADE, related_name='distributions')
    hotkey = models.CharField(max_length=255)
    percentage = models.DecimalField(max_digits=5, decimal_places=4)  # 0.0000 to 1.0000
    
    class Meta:
        unique_together = ('split', 'hotkey')
        constraints = [
            models.CheckConstraint(
                check=models.Q(percentage__gte=0) & models.Q(percentage__lte=1),
                name='valid_percentage_range'
            )
        ]
    
    def __str__(self):
        return f"{self.hotkey}: {self.percentage}%" 