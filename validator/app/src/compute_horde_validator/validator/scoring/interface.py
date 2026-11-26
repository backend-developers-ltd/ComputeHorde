"""
Abstract interface for scoring engine.
"""

from abc import ABC, abstractmethod
from collections.abc import Collection


class ScoringEngine(ABC):
    """
    Abstract interface for scoring engine.
    """

    @abstractmethod
    def calculate_scores_for_cycles(
        self,
        current_cycle_start: int,
        previous_cycle_start: int,
        registered_hotkeys: Collection[str] | None = None,
    ) -> dict[str, float]:
        """
        Calculate scores for current cycle.

        Args:
            current_cycle_start: Start block of current cycle
            previous_cycle_start: Start block of previous cycle
            registered_hotkeys: Hotkeys currently registered on the subnet. When given, a
                coldkey's score is only distributed among its registered hotkeys.

        Returns:
            Dictionary mapping hotkey to final score
        """
        pass
