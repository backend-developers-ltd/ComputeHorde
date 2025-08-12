"""
Abstract interface for scoring engine.
"""

from abc import ABC, abstractmethod


class ScoringEngine(ABC):
    """
    Abstract interface for scoring engine.
    """

    @abstractmethod
    def calculate_scores_for_cycles(
        self, current_cycle_start: int, previous_cycle_start: int
    ) -> dict[str, float]:
        """
        Calculate scores for current cycle.

        Args:
            current_cycle_start: Start block of current cycle
            previous_cycle_start: Start block of previous cycle

        Returns:
            Dictionary mapping hotkey to final score
        """
        pass
