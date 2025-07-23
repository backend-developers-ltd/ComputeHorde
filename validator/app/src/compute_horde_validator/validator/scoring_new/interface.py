"""
Abstract interface for scoring engines.
"""

from abc import ABC, abstractmethod


class ScoringEngine(ABC):
    """
    Abstract interface for scoring engines that calculate scores.
    """

    @abstractmethod
    async def calculate_scores_for_cycles(
        self, current_cycle_start: int, previous_cycle_start: int, validator_hotkey: str
    ) -> dict[str, float]:
        """
        Calculate scores for two cycles.

        Args:
            current_cycle_start: Start block of current cycle
            previous_cycle_start: Start block of previous cycle
            validator_hotkey: Validator hotkey for split retrieval

        Returns:
            Dictionary mapping hotkey to final score
        """
        pass
