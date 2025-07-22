"""
Abstract interface for scoring engines.
"""

from abc import ABC, abstractmethod
from typing import Dict


class ScoringEngine(ABC):
    """
    Abstract interface for scoring engines that calculate scores for cycles and apply decoupled dancing.
    """
    
    @abstractmethod
    async def calculate_scores_for_cycles(
        self, 
        current_cycle_start: int,
        previous_cycle_start: int,
        validator_hotkey: str
    ) -> Dict[str, float]:
        """
        Calculate scores for two cycles and apply decoupled dancing.
        
        Args:
            current_cycle_start: Start block of current cycle
            previous_cycle_start: Start block of previous cycle
            validator_hotkey: Validator hotkey for split retrieval
            
        Returns:
            Dictionary mapping hotkey to final score
        """
        pass 