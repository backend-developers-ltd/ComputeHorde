"""
Scoring module for validator.

This module provides scoring functionality through a clean interface.
Users should interact with the scoring module only through the engine interface.
"""

from .interface import ScoringEngine
from .factory import create_scoring_engine, get_available_engine_types
from .calculations import (
    score_batches,
    get_executor_counts,
    get_penalty_multiplier,
)

__all__ = [
    "ScoringEngine",
    "create_scoring_engine",
    "get_available_engine_types",
    # Backward compatibility exports
    "score_batches",
    "get_executor_counts", 
    "get_penalty_multiplier",
] 