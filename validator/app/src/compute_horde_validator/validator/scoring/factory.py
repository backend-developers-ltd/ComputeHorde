"""
Factory for creating scoring engine instances.
"""

from typing import Optional
from .interface import ScoringEngine
from .engine import DefaultScoringEngine


def create_scoring_engine(engine_type: Optional[str] = None) -> ScoringEngine:
    """
    Create a scoring engine instance.
    
    Args:
        engine_type: Type of engine to create. Currently only supports "default" or None.
                    Defaults to "default" engine.
    
    Returns:
        ScoringEngine instance
        
    Raises:
        ValueError: If engine_type is not supported
    """
    if engine_type is None or engine_type == "default":
        return DefaultScoringEngine()
    else:
        raise ValueError(f"Unsupported engine type: {engine_type}")


def get_available_engine_types() -> list[str]:
    """
    Get list of available engine types.
    
    Returns:
        List of available engine type names
    """
    return ["default"] 