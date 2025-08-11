"""
Receipts module for validator.

This module provides receipts management functionality for the validator.
"""

from .exceptions import ReceiptsConfigurationError, ReceiptsGenerationError, ReceiptsScrapingError
from .interface import ReceiptsBase
from .manager import Receipts

__all__ = [
    "ReceiptsBase",
    "Receipts",
    "ReceiptsConfigurationError",
    "ReceiptsGenerationError",
    "ReceiptsScrapingError",
]
