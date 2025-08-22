"""
Receipts module for validator.

This module provides receipts management functionality for the validator.
"""

from .base import ReceiptsBase
from .default import Receipts

__all__ = [
    "ReceiptsBase",
    "Receipts",
]
