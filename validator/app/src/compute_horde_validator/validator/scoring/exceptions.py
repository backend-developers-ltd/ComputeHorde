"""
Custom exceptions for the scoring module.
"""


class ScoringError(Exception):
    """Base exception for scoring module errors."""

    pass


class SplitDistributionError(ScoringError):
    """Raised when split distribution is invalid or missing."""

    pass


class ScoringConfigurationError(ScoringError):
    """Raised when scoring configuration is invalid or missing."""

    pass
