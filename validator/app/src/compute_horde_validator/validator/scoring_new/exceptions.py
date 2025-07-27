"""
Custom exceptions for the scoring module.
"""


class ScoringError(Exception):
    """Base exception for scoring module errors."""

    pass


class SplitDistributionError(ScoringError):
    """Raised when split distribution is invalid or missing."""

    pass


class MinerConnectionError(ScoringError):
    """Raised when connection to Bittensor metagraph fails."""

    def __init__(self, message: str, cause: Exception = None):
        super().__init__(message)
        if cause:
            self.__cause__ = cause


class ScoringConfigurationError(ScoringError):
    """Raised when scoring configuration is invalid or missing."""

    pass
