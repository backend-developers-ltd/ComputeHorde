"""
Custom exceptions for the scoring module.

This module defines domain-specific exceptions that can be raised by the scoring module
to provide more explicit error handling and better debugging information.
"""


class ScoringError(Exception):
    """Base exception for scoring module errors."""

    pass


class SplitDistributionError(ScoringError):
    """Raised when split distribution is invalid or missing."""

    pass


class ColdkeyMappingError(ScoringError):
    """Raised when coldkey mapping cannot be determined."""

    pass


class InvalidSplitPercentageError(ScoringError):
    """Raised when split distribution percentages don't sum to 1.0."""

    def __init__(self, coldkey: str, total_percentage: float, distributions: dict):
        self.coldkey = coldkey
        self.total_percentage = total_percentage
        self.distributions = distributions
        super().__init__(
            f"Split distribution for {coldkey} doesn't sum to 1.0: {total_percentage}. "
            f"Distributions: {distributions}"
        )


class BittensorConnectionError(ScoringError):
    """Raised when connection to Bittensor metagraph fails."""

    def __init__(self, message: str, cause: Exception = None):
        super().__init__(message)
        if cause:
            self.__cause__ = cause


class ScoringConfigurationError(ScoringError):
    """Raised when scoring configuration is invalid or missing."""

    pass
