"""
Custom exceptions for the scoring module.
"""


class ScoringError(Exception):
    """Base exception for scoring module errors."""

    pass


class MainHotkeyError(ScoringError):
    """Raised when main hotkey processing fails."""

    pass


class ScoringConfigurationError(ScoringError):
    """Raised when scoring configuration is invalid or missing."""

    pass
