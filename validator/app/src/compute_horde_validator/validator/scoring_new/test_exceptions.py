"""
Tests for custom exceptions in the scoring module.
"""

import pytest

from .exceptions import (
    BittensorConnectionError,
    ColdkeyMappingError,
    InvalidSplitPercentageError,
    ScoringConfigurationError,
    ScoringError,
    SplitDistributionError,
)


class TestScoringExceptions:
    """Test custom exceptions in the scoring module."""

    def test_scoring_error_base_exception(self):
        """Test that ScoringError is the base exception."""
        error = ScoringError("Test error")
        assert isinstance(error, Exception)
        assert str(error) == "Test error"

    def test_split_distribution_error(self):
        """Test SplitDistributionError."""
        error = SplitDistributionError("Invalid split")
        assert isinstance(error, ScoringError)
        assert str(error) == "Invalid split"

    def test_coldkey_mapping_error(self):
        """Test ColdkeyMappingError."""
        error = ColdkeyMappingError("Cannot map coldkey")
        assert isinstance(error, ScoringError)
        assert str(error) == "Cannot map coldkey"

    def test_invalid_split_percentage_error(self):
        """Test InvalidSplitPercentageError with detailed information."""
        coldkey = "test_coldkey"
        total_percentage = 1.2
        distributions = {"hotkey1": 0.7, "hotkey2": 0.5}

        error = InvalidSplitPercentageError(coldkey, total_percentage, distributions)

        assert isinstance(error, ScoringError)
        assert error.coldkey == coldkey
        assert error.total_percentage == total_percentage
        assert error.distributions == distributions
        assert "test_coldkey" in str(error)
        assert "1.2" in str(error)
        assert "hotkey1" in str(error)

    def test_bittensor_connection_error(self):
        """Test BittensorConnectionError."""
        original_error = ConnectionError("Connection refused")
        error = BittensorConnectionError("Failed to connect", original_error)

        assert isinstance(error, ScoringError)
        assert "Failed to connect" in str(error)
        assert error.__cause__ == original_error

    def test_scoring_configuration_error(self):
        """Test ScoringConfigurationError."""
        error = ScoringConfigurationError("Invalid configuration")
        assert isinstance(error, ScoringError)
        assert str(error) == "Invalid configuration"

    def test_exception_hierarchy(self):
        """Test that all custom exceptions inherit from ScoringError."""
        exceptions = [
            SplitDistributionError("test"),
            ColdkeyMappingError("test"),
            InvalidSplitPercentageError("test", 1.0, {}),
            BittensorConnectionError("test"),
            ScoringConfigurationError("test"),
        ]

        for exc in exceptions:
            assert isinstance(exc, ScoringError)
            assert isinstance(exc, Exception)

    def test_exception_usage_example(self):
        """Test how exceptions can be used in practice."""

        def validate_split_percentage(coldkey: str, percentages: dict) -> None:
            total = sum(percentages.values())
            if abs(total - 1.0) > 0.0001:
                raise InvalidSplitPercentageError(coldkey, total, percentages)

        # Valid case
        validate_split_percentage("coldkey1", {"hotkey1": 0.6, "hotkey2": 0.4})

        # Invalid case
        with pytest.raises(InvalidSplitPercentageError) as exc_info:
            validate_split_percentage("coldkey2", {"hotkey1": 0.6, "hotkey2": 0.5})

        error = exc_info.value
        assert error.coldkey == "coldkey2"
        assert error.total_percentage == 1.1
        assert error.distributions == {"hotkey1": 0.6, "hotkey2": 0.5}
