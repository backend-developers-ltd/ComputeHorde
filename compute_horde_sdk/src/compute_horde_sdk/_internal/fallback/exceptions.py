class FallbackError(Exception):
    """The base class for all errors thrown by the fallback."""


class FallbackNotFoundError(FallbackError):
    """The requested resource was not found in fallback."""


class FallbackJobTimeoutError(FallbackError, TimeoutError):
    """Fallback job timed out."""
