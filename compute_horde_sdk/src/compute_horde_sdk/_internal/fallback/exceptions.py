class FallbackError(Exception):
    """The base class for all errors thrown by the fallback service."""


class FallbackNotFoundError(FallbackError):
    """The requested resource was not found in the fallback cluster."""


class FallbackJobTimeoutError(FallbackError, TimeoutError):
    """Fallback job timed out."""
