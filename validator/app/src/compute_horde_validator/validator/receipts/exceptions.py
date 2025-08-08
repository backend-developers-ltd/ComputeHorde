class ReceiptsConfigurationError(Exception):
    """Raised when there is a configuration error in the receipts module."""

    pass


class ReceiptsScrapingError(Exception):
    """Raised when there is an error scraping receipts from miners."""

    pass


class ReceiptsGenerationError(Exception):
    """Raised when there is an error generating receipts."""

    pass


class ReceiptsValidationError(Exception):
    """Raised when there is an error validating receipts."""

    pass
