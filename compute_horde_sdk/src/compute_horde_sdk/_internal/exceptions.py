class ComputeHordeError(Exception):
    """The base class for all errors thrown by the ComputeHorde."""


class ComputeHordeNotFoundError(ComputeHordeError):
    """The requested resource was not found in ComputeHorde."""


class ComputeHordeJobTimeoutError(ComputeHordeError, TimeoutError):
    """ComputeHorde job timed out."""
