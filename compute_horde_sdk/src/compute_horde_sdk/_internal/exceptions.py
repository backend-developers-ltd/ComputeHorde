class ComputeHordeError(Exception):
    """The base class for all errors thrown by the Compute Horde."""


class ComputeHordeNotFoundError(ComputeHordeError):
    """The requested resource was not found in Compute Horde."""


class ComputeHordeJobTimeoutError(ComputeHordeError, TimeoutError):
    """Compute Horde job timed out."""
