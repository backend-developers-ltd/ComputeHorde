class ComputeHordeError(Exception):
    """Something went wrong on the Compute Horde side."""


class ComputeHordeNotFoundError(ComputeHordeError):
    """The requested resource was not found in Compute Horde."""


class ComputeHordeJobTimeoutError(ComputeHordeError, TimeoutError):
    """Compute Horde job timed out."""
