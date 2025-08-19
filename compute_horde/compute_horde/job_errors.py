import logging
from typing import Generic, TypeVar

from compute_horde.protocol_consts import HordeFailureReason, JobFailureReason, JobRejectionReason
from compute_horde.protocol_messages import FailureContext

ReasonType = TypeVar("ReasonType")

logger = logging.getLogger(__name__)


class BaseJobError(Exception, Generic[ReasonType]):
    """
    Common structure for job-related errors.
    """

    def __init__(self, message: str, reason: ReasonType, context: FailureContext | None = None):
        """
        Parameters:
            message: str
                Short message for a human seeing the error somewhere.
            reason: ReasonType
                Structured error reason.
            context: FailureContext | None
                Optional arbitrary key-value context to be sent along the error.
                Avoid
        """
        super().__init__(message)
        self.message = message
        self.reason = reason
        self.context = context

    def add_context(self, context: FailureContext):
        if self.context is None:
            self.context = {**context}
        else:
            self.context.update(context)

    def __str__(self):
        return f"{self.message}, {self.reason=}"

    def __repr__(self):
        return f"{type(self).__name__}: {str(self)}"


class JobRejection(BaseJobError[JobRejectionReason]):
    def __str__(self):
        return f"Job rejected ({self.reason}): {self.message} "


class JobError(BaseJobError[JobFailureReason]):
    def __str__(self):
        return f"Job failed ({self.reason}): {self.message} "


class HordeError(BaseJobError[HordeFailureReason]):
    def __init__(
        self,
        message: str,
        reason: HordeFailureReason = HordeFailureReason.GENERIC_ERROR,
        context: FailureContext | None = None,
    ):
        super().__init__(message, reason, context)

    def __str__(self):
        return f"Horde failed ({self.reason}): {self.message}"

    @classmethod
    def wrap_unhandled(cls, e: Exception, context: FailureContext | None = None) -> "HordeError":
        """
        Intended for "catch-all" except blocks - consistently wraps the exception in a HordeError.
        For catching errors somewhere in the job code, do instead:
            `raise HordeError("Spline reticulation failed") from e`
        """
        if isinstance(e, HordeError):
            # Don't wrap another horde failure
            return e

        if isinstance(e, BaseJobError):
            # If another job error is to be wrapped by accident - don't blow up, but scream for help
            logger.error(f"Wrapping a {type(e).__qualname__} in a HordeError", exc_info=False)

        failure = cls("Unhandled exception", HordeFailureReason.UNHANDLED_EXCEPTION, context)
        failure.__cause__ = e
        failure.add_context({"exception_type": type(e).__qualname__})
        return failure
