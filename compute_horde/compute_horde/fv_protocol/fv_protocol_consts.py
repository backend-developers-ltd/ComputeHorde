import sys

if sys.version_info >= (3, 11):  # noqa: UP036
    from enum import StrEnum
else:
    from backports.strenum import StrEnum  #  noqa: UP035


class FaciValiJobStatus(StrEnum):
    """
    Job status common between the validator, facilitator, and the SDK.
    """

    SENT = "sent"
    RECEIVED = "received"
    ACCEPTED = "accepted"

    EXECUTOR_READY = "executor_ready"
    STREAMING_READY = "streaming_ready"
    VOLUMES_READY = "volumes_ready"
    EXECUTION_DONE = "execution_done"

    COMPLETED = "completed"
    REJECTED = "rejected"
    FAILED = "failed"

    @classmethod
    def choices(cls):
        """Return Django-compatible choices tuple for model fields."""
        return [(status.value, status.value) for status in cls]

    @classmethod
    def end_states(cls) -> set["FaciValiJobStatus"]:
        return {FaciValiJobStatus.COMPLETED, FaciValiJobStatus.REJECTED, FaciValiJobStatus.FAILED}

    def is_in_progress(self) -> bool:
        return self not in FaciValiJobStatus.end_states()

    def is_successful(self) -> bool:
        return self == FaciValiJobStatus.COMPLETED

    def is_failed(self) -> bool:
        return self in {FaciValiJobStatus.REJECTED, FaciValiJobStatus.FAILED}
