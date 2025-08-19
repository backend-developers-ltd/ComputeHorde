import sys
from typing import Any

if sys.version_info >= (3, 11):  # noqa: UP036
    from enum import StrEnum
else:
    from backports.strenum import StrEnum  #  noqa: UP035


class JobParticipantType(StrEnum):
    UNKNOWN = "unknown"

    SDK = "sdk"
    FACILITATOR = "facilitator"
    VALIDATOR = "validator"
    MINER = "miner"
    EXECUTOR = "executor"

    @classmethod
    def choices(cls):
        """Return Django-compatible choices tuple for model fields."""
        return [(status.value, status.value) for status in cls]

    @classmethod
    def _missing_(cls, value: Any) -> "JobParticipantType":
        return cls.UNKNOWN


class JobStatus(StrEnum):
    """
    Job status common between the validator, facilitator, and the SDK.
    """

    UNKNOWN = "unknown"

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
    HORDE_FAILED = "horde_failed"

    @classmethod
    def choices(cls):
        """Return Django-compatible choices tuple for model fields."""
        return [(status.value, status.value) for status in cls]

    @classmethod
    def end_states(cls) -> set["JobStatus"]:
        return {cls.COMPLETED, cls.REJECTED, cls.FAILED}

    def is_in_progress(self) -> bool:
        return self not in self.end_states()

    def is_successful(self) -> bool:
        return self == self.COMPLETED

    def is_failed(self) -> bool:
        return self in {self.REJECTED, self.FAILED}

    @classmethod
    def _missing_(cls, value: Any) -> "JobStatus":
        return cls.UNKNOWN


class JobStage(StrEnum):
    UNKNOWN = "unknown"

    EXECUTOR_STARTUP = "executor_startup"
    VOLUME_DOWNLOAD = "volume_download"
    STREAMING_STARTUP = "streaming_startup"
    EXECUTION = "execution"
    RESULT_UPLOAD = "result_upload"

    @classmethod
    def choices(cls):
        """Return Django-compatible choices tuple for model fields."""
        return [(status.value, status.value) for status in cls]

    @classmethod
    def _missing_(cls, value: Any) -> "JobStage":
        return cls.UNKNOWN


class HordeFailureReason(StrEnum):
    # When we somehow fail to propagate the failure reason
    UNKNOWN = "unknown"

    # For a multitude of random issues that don't require their own specific reason.
    # Make sure to provide a sensible message alongside the error.
    GENERIC_ERROR = "generic_error"

    # ↓ "except Exception" and "except TimeoutError"
    UNHANDLED_EXCEPTION = "unhandled_exception"
    UNHANDLED_TIMEOUT = "unhandled_timeout"

    # Received a legacy V0StreamingNotReady TODO(post error propagation): remove these
    STREAMING_FAILED = "streaming_failed"

    # Validator couldn't connect to a miner
    MINER_CONNECTION_FAILED = "miner_connection_failed"

    # ↓ Validator timeouts when waiting for a miner response
    # Note: miner not sending a message in time is not a job failure
    INITIAL_RESPONSE_TIMED_OUT = "initial_response_timed_out"
    EXECUTOR_READINESS_RESPONSE_TIMED_OUT = "executor_readiness_response_timed_out"
    VOLUMES_TIMED_OUT = "volumes_timed_out"
    STREAMING_JOB_READY_TIMED_OUT = "streaming_job_ready_timed_out"
    EXECUTION_TIMED_OUT = "execution_timed_out"
    FINAL_RESPONSE_TIMED_OUT = "final_response_timed_out"

    # Miner noticed that the executor failed to spin up
    EXECUTOR_SPINUP_FAILED = "executor_spinup_failed"

    # Executor disconnected unexpectedly
    EXECUTOR_DISCONNECTED = "executor_disconnected"

    # Executor failed the security check, or could not run it for some reason.
    SECURITY_CHECK_FAILED = "security_check_failed"

    @classmethod
    def _missing_(cls, value: Any) -> "HordeFailureReason":
        return cls.UNKNOWN


class JobFailureReason(StrEnum):
    # When we somehow fail to propagate the failure reason
    UNKNOWN = "unknown"

    # Exceeded one of the expected job timings
    TIMEOUT = "timeout"

    # Job container exited with a nonzero return code
    NONZERO_RETURN_CODE = "nonzero_return_code"
    DOWNLOAD_FAILED = "download_failed"
    UPLOAD_FAILED = "upload_failed"

    @classmethod
    def _missing_(cls, value: Any) -> "JobFailureReason":
        return cls.UNKNOWN


class JobRejectionReason(StrEnum):
    UNKNOWN = "unknown"
    BUSY = "busy"
    INVALID_SIGNATURE = "invalid_signature"
    NO_MINER_FOR_JOB = "no_miner_for_job"
    VALIDATOR_BLACKLISTED = "validator_blacklisted"

    @classmethod
    def _missing_(cls, value: Any) -> "JobRejectionReason":
        return cls.UNKNOWN
