import sys
from enum import Enum

if sys.version_info >= (3, 11):  # noqa: UP036
    from enum import StrEnum
else:
    from backports.strenum import StrEnum  #  noqa: UP035


class JobStatus(Enum):
    UNKNOWN = "unknown"
    NOT_SUBMITTED = "not_submitted"
    IN_PROGRESS = "in_progress"
    COMPLETED = "completed"
    REJECTED = "rejected"
    JOB_FAILED = "job_failed"
    HORDE_FAILED = "horde_failed"

    @classmethod
    def end_states(cls) -> set["JobStatus"]:
        """
        Determines which job statuses mean that the job will not be updated anymore.
        """
        return {cls.COMPLETED, cls.REJECTED, cls.JOB_FAILED, cls.HORDE_FAILED}

    def is_in_progress(self) -> bool:
        """
        Check if the job is in progress (has not completed or failed yet).
        """
        return self not in JobStatus.end_states()

    def is_successful(self) -> bool:
        """Check if the job has finished successfully."""
        return self == JobStatus.COMPLETED

    def is_failed(self) -> bool:
        """Check if the job has failed."""
        return self in {JobStatus.REJECTED, JobStatus.JOB_FAILED, JobStatus.HORDE_FAILED}

    @classmethod
    def choices(cls):
        """Return Django-compatible choices tuple for model fields."""
        return [(status.value, status.value) for status in cls]


class JobParticipantType(Enum):
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


class JobStage(Enum):
    UNKNOWN = "unknown"
    # ↓ Facilitator, validator
    SUBMISSION = "submission"
    ROUTING = "routing"
    # ↓ Miner
    RESERVATION = "reservation"
    EXECUTOR_SPINUP = "executor_spinup"
    # ↓ Executor
    EXECUTOR_STARTUP = "executor_startup"
    STREAMING_STARTUP = "streaming_startup"
    VOLUME_DOWNLOAD = "volume_download"
    EXECUTION = "execution"
    RESULT_UPLOAD = "result_upload"
    CLOSURE = "closure"

    @classmethod
    def choices(cls):
        """Return Django-compatible choices tuple for model fields."""
        return [(status.value, status.value) for status in cls]


class HordeFailureReason(StrEnum):
    # TODO(error propagation): it could be beneficial to turn these into simple str constants
    UNCAUGHT_EXCEPTION = "uncaught_exception"
    STREAMING_SETUP_FAILED = "streaming_setup_failed"
    JOB_IMAGE_MISSING = "job_image_missing"
    SECURITY_CHECK_FAILED = "security_check_failed"
    UPSTREAM_CONNECTION_ERROR = "upstream_connection_error"


class JobFailureReason(StrEnum):
    TIMEOUT = "TIMEOUT"
    SECURITY_CHECK = "SECURITY_CHECK"
    HUGGINGFACE_DOWNLOAD = "HUGGINGFACE_DOWNLOAD"
    NONZERO_EXIT_CODE = "NONZERO_EXIT_STATUS"


class JobRejectionReason(StrEnum):
    NOT_SPECIFIED = "not_specified"
    BUSY = "busy"
    EXECUTOR_FAILURE = "executor_failure"
    VALIDATOR_BLACKLISTED = "validator_blacklisted"
