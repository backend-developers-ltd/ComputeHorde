import sys
from enum import Enum

if sys.version_info >= (3, 11):  # noqa: UP036
    from enum import StrEnum
else:
    from backports.strenum import StrEnum  #  noqa: UP035


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
    NOT_SPECIFIED = "not_specified"
    # ↓ Facilitator, validator
    ACCEPTANCE = "acceptance"
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
    INVALID_SIGNATURE = "invalid_signature"
    NO_MINER_FOR_JOB = "no_miner_for_job"
    EXECUTOR_FAILURE = "executor_failure"
    VALIDATOR_BLACKLISTED = "validator_blacklisted"
